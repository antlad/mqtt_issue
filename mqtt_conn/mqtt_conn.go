//
// Copyright (c) 2018 Litmus Automation Inc.
//

package cloud_conn

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/eclipse/paho.mqtt.golang"
)

const (
	keepAliveTimeout        = time.Second * 2
	pingTimeOut             = time.Second * 2
	connectTimeOut          = time.Second * 3
	publishTimeout          = time.Second * 7 // previous 1 second seams was too aggressive
	reconnectTimeoutStep    = time.Second * 3
	publishMessageQueueSize = 100
	checkTokensTimeout      = time.Millisecond * 100
)

const (
	mqttQOS = 1
)

type PublishError struct {
	Err     error
	topic   string
	Payload []byte
}

type publishMessage struct {
	topic   string
	payload []byte
	token   mqtt.Token
}

type CloudConnection interface {
	Run(ctx context.Context)
	Stop() error
	Start() error
	IsConnected() bool
	LastError() error
	Publish(topic string, payload []byte) error
}

type CloudConnectionOptions struct {
	Host   string
	Scheme string
	Port   int

	ClientID  string
	UserName  string
	Password  string
	SslRootCA string

	PubErrorChan       chan *PublishError
	PubDoneChan        chan int32
	StateChangedNotify chan ConnectionState
}

type ConnectionState int32

const (
	Disconnected ConnectionState = iota
	Connected
)

type mqttConnection struct {
	host               string
	scheme             string
	port               int
	clientID           string
	userName           string
	lastErr            atomic.Value
	state              int32
	client             mqtt.Client
	commands           chan command
	reconnectEnabled   bool
	publishMessages    chan *publishMessage
	pubErrorChan       chan *PublishError
	pubDoneChan        chan int32
	stateChangedNotify chan ConnectionState
}

type command int32

const (
	commandStart command = iota
	commandStop
)

var once sync.Once
var systemPool *x509.CertPool


func NewConnection(conf *CloudConnectionOptions) (CloudConnection, error) {

	options := mqtt.NewClientOptions()
	serverAddress := fmt.Sprintf("%s://%s:%d", conf.Scheme, conf.Host, conf.Port)
	options.AddBroker(serverAddress)
	options.SetClientID(conf.ClientID)
	options.SetUsername(conf.UserName)
	options.SetPassword(conf.Password)
	options.SetOrderMatters(false)

	if conf.Scheme == "ssl" || conf.Scheme == "https" {
		options.TLSConfig = &tls.Config{}
		options.TLSConfig.InsecureSkipVerify = true
		if len(conf.SslRootCA) > 0 {
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM([]byte(conf.SslRootCA)) {
				return nil, errors.New("bad config for SSL Root CA")
			}
			options.TLSConfig.RootCAs = pool
		} else {
			once.Do(func() {
				var err error
				systemPool, err = x509.SystemCertPool()
				if err != nil {
					logrus.Error(fmt.Sprintf("system cert pool failed %s", err.Error()))
				}
			})
			options.TLSConfig.RootCAs = systemPool
		}
	}

	options.SetKeepAlive(keepAliveTimeout)
	options.SetPingTimeout(pingTimeOut)
	options.SetCleanSession(true)
	options.SetWriteTimeout(publishTimeout)
	options.SetConnectTimeout(connectTimeOut)
	options.SetAutoReconnect(false)

	con := &mqttConnection{
		state:              int32(Disconnected),
		reconnectEnabled:   false,
		commands:           make(chan command),
		publishMessages:    make(chan *publishMessage),
		pubErrorChan:       conf.PubErrorChan,
		host:               conf.Host,
		scheme:             conf.Scheme,
		port:               conf.Port,
		clientID:           conf.ClientID,
		userName:           conf.UserName,
		stateChangedNotify: conf.StateChangedNotify,
		pubDoneChan:        conf.PubDoneChan,
	}
	options.SetOnConnectHandler(con.onConnected)
	options.SetConnectionLostHandler(con.onConnectionLost)

	con.client = mqtt.NewClient(options)
	return con, nil
}

func (m *mqttConnection) logMessage() *logrus.Entry {
	return logrus.WithFields(logrus.Fields{
		"component": "mqtt_connection",
		"host":      m.host,
		"scheme":    m.scheme,
		"port":      m.port,
		"clientID":  m.clientID,
		"userName":  m.userName,
	})
}

func (m *mqttConnection) Run(ctx context.Context) {

	waitList := make([]*publishMessage, 0)
	checkTokens := time.NewTicker(checkTokensTimeout)
	reconnectTimer := time.NewTicker(reconnectTimeoutStep)

	sendBack := func(t *publishMessage, err error) {
		select {
		case m.pubErrorChan <- &PublishError{
			Err:     err,
			topic:   t.topic,
			Payload: t.payload,
		}:
		default:

			m.logMessage().Error("mqtt connection pub error queue full. dropping")
		}
	}

	doConnect := func() {
		t := m.client.Connect()
		if !t.Wait() {
			m.logMessage().Error("no response from mqtt connection")
		} else {
			if t.Error() != nil {
				m.lastErr.Store(t.Error())
				m.logMessage().WithField("error", t.Error()).Error("connect failed")
			} else {
				m.logMessage().Info("connect done")
			}
		}
	}
	cycleTimer := time.NewTicker(time.Second * 2)

	for {
		select {
		case <-cycleTimer.C:
			{
				fmt.Println("---------------------------------------")
				cycleTimer = time.NewTicker(time.Second * 2)
			}
		case <-checkTokens.C:
			{
				if len(waitList) == 0 {
					continue
				}

				waitMore := make([]*publishMessage, 0)
				var doneCounter int32 = 0

				for _, t := range waitList {

					if !t.token.WaitTimeout(0) {
						waitMore = append(waitMore, t)
						continue
					}

					if t.token.Error() != nil {
						m.lastErr.Store(t.token.Error())
						sendBack(t, t.token.Error())
						continue
					}

					doneCounter++
				}

				if doneCounter != 0 {
					select {
					case m.pubDoneChan <- doneCounter:
						{
						}
					default:
						m.logMessage().Error("mqtt connection pub done queue full. dropping")
					}
				}

				waitList = waitMore
				checkTokens = time.NewTicker(checkTokensTimeout)
			}
		case <-reconnectTimer.C:
			{
				if m.reconnectEnabled && !m.IsConnected() {
					m.logMessage().Info("try to reconnect")
					doConnect()
				}
				reconnectTimer = time.NewTicker(reconnectTimeoutStep)
			}
		case <-ctx.Done():
			{
				m.logMessage().Info("exiting mqtt connection")
				return
			}
		case msg := <-m.publishMessages:
			{
				if !m.IsConnected() {
					sendBack(msg, errors.New("not Connected"))
					continue
				}

				fmt.Println("+++ before publish")
				t := m.client.Publish(msg.topic, mqttQOS, false, msg.payload)
				fmt.Println("+++ after publish")
				t.Wait()
				if t.Error() != nil {
					m.lastErr.Store(t.Error())
					msg.token = t
					sendBack(msg, t.Error())
				}

				msg.token = t
				waitList = append(waitList, msg)
			}
		case c := <-m.commands:
			{
				switch c {
				case commandStart:
					{
						m.logMessage().Info("starting")
						m.reconnectEnabled = true
						if !m.IsConnected() {
							doConnect()
						}
					}
				case commandStop:
					{
						m.logMessage().Info("stopping")
						m.reconnectEnabled = false
						if m.IsConnected() {
							m.client.Disconnect(100)
						}
					}
				}
			}
		}
	}

}

func (m *mqttConnection) onConnected(mqtt.Client) {
	atomic.StoreInt32(&m.state, int32(Connected))
	select {
	case m.stateChangedNotify <- Connected:
	default:
		m.logMessage().Debug("Connected dropped")
	}
	m.logMessage().Info("Connected")
}

func (m *mqttConnection) onConnectionLost(mqtt.Client, error) {
	atomic.StoreInt32(&m.state, int32(Disconnected))
	select {
	case m.stateChangedNotify <- Disconnected:
	default:
		m.logMessage().Debug("Disconnected dropped")
	}
	m.logMessage().Info("Disconnected")
}

func (m *mqttConnection) LastError() error {
	v := m.lastErr.Load()
	if v == nil {
		return nil
	}

	err, ok := v.(error)
	if !ok {
		return nil
	}

	return err
}

func (m *mqttConnection) Start() error {
	t := time.NewTicker(checkTokensTimeout)

	select {
	case m.commands <- commandStart:
		{
			return nil
		}

	case <-t.C:
		{
			return errors.New("can't send start command")
		}
	}
}

func (m *mqttConnection) Stop() error {
	t := time.NewTicker(checkTokensTimeout)

	select {
	case m.commands <- commandStop:
		{
			return nil
		}

	case <-t.C:
		{
			return errors.New("can't send stop command")
		}
	}
}

func (m *mqttConnection) IsConnected() bool {
	return atomic.LoadInt32(&m.state) == int32(Connected)
}

func (m *mqttConnection) Publish(topic string, payload []byte) error {
	if !m.IsConnected() {
		return errors.New("connection lost")
	}

	select {
	case m.publishMessages <- &publishMessage{
		topic:   topic,
		payload: payload,
	}:
		{
			return nil
		}
	default:
		return errors.New("queue is full")
	}
}
