package cloud_conn

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/goiiot/libmqtt"
)

type AnotherConnection struct {
	client           libmqtt.Client
	publishMessages  chan *publishMessage
	pubErrorChan     chan *PublishError
	pubDoneChan      chan int32
	reconnectEnabled bool
	commands         chan command
}

func NewAnotherConnection(conf *CloudConnectionOptions) (CloudConnection, error) {
	connectOptions := []libmqtt.Option{
		libmqtt.WithClientID(conf.ClientID),
		libmqtt.WithServer(fmt.Sprintf("%s:%d", conf.Host, conf.Port)),
		libmqtt.WithKeepalive(10, 1.2),
		libmqtt.WithAutoReconnect(false),
		libmqtt.WithCleanSession(true),
		libmqtt.WithBackoffStrategy(time.Second, 5*time.Second, 1.2),
		// use RegexRouter for topic routing if not specified
		// will use TextRouter, which will match full text
		libmqtt.WithRouter(libmqtt.NewRegexRouter()),
		libmqtt.WithIdentity(conf.UserName, conf.Password),
		libmqtt.WithLog(libmqtt.Verbose),
	}
	if conf.Scheme == "ssl" || conf.Scheme == "https" {
		TLSConfig := &tls.Config{}
		TLSConfig.InsecureSkipVerify = true
		if len(conf.SslRootCA) > 0 {
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM([]byte(conf.SslRootCA)) {
				return nil, errors.New("bad config for SSL Root CA")
			}
			TLSConfig.RootCAs = pool
		} else {
			once.Do(func() {
				var err error
				systemPool, err = x509.SystemCertPool()
				if err != nil {
					logrus.Error(fmt.Sprintf("system cert pool failed %s", err.Error()))
				}
			})
			TLSConfig.RootCAs = systemPool
		}
		connectOptions = append(connectOptions, libmqtt.WithCustomTLS(TLSConfig))
	}
	client, err := libmqtt.NewClient(connectOptions...)
	client.HandleNet(func(server string, err error) {
		logrus.Debug("handle net", "server", server, "error", err)
	})
	ch := make(chan error, 0)
	client.Connect(func(server string, code byte, err error) {
		logrus.Debug("here", "s", server, "c", code, "e", err)
		ch <- err
	})
	select {
	case err = <-ch:
	}
	return &AnotherConnection{
		client:           client,
		pubErrorChan:     conf.PubErrorChan,
		pubDoneChan:      conf.PubDoneChan,
		reconnectEnabled: false,
		commands:         make(chan command),
		publishMessages:  make(chan *publishMessage),
	}, err
}

func (m *AnotherConnection) logMessage() *logrus.Entry {
	return logrus.WithFields(logrus.Fields{
		"component": "mqtt_connection",
	})
}

func (m *AnotherConnection) Run(ctx context.Context) {
	messages := make([]*publishMessage, 0)
	reconnectTimer := time.NewTicker(reconnectTimeoutStep)
	cycleTimer := time.NewTicker(time.Second)

	sendBack := func(t *publishMessage, err error) {
		select {
		case m.pubErrorChan <- &PublishError{
			Err:     err,
			topic:   t.topic,
			Payload: t.payload,
		}:
		default:

		}
	}

	m.client.HandlePub(func(topic string, err error) {
		msg := messages[0]
		messages = messages[1:]
		m.logMessage().WithField("error", err)
		if err != nil {
			sendBack(msg, err)
		} else {
			m.pubDoneChan <- 1
		}
	})

	doConnect := func() {
		ch := make(chan error, 1)
		m.client.Connect(func(server string, code byte, err error) {
			ch <- err
		})
		var err error
		select {
		case err = <-ch:
		}
		if err != nil {
			// m.lastErr.Store(t.Error())
			m.logMessage().WithField("error", err).Error("connect failed")
		} else {
			m.logMessage().Info("connect done")
		}
	}

	for {
		select {
		case <-cycleTimer.C:
			{
				fmt.Println("---------------------------------------")
			}
		case <-reconnectTimer.C:
			{
				if m.reconnectEnabled && !m.IsConnected() {
					m.logMessage().Info("try to reconnect")
					doConnect()
				}
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

				// fmt.Println("+++ before publish")
				m.client.Publish(&libmqtt.PublishPacket{
					TopicName: msg.topic,
					Qos:       mqttQOS,
					Payload:   msg.payload,
				})
				messages = append(messages, msg)
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
							m.client.Destroy(true)
						}
					}
				}
			}
		}
	}
}

func (m *AnotherConnection) Stop() error {
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

func (m *AnotherConnection) Start() error {
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

func (m *AnotherConnection) IsConnected() bool {
	return true
}

func (*AnotherConnection) LastError() error {
	return nil
}

func (m *AnotherConnection) Publish(topic string, payload []byte) error {
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
