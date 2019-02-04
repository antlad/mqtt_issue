package main

import (
	"bytes"
	"context"
	"fmt"
	"html/template"
	"io/ioutil"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/antlad/mqtt_issue/mqtt_conn"
	"github.com/satori/go.uuid"
)

type producerCtx struct {
	pubErrorChan       chan *cloud_conn.PublishError
	pubDoneChan        chan int32
	conn               cloud_conn.CloudConnection
	stateChangedNotify chan cloud_conn.ConnectionState
}

const (
	freq     = 100
	pubCount = 100
)

func getEnv(env string, val string) string {
	actual := os.Getenv(env)
	if len(actual) == 0 {
		return val
	}

	return actual
}

var (
	mqttTopic   = getEnv("MQTT_TOPIC", "topic1")
	msgTemplate = `{"success": true, "datatype": "float", "timestamp": {{ unixNow }}, "registerId": "{{ randomUUID }}", "value": {{ nextValue }}, "deviceID": "68AD6ADD-86B6-443B-B6EB-9C0275622649", "tagName": "ttt"}`

	funcMap = template.FuncMap{
		"unixNow":    unixNow,
		"randomUUID": randomUUID,
		"nextValue":  nextValue,
	}

	value = 0
)

func randomUUID() string {
	return uuid.NewV4().String()
}

func nextValue() string {
	value += 1
	return strconv.Itoa(value)
}

func unixNow() string {
	return strconv.FormatInt(time.Now().Unix(), 10)
}

func produceMessage(t *template.Template) ([]byte, error) {
	buffer := bytes.NewBuffer(nil)
	err := t.Execute(buffer, nil)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func producer(ctx context.Context, pctx *producerCtx) {

	tmpl := template.New("").Funcs(funcMap)
	tmplGet := template.Must(tmpl.Parse(msgTemplate))

	var done int32 = 0
	var failed int32 = 0
	var sent int32 = 0
	counter := 0
	producerStats := time.NewTicker(time.Second)
	t := time.NewTicker(time.Duration(float64(time.Second) / float64(freq)))

	for {
		select {
		case <-t.C:
			{

				for i := 0; i < pubCount; i++ {
					data, err := produceMessage(tmplGet)
					if err != nil {
						panic(fmt.Sprintf("produce message failed %s", err.Error()))
					}

					err = pctx.conn.Publish(mqttTopic, data)
					counter++
					sent++
					if err != nil {
						failed++
						time.Sleep(time.Millisecond) // imitate of work on persist
					}
				}
			}
		case <-producerStats.C:
			{
				fmt.Println(fmt.Sprintf("+++++++++ sent: %d, done: %d, failed: %d ++++++++++++++++++++", sent, done, failed))
				done = 0
				failed = 0
				sent = 0
			}
		case <-ctx.Done():
			{
				logrus.Info("exiting producer")
				return
			}
		case d := <-pctx.pubDoneChan:
			{
				done += d
			}
		case <-pctx.pubErrorChan:
			{
				failed += 1
				time.Sleep(time.Millisecond) // imitate of work on persist
			}
		case s := <-pctx.stateChangedNotify:
			{
				switch s {
				case cloud_conn.Disconnected:
					{
						logrus.Info("event disconnected")

					}
				case cloud_conn.Connected:
					{
						logrus.Info("event connected")
					}
				}
			}
		}
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	pctx := &producerCtx{
		pubErrorChan:       make(chan *cloud_conn.PublishError),
		pubDoneChan:        make(chan int32),
		stateChangedNotify: make(chan cloud_conn.ConnectionState),
	}

	logrus.SetLevel(logrus.DebugLevel)

	cert, err := ioutil.ReadFile("mqtt_srv/m2mqtt_ca.crt")
	if err != nil {
		logrus.Error("read cert failed ", err.Error())
		return
	}

	connection, err := cloud_conn.NewConnection(&cloud_conn.CloudConnectionOptions{
		Host:               "127.0.0.1",
		Scheme:             "ssl",
		Port:               8883,
		ClientID:           "id1",
		UserName:           "user1",
		Password:           "password",
		SslRootCA:          string(cert),
		PubErrorChan:       pctx.pubErrorChan,
		PubDoneChan:        pctx.pubDoneChan,
		StateChangedNotify: pctx.stateChangedNotify,
	})

	if err != nil {
		logrus.Error("create connection failed ", err.Error())
		return
	}
	pctx.conn = connection

	go connection.Run(ctx)
	go producer(ctx, pctx)

	if err := connection.Start(); err != nil {
		logrus.Errorf("start failed %s", err.Error())
		cancel()
		return
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)

	go func() {
		s := <-sig
		logrus.Infof("signal %s received", s.String())
		cancel()
	}()

	<-ctx.Done()
	time.Sleep(time.Second)
}
