package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/antlad/mqtt_issue/mqtt_conn"
)

type producerCtx struct {
	pubErrorChan chan *cloud_conn.PublishError
	pubDoneChan  chan int32
	conn         cloud_conn.CloudConnection
}

const (
	freq = 10000
)

func getEnv(env string, val string) string {
	actual := os.Getenv(env)
	if len(actual) == 0 {
		return val
	}

	return actual
}

var (
	mqttTopic = getEnv("MQTT_TOPIC", "topic1")
)

func producer(ctx context.Context, pctx *producerCtx) {
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
				err := pctx.conn.Publish(mqttTopic, []byte(fmt.Sprintf("message %d", counter)))
				counter++
				sent++
				if err != nil {
					failed++
				}
			}
		case <-producerStats.C:
			{
				fmt.Println(fmt.Sprintf("+++++++++ sent: %d, done: %d, failed: %d ++++++++++++++++++++", sent, done, failed))
				done = 0
				failed = 0
				sent = 0
				producerStats = time.NewTicker(time.Second)
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
			}
		}
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	pctx := &producerCtx{
		pubErrorChan: make(chan *cloud_conn.PublishError, 1),
		pubDoneChan:  make(chan int32, 1),
	}

	logrus.SetLevel(logrus.DebugLevel)

	cert, err := ioutil.ReadFile("mqtt_srv/m2mqtt_ca.crt")
	if err != nil {
		logrus.Error("read cert failed ", err.Error())
		return
	}

	connection, err := cloud_conn.NewConnection(&cloud_conn.CloudConnectionOptions{
		Host:         "127.0.0.1",
		Scheme:       "ssl",
		Port:         8883,
		ClientID:     "id1",
		UserName:     "user1",
		Password:     "password",
		SslRootCA:    string(cert),
		PubErrorChan: pctx.pubErrorChan,
		PubDoneChan:  pctx.pubDoneChan,
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
