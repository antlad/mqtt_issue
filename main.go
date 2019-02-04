package main

import (
	"context"
	"io/ioutil"
	"os"
	"os/signal"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/antlad/mqtt_issue/mqtt_conn"
)

type producerCtx struct {
	pubErrorChan       chan *cloud_conn.PublishError
	pubDoneChan        chan int32
	conn cloud_conn.CloudConnection
}

func producer(ctx context.Context, pctx *producerCtx) {
	for{
		select {
		case <-ctx.Done():{
			logrus.Info("exiting producer")
			return
		}
		case d := <-pctx.pubDoneChan:{
			logrus.Infof("publish done for %d messages", d)
		}


		}
	}
}



func main() {
	ctx, cancel := context.WithCancel(context.Background())

	pctx := &producerCtx{
		pubErrorChan: make(chan *cloud_conn.PublishError, 1),
		pubDoneChan: make(chan int32, 1),
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
