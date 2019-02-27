/*
 * Copyright Go-IIoT (https://github.com/goiiot)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package libmqtt

import (
	"bytes"
	"testing"
	"time"

	"go.uber.org/goleak"
)

// test with emqx server (http://emqtt.io/ or https://github.com/emqx/emqx)
// the server is configured with default configuration

type extraHandler struct {
	afterPubSuccess   func()
	afterSubSuccess   func()
	afterUnSubSuccess func()
}

func plainClient(t *testing.T, exH *extraHandler) Client {
	c, err := NewClient(
		WithLog(Verbose),
		WithServer("localhost:1883"),
		WithDialTimeout(10),
		WithKeepalive(10, 1.2),
		WithIdentity("admin", "public"),
		WithWill("test", Qos0, false, []byte("test data")),
		WithAutoReconnect(true),
		WithBackoffStrategy(1*time.Second, 5*time.Second, 1.5),
	)

	if err != nil {
		t.Error(err)
	}
	initClient(c, exH, t)
	return c
}

func tlsClient(t *testing.T, exH *extraHandler) Client {
	c, err := NewClient(
		WithLog(Verbose),
		WithServer("localhost:8883"),
		WithTLS(
			"./testdata/client-cert.pem",
			"./testdata/client-key.pem",
			"./testdata/ca-cert.pem",
			"MacBook-Air.local",
			true),
		WithDialTimeout(10),
		WithKeepalive(10, 1.2),
		WithIdentity("admin", "public"),
		WithWill("test", Qos0, false, []byte("test data")),
		WithAutoReconnect(true),
		WithBackoffStrategy(1*time.Second, 5*time.Second, 1.5),
	)

	if err != nil {
		t.Error(err)
	}
	initClient(c, exH, t)
	return c
}

func initClient(c Client, exH *extraHandler, t *testing.T) {
	c.HandlePub(func(topic string, err error) {
		println("exH.Pub")
		if err != nil {
			t.Error(err)
		}

		if exH != nil && exH.afterPubSuccess != nil {
			println("afterPubSuccess()")
			exH.afterPubSuccess()
		}
	})

	c.HandleSub(func(topics []*Topic, err error) {
		println("exH.Sub")
		if err != nil {
			t.Error(err)
		}

		if exH != nil && exH.afterSubSuccess != nil {
			println("afterSubSuccess()")
			exH.afterSubSuccess()
		}
	})

	c.HandleUnSub(func(topics []string, err error) {
		println("exH.UnSub")
		if err != nil {
			t.Error(err)
		}

		if exH != nil && exH.afterUnSubSuccess != nil {
			println("afterUnSubSuccess()")
			exH.afterUnSubSuccess()
		}
	})

	c.HandleNet(func(server string, err error) {
		if err != nil {
			t.Error(err)
		}
	})
}

func conn(c Client, t *testing.T, afterConnSuccess func()) {
	c.Connect(func(server string, code byte, err error) {
		if err != nil {
			t.Errorf("connect errored: %v", err)
		}

		if code != CodeSuccess {
			t.Errorf("connect failed with code: %d", code)
		}

		if afterConnSuccess != nil {
			println("afterConnSuccess()")
			afterConnSuccess()
		}
	})
}

func handleTopicAndSub(c Client, t *testing.T) {
	for i := range testTopics {
		c.Handle(testTopics[i], func(topic string, maxQos byte, msg []byte) {
			if maxQos != testPubMsgs[i].Qos || bytes.Compare(testPubMsgs[i].Payload, msg) != 0 {
				t.Errorf("fail at sub topic = %v, content unexpected, payload = %v, target payload = %v",
					topic, string(msg), string(testPubMsgs[i].Payload))
			}
		})
	}

	c.Subscribe(testSubTopics...)
}

func TestNewClient(t *testing.T) {
	_, err := NewClient()
	if err == nil {
		t.Errorf("create new client with no server should fail")
	}

	_, err = NewClient(
		WithTLS("foo", "bar", "foobar", "foo.bar", true),
	)
	if err == nil {
		t.Errorf("create new client with invalid tls should fail")
	}

	goleak.VerifyNoLeaks(t)
}

// conn
func TestClient_Connect(t *testing.T) {
	var c Client
	afterConn := func() {
		c.Destroy(true)
	}

	c = plainClient(t, nil)

	conn(c, t, afterConn)
	c.Wait()

	// test client with tls
	c = tlsClient(t, nil)
	conn(c, t, afterConn)
	c.Wait()

	goleak.VerifyNoLeaks(t)
}

// conn -> pub
func TestClient_Publish(t *testing.T) {
	var c Client
	afterConn := func() {
		c.Publish(testPubMsgs...)
	}

	exH := &extraHandler{
		afterPubSuccess: func() {
			c.Destroy(true)
		},
	}

	c = plainClient(t, exH)
	conn(c, t, afterConn)
	c.Wait()

	c = tlsClient(t, exH)
	conn(c, t, afterConn)
	c.Wait()

	goleak.VerifyNoLeaks(t)
}

// conn -> sub -> pub
func TestClient_Subscribe(t *testing.T) {
	var c Client
	afterConn := func() {
		handleTopicAndSub(c, t)
	}

	extH := &extraHandler{
		afterSubSuccess: func() {
			c.Publish(testPubMsgs...)
		},
		afterPubSuccess: func() {
			c.Destroy(true)
		},
	}

	c = plainClient(t, extH)
	conn(c, t, afterConn)
	c.Wait()

	c = tlsClient(t, extH)
	conn(c, t, afterConn)
	c.Wait()

	goleak.VerifyNoLeaks(t)
}

// conn -> sub -> pub -> unSub
func TestClient_UnSubscribe(t *testing.T) {
	var c Client
	afterConn := func() {
		handleTopicAndSub(c, t)
	}

	extH := &extraHandler{
		afterSubSuccess: func() {
			c.UnSubscribe(testTopics...)
		},
		afterUnSubSuccess: func() {
			c.Destroy(true)
		},
	}

	c = plainClient(t, extH)
	conn(c, t, afterConn)
	c.Wait()

	c = tlsClient(t, extH)
	conn(c, t, afterConn)
	c.Wait()

	goleak.VerifyNoLeaks(t)
}
