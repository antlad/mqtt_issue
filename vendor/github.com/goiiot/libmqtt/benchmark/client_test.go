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

package benchmark

import (
	"net/url"
	"testing"

	pah "github.com/eclipse/paho.mqtt.golang"
	lib "github.com/goiiot/libmqtt"
)

const (
	testKeepalive = 3600             // prevent keepalive packet disturb
	testServer    = "localhost:1883" // emqttd broker address
	testTopic     = "/foo"
	testBufSize   = 100
	testPubCount  = 100000
)

var (
	testTopicMsg = []byte("bar")
)

func BenchmarkLibmqttClient(b *testing.B) {
	b.N = testPubCount
	b.ReportAllocs()

	client, err := lib.NewClient(
		lib.WithServer(testServer),
		lib.WithKeepalive(testKeepalive, 1.2),
		lib.WithBuf(testBufSize, testBufSize),
		lib.WithCleanSession(true),
	)

	if err != nil {
		b.Error(err)
	}

	client.HandleUnSub(func(topic []string, err error) {
		if err != nil {
			b.Error(err)
		}
		client.Destroy(true)
	})

	b.ResetTimer()
	client.Connect(func(server string, code byte, err error) {
		if err != nil {
			b.Error(err)
		} else if code != lib.CodeSuccess {
			b.Error(code)
		}
		for i := 0; i < b.N; i++ {
			client.Publish(&lib.PublishPacket{
				TopicName: testTopic,
				Payload:   testTopicMsg,
			})
		}
		client.UnSubscribe(testTopic)
	})
	client.Wait()
}

func BenchmarkPahoClient(b *testing.B) {
	b.N = testPubCount
	b.ReportAllocs()

	serverURL, err := url.Parse("tcp://" + testServer)
	if err != nil {
		b.Error(err)
	}

	client := pah.NewClient(&pah.ClientOptions{
		Servers:             []*url.URL{serverURL},
		KeepAlive:           testKeepalive,
		CleanSession:        true,
		ProtocolVersion:     4,
		MessageChannelDepth: testBufSize,
		Store:               pah.NewMemoryStore(),
	})

	b.ResetTimer()
	t := client.Connect()
	if !t.Wait() {
		b.Fail()
	}

	if err := t.Error(); err != nil {
		b.Error(err)
	}

	for i := 0; i < b.N; i++ {
		client.Publish(testTopic, 0, false, testTopicMsg)
	}

	t = client.Unsubscribe(testTopic)
	if !t.Wait() {
		b.Fail()
	}
	if err := t.Error(); err != nil {
		b.Error(err)
	}

	client.Disconnect(0)
}
