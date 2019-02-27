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
	"os"
	"sync/atomic"
	"testing"
	"time"
)

var (
	// drop packets
	testPersistStrategy = &PersistStrategy{
		Interval:         time.Second,
		MaxCount:         1,
		DropOnExceed:     true,
		DuplicateReplace: false,
	}

	testPersistKeys    = []string{"foo", "foo", "bar"}
	testPersistPackets = []Packet{
		&SubscribePacket{
			Topics: []*Topic{
				{Name: "test"},
			},
		},
		&PublishPacket{},
		&ConnPacket{},
	}
)

func testPersist(p PersistMethod, t *testing.T) {
	if _, ok := p.Load(testPersistKeys[len(testPersistKeys)-1]); ok {
		t.Error("persist strategy failed")
	}

	if v, ok := p.Load(testPersistKeys[0]); !ok {
		t.Error("load persisted packet fail, packet =", v)
	} else {
		if v.Type() == CtrlSubscribe {
			if v.(*SubscribePacket).Topics[0].Name !=
				testPersistPackets[0].(*SubscribePacket).Topics[0].Name {
				t.Error("source topic name =", v.(*SubscribePacket).Topics[0].Name,
					"target topic name =", testPersistPackets[0].(*SubscribePacket).Topics[0].Name)
			}
		}
	}
}

func TestMemPersist(t *testing.T) {
	tmp := NewMemPersist(testPersistStrategy)
	p := tmp.(*memPersist)
	for i, k := range testPersistKeys {
		if err := p.Store(k, testPersistPackets[i]); err != nil {
			if err != ErrPacketDroppedByStrategy {
				t.Error(err)
			}
		}
	}

	if p.n != 1 {
		t.Error("persist strategy failed, count =", p.n)
	}

	testPersist(p, t)
}

func TestFilePersist(t *testing.T) {
	dirPath := "test-file-persist"
	err := os.MkdirAll(dirPath, 0755)
	if err != nil {
		t.Error(err)
	}

	tmp := NewFilePersist(dirPath, testPersistStrategy)
	p := tmp.(*filePersist)

	for i, k := range testPersistKeys {
		if err := p.Store(k, testPersistPackets[i]); err != nil {
			if err != ErrPacketDroppedByStrategy {
				t.Error(err)
			}
		}
	}

	if atomic.LoadUint32(&p.n) == 1 {
		t.Error()
	}

	<-time.After(750 * time.Millisecond)

	if atomic.LoadUint32(&p.n) == 1 {
		t.Error()
	}

	<-time.After(750 * time.Millisecond)

	if atomic.LoadUint32(&p.n) != 1 {
		t.Error("persist strategy failed, count =", p.n)
	}

	testPersist(p, t)
	err = os.RemoveAll(dirPath)
	if err != nil {
		t.Error(err)
	}
}
