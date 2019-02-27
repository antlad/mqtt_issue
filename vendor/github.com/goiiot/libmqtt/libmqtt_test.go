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
	"math"
	"testing"
)

// common test data
const (
	testUsername       = "foo"
	testPassword       = "bar"
	testClientID       = "1"
	testCleanSession   = true
	testWill           = true
	testWillQos        = Qos2
	testWillRetain     = true
	testWillTopic      = "foo"
	testKeepalive      = uint16(1)
	testProtoVersion   = V311
	testPubDup         = false
	testPacketID       = math.MaxUint16 / 2
	testConnackPresent = true
	testConnackCode    = byte(CodeSuccess)
)

var (
	testWillMessage = []byte("bar")
	testSubAckCodes = []byte{SubOkMaxQos0, SubOkMaxQos1, SubFail}
	testTopics      = []string{"/test", "/test/foo", "/test/bar"}
	testTopicQos    = []QosLevel{Qos0, Qos1, Qos2}
	testTopicMsgs   = []string{"test data qos0", "foo data qos1", "bar data qos2"}
)

func testPacketBytes(pkt Packet, target []byte, t *testing.T) {
	data := pkt.Bytes()
	if bytes.Compare(data, target) != 0 {
		t.Errorf("packet mismatch\nGenerated:%v\nTarget:%v", data, target)
	}
}

func init() {
	initTestData_Auth()
	initTestData_Ping()
	initTestData_Conn()
	initTestData_Sub()
	initTestData_Pub()
}
