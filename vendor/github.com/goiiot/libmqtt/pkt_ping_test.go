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

	std "github.com/eclipse/paho.mqtt.golang/packets"
)

var (
	testPingReqMsg       = PingReqPacket
	testPingReqMsgBytes  []byte
	testPingRespMsg      = PingRespPacket
	testPingRespMsgBytes []byte
)

func initTestData_Ping() {
	// pingreq
	reqBuf := &bytes.Buffer{}
	req := std.NewControlPacket(std.Pingreq).(*std.PingreqPacket)
	req.Write(reqBuf)
	testPingReqMsgBytes = reqBuf.Bytes()

	// pingresp
	respBuf := &bytes.Buffer{}
	resp := std.NewControlPacket(std.Pingresp).(*std.PingrespPacket)
	resp.Write(respBuf)
	testPingRespMsgBytes = respBuf.Bytes()
}

func TestPingReqPacket_Bytes(t *testing.T) {
	testPacketBytes(testPingReqMsg, testPingReqMsgBytes, t)
}

func TestPingRespPacket_Bytes(t *testing.T) {
	testPacketBytes(testPingRespMsg, testPingRespMsgBytes, t)
}
