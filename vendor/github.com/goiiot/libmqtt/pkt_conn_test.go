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

// conn test data
var (
	testConnWillMsg = &ConnPacket{
		BasePacket:   BasePacket{ProtoVersion: testProtoVersion},
		Username:     testUsername,
		Password:     testPassword,
		ClientID:     testClientID,
		CleanSession: testCleanSession,
		IsWill:       testWill,
		WillQos:      testWillQos,
		WillRetain:   testWillRetain,
		WillTopic:    testWillTopic,
		WillMessage:  testWillMessage,
		Keepalive:    testKeepalive,
	}

	testConnMsg = &ConnPacket{
		BasePacket:   BasePacket{ProtoVersion: testProtoVersion},
		Username:     testUsername,
		Password:     testPassword,
		ClientID:     testClientID,
		CleanSession: testCleanSession,
		Keepalive:    testKeepalive,
	}

	testConnAckMsg = &ConnAckPacket{
		Present: testConnackPresent,
		Code:    testConnackCode,
	}

	testDisConnMsg = &DisConnPacket{}

	// mqtt 3.1.1
	testConnWillMsgBytesV311 []byte
	testConnMsgBytesV311     []byte
	testConnAckMsgBytesV311  []byte
	testDisConnMsgBytesV311  []byte

	// mqtt 5.0
	testConnWillMsgBytesV5 []byte
	testConnMsgBytesV5     []byte
	testConnAckMsgBytesV5  []byte
	testDisConnMsgBytesV5  []byte
)

func initTestData_Conn() {
	// conn (with will)
	connWillPkt := std.NewControlPacket(std.Connect).(*std.ConnectPacket)
	connWillPkt.Username = testUsername
	connWillPkt.UsernameFlag = true
	connWillPkt.Password = []byte(testPassword)
	connWillPkt.PasswordFlag = true
	connWillPkt.ProtocolName = "MQTT"
	connWillPkt.ProtocolVersion = byte(testProtoVersion)
	connWillPkt.ClientIdentifier = testClientID
	connWillPkt.CleanSession = testCleanSession
	connWillPkt.WillFlag = testWill
	connWillPkt.WillQos = testWillQos
	connWillPkt.WillRetain = testWillRetain
	connWillPkt.WillTopic = testWillTopic
	connWillPkt.WillMessage = testWillMessage
	connWillPkt.Keepalive = testKeepalive
	connWillBuf := &bytes.Buffer{}
	connWillPkt.Write(connWillBuf)
	testConnWillMsgBytesV311 = connWillBuf.Bytes()

	// conn (no will)
	connPkt := std.NewControlPacket(std.Connect).(*std.ConnectPacket)
	connPkt.Username = testUsername
	connPkt.UsernameFlag = true
	connPkt.Password = []byte(testPassword)
	connPkt.PasswordFlag = true
	connPkt.ProtocolName = "MQTT"
	connPkt.ProtocolVersion = byte(testProtoVersion)
	connPkt.ClientIdentifier = testClientID
	connPkt.CleanSession = testCleanSession
	connPkt.Keepalive = testKeepalive
	connBuf := &bytes.Buffer{}
	connPkt.Write(connBuf)
	testConnMsgBytesV311 = connBuf.Bytes()

	// connack
	connackPkt := std.NewControlPacket(std.Connack).(*std.ConnackPacket)
	connackPkt.SessionPresent = testConnackPresent
	connackPkt.ReturnCode = testConnackCode
	connAckBuf := &bytes.Buffer{}
	connackPkt.Write(connAckBuf)
	testConnAckMsgBytesV311 = connAckBuf.Bytes()

	// disconn
	disConnPkt := std.NewControlPacket(std.Disconnect).(*std.DisconnectPacket)
	disconnBuf := &bytes.Buffer{}
	disConnPkt.Write(disconnBuf)
	testDisConnMsgBytesV311 = disconnBuf.Bytes()
}

func TestConnPacket_Bytes(t *testing.T) {
	testConnMsg.ProtoVersion = V311
	testPacketBytes(testConnMsg, testConnMsgBytesV311, t)
	// testConnMsg.ProtoVersion = V5
	// testV5Bytes(testConnMsg, testConnMsgBytesV5, t)
}

func TestConnWillPacket_Bytes(t *testing.T) {
	testConnWillMsg.ProtoVersion = V311
	testPacketBytes(testConnWillMsg, testConnWillMsgBytesV311, t)
	// testConnWillMsg.ProtoVersion = V5
	// testV5Bytes(testConnWillMsg, testConnWillMsgBytesV5, t)
}

func TestConnProps_Props(t *testing.T) {

}

func TestConnProps_SetProps(t *testing.T) {

}

func TestConnAckPacket_Bytes(t *testing.T) {
	testConnAckMsg.ProtoVersion = V311
	testPacketBytes(testConnAckMsg, testConnAckMsgBytesV311, t)
	// testConnAckMsg.ProtoVersion = V5
	// testV5Bytes(testConnAckMsg, testConnAckMsgBytesV5, t)
}

func TestConnAckProps_Props(t *testing.T) {

}

func TestConnAckProps_SetProps(t *testing.T) {

}

func TestDisConnPacket_Bytes(t *testing.T) {
	testDisConnMsg.ProtoVersion = V311
	testPacketBytes(testDisConnMsg, testDisConnMsgBytesV311, t)
	// testDisConnMsg.ProtoVersion = V5
	// testV5Bytes(testDisConnMsg, testDisConnMsgBytesV5, t)
}

func TestDisConnProps_Props(t *testing.T) {

}

func TestDisConnProps_SetProps(t *testing.T) {

}
