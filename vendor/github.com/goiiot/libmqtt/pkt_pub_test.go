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

// pub test data
var (
	testPubMsgs    []*PublishPacket
	testPubAckMsg  = &PubAckPacket{PacketID: testPacketID}
	testPubRecvMsg = &PubRecvPacket{PacketID: testPacketID}
	testPubRelMsg  = &PubRelPacket{PacketID: testPacketID}
	testPubCompMsg = &PubCompPacket{PacketID: testPacketID}

	// mqtt 3.1.1
	testPubMsgBytesV311     [][]byte
	testPubAckMsgBytesV311  []byte
	testPubRecvMsgBytesV311 []byte
	testPubRelMsgBytesV311  []byte
	testPubCompMsgBytesV311 []byte

	// mqtt 5.0
	testPubMsgBytesV5     [][]byte
	testPubAckMsgBytesV5  []byte
	testPubRecvMsgBytesV5 []byte
	testPubRelMsgBytesV5  []byte
	testPubCompMsgBytesV5 []byte
)

// init pub test data
func initTestData_Pub() {
	// pub
	testPubMsgs = make([]*PublishPacket, len(testTopics))
	testPubMsgBytesV311 = make([][]byte, len(testTopics))
	testPubMsgBytesV5 = make([][]byte, len(testTopics))
	for i := range testTopics {
		testPubMsgs[i] = &PublishPacket{
			IsDup:     testPubDup,
			TopicName: testTopics[i],
			Qos:       testTopicQos[i],
			Payload:   []byte(testTopicMsgs[i]),
			PacketID:  testPacketID,
		}

		// create standard publish packet and make bytes
		pkt := std.NewControlPacketWithHeader(std.FixedHeader{
			MessageType: std.Publish,
			Dup:         testPubDup,
			Qos:         testTopicQos[i],
		}).(*std.PublishPacket)
		pkt.TopicName = testTopics[i]
		pkt.Payload = []byte(testTopicMsgs[i])
		pkt.MessageID = testPacketID

		buf := &bytes.Buffer{}
		pkt.Write(buf)
		testPubMsgBytesV311[i] = buf.Bytes()
	}

	// puback
	pubAckPkt := std.NewControlPacket(std.Puback).(*std.PubackPacket)
	pubAckPkt.MessageID = testPacketID
	pubAckBuf := &bytes.Buffer{}
	pubAckPkt.Write(pubAckBuf)
	testPubAckMsgBytesV311 = pubAckBuf.Bytes()

	// pubrecv
	pubRecvBuf := &bytes.Buffer{}
	pubRecPkt := std.NewControlPacket(std.Pubrec).(*std.PubrecPacket)
	pubRecPkt.MessageID = testPacketID
	pubRecPkt.Write(pubRecvBuf)
	testPubRecvMsgBytesV311 = pubRecvBuf.Bytes()

	// pubrel
	pubRelBuf := &bytes.Buffer{}
	pubRelPkt := std.NewControlPacket(std.Pubrel).(*std.PubrelPacket)
	pubRelPkt.MessageID = testPacketID
	pubRelPkt.Write(pubRelBuf)
	testPubRelMsgBytesV311 = pubRelBuf.Bytes()

	// pubcomp
	pubCompBuf := &bytes.Buffer{}
	pubCompPkt := std.NewControlPacket(std.Pubcomp).(*std.PubcompPacket)
	pubCompPkt.MessageID = testPacketID
	pubCompPkt.Write(pubCompBuf)
	testPubCompMsgBytesV311 = pubCompBuf.Bytes()
}

func TestPublishPacket_Bytes(t *testing.T) {
	for i, p := range testPubMsgs {
		p.ProtoVersion = V311
		testPacketBytes(p, testPubMsgBytesV311[i], t)
		// p.ProtoVersion = V5
		// testPacketBytes(p, testPubMsgBytesV5[i], t)
		// testV5Bytes(p, testPubMsgBytesV5[i], t)
	}
}

func TestPubProps_Props(t *testing.T) {

}

func TestPubProps_SetProps(t *testing.T) {

}

func TestPubAckPacket_Bytes(t *testing.T) {
	testPubAckMsg.ProtoVersion = V311
	testPacketBytes(testPubAckMsg, testPubAckMsgBytesV311, t)
	// testPubAckMsg.ProtoVersion = V5
	// testPacketBytes(testPubAckMsg, testPubAckMsgBytesV5, t)
}

func TestPubAckProps_Props(t *testing.T) {

}

func TestPubAckProps_SetProps(t *testing.T) {

}

func TestPubRecvPacket_Bytes(t *testing.T) {
	testPubRecvMsg.ProtoVersion = V311
	testPacketBytes(testPubRecvMsg, testPubRecvMsgBytesV311, t)
	// testPubRecvMsg.ProtoVersion = V5
	// testPacketBytes(testPubRecvMsg, testPubRecvMsgBytesV5, t)
}

func TestPubRecvProps_Props(t *testing.T) {

}

func TestPubRecvProps_SetProps(t *testing.T) {

}

func TestPubRelPacket_Bytes(t *testing.T) {
	testPubRelMsg.ProtoVersion = V311
	testPacketBytes(testPubRelMsg, testPubRelMsgBytesV311, t)
	// testPubRelMsg.ProtoVersion = V5
	// testPacketBytes(testPubRelMsg, testPubRelMsgBytesV5, t)
}

func TestPubRelProps_Props(t *testing.T) {

}

func TestPubRelProps_SetProps(t *testing.T) {

}

func TestPubCompPacket_Bytes(t *testing.T) {
	testPubCompMsg.ProtoVersion = V311
	testPacketBytes(testPubCompMsg, testPubCompMsgBytesV311, t)
	// testPubCompMsg.ProtoVersion = V5
	// testPacketBytes(testPubCompMsg, testPubCompMsgBytesV5, t)
}

func TestPubCompProps_Props(t *testing.T) {

}

func TestPubCompProps_SetProps(t *testing.T) {

}
