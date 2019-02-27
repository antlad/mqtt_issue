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

// sub test data
var (
	testSubTopics   []*Topic
	testSubMsgs     []*SubscribePacket
	testSubAckMsgs  []*SubAckPacket
	testUnSubMsgs   []*UnSubPacket
	testUnSubAckMsg *UnSubAckPacket

	// mqtt 3.1.1
	testSubMsgBytesV311      [][]byte
	testSubAckMsgBytesV311   [][]byte
	testUnSubMsgBytesV311    [][]byte
	testUnSubAckMsgBytesV311 []byte

	// mqtt 5.0
	testSubMsgBytesV5      [][]byte
	testSubAckMsgBytesV5   [][]byte
	testUnSubMsgBytesV5    [][]byte
	testUnSubAckMsgBytesV5 []byte
)

func initTestData_Sub() {
	testSubTopics = make([]*Topic, len(testTopics))
	for i := range testSubTopics {
		testSubTopics[i] = &Topic{Name: testTopics[i], Qos: testTopicQos[i]}
	}

	testSubMsgs = make([]*SubscribePacket, len(testTopics))
	testSubMsgBytesV311 = make([][]byte, len(testTopics))
	testSubAckMsgs = make([]*SubAckPacket, len(testTopics))
	testSubAckMsgBytesV311 = make([][]byte, len(testTopics))
	testUnSubMsgs = make([]*UnSubPacket, len(testTopics))
	testUnSubMsgBytesV311 = make([][]byte, len(testTopics))

	for i := range testTopics {
		testSubMsgs[i] = &SubscribePacket{
			Topics:   testSubTopics[:i+1],
			PacketID: testPacketID,
		}

		subPkt := std.NewControlPacket(std.Subscribe).(*std.SubscribePacket)
		subPkt.Topics = testTopics[:i+1]
		subPkt.Qoss = testTopicQos[:i+1]
		subPkt.MessageID = testPacketID

		subBuf := &bytes.Buffer{}
		subPkt.Write(subBuf)
		testSubMsgBytesV311[i] = subBuf.Bytes()

		testSubAckMsgs[i] = &SubAckPacket{
			PacketID: testPacketID,
			Codes:    testSubAckCodes[:i+1],
		}
		subAckPkt := std.NewControlPacket(std.Suback).(*std.SubackPacket)
		subAckPkt.MessageID = testPacketID
		subAckPkt.ReturnCodes = testSubAckCodes[:i+1]
		subAckBuf := &bytes.Buffer{}
		subAckPkt.Write(subAckBuf)
		testSubAckMsgBytesV311[i] = subAckBuf.Bytes()

		testUnSubMsgs[i] = &UnSubPacket{
			PacketID:   testPacketID,
			TopicNames: testTopics[:i+1],
		}
		unsubPkt := std.NewControlPacket(std.Unsubscribe).(*std.UnsubscribePacket)
		unsubPkt.Topics = testTopics[:i+1]
		unsubPkt.MessageID = testPacketID
		unSubBuf := &bytes.Buffer{}
		unsubPkt.Write(unSubBuf)
		testUnSubMsgBytesV311[i] = unSubBuf.Bytes()
	}

	unSunAckBuf := &bytes.Buffer{}
	testUnSubAckMsg = &UnSubAckPacket{PacketID: testPacketID}
	unsubAckPkt := std.NewControlPacket(std.Unsuback).(*std.UnsubackPacket)
	unsubAckPkt.MessageID = testPacketID
	unsubAckPkt.Write(unSunAckBuf)
	testUnSubAckMsgBytesV311 = unSunAckBuf.Bytes()
}

func TestSubscribePacket_Bytes(t *testing.T) {
	for i, p := range testSubMsgs {
		p.ProtoVersion = V311
		testPacketBytes(p, testSubMsgBytesV311[i], t)
		// p.ProtoVersion = V5
		// testPacketBytes(p, testSubMsgBytesV5[i], t)
	}
}

func TestSubAckPacket_Bytes(t *testing.T) {
	for i, p := range testSubAckMsgs {
		p.ProtoVersion = V311
		testPacketBytes(p, testSubAckMsgBytesV311[i], t)
		// p.ProtoVersion = V5
		// testPacketBytes(p, testSubAckMsgBytesV5[i], t)
	}
}

func TestUnSubPacket_Bytes(t *testing.T) {
	for i, p := range testUnSubMsgs {
		p.ProtoVersion = V311
		testPacketBytes(p, testUnSubMsgBytesV311[i], t)
		// p.ProtoVersion = V5
		// testPacketBytes(p, testUnSubMsgBytesV5[i], t)
	}
}

func TestUnSubAckPacket_Bytes(t *testing.T) {
	testUnSubAckMsg.ProtoVersion = V311
	testPacketBytes(testUnSubAckMsg, testUnSubAckMsgBytesV311, t)
	// testUnSubAckMsg.ProtoVersion = V5
	// testPacketBytes(testUnSubAckMsg, testUnSubAckMsgBytesV5, t)
}
