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

func TestIdGenerator_next(t *testing.T) {
	gen := newIDGenerator()
	pkt := &PublishPacket{}

	for i := uint32(1); i < math.MaxUint16+100; i++ {
		id := gen.next(pkt)

		if i <= math.MaxUint16 {
			if id == 0 {
				t.Errorf("generated id shouldn't be 0: %d", id)
			}
			if i != uint32(id) {
				t.Errorf("generated id not match: target = %d, generated = %d", i, id)
			}

			if _, ok := gen.getExtra(id); !ok {
				t.Errorf("extra info not stored")
			}
		}

		if i > math.MaxUint16 {
			if id != 0 {
				t.Errorf("generated id should be 0: %d", id)
			}

			if _, ok := gen.getExtra(id); ok {
				t.Errorf("extra info stored")
			}
		}
	}
}

func BenchmarkIdGenerator_next(b *testing.B) {
	gen := newIDGenerator()
	pkt := &PublishPacket{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := gen.next(pkt)
		gen.free(id)
	}
}

func TestBoolToByte(t *testing.T) {
	if boolToByte(false) != 0x00 {
		t.Fail()
	}

	if boolToByte(true) != 0x01 {
		t.Fail()
	}
}

func TestGetRawProps(t *testing.T) {
	props := []byte{
		propKeyPayloadFormatIndicator, 1,
		propKeyMessageExpiryInterval, 1, 1, 1, 1,
		propKeyContentType, 0, 4, 'M', 'Q', 'T', 'T',
		propKeyRespTopic, 0, 4, 'M', 'Q', 'T', 'T',
		propKeyCorrelationData, 0, 4, 'M', 'Q', 'T', 'T',
		propKeySubID, 0,
		propKeySessionExpiryInterval, 1, 1, 1, 1,
		propKeyAssignedClientID, 0, 4, 'M', 'Q', 'T', 'T',
		propKeyServerKeepalive, 1, 1,
		propKeyAuthMethod, 0, 4, 'M', 'Q', 'T', 'T',
		propKeyAuthData, 0, 4, 'M', 'Q', 'T', 'T',
		propKeyReqProblemInfo, 1,
		propKeyWillDelayInterval, 1, 1, 1, 1,
		propKeyReqRespInfo, 1,
		propKeyRespInfo, 0, 4, 'M', 'Q', 'T', 'T',
		propKeyServerRef, 0, 4, 'M', 'Q', 'T', 'T',
		propKeyReasonString, 0, 4, 'M', 'Q', 'T', 'T',
		propKeyMaxRecv, 1, 1,
		propKeyMaxTopicAlias, 1, 1,
		propKeyTopicAlias, 1, 1,
		propKeyMaxQos, 1,
		propKeyRetainAvail, 1,
		propKeyMaxPacketSize, 1, 1, 1, 1,
		propKeyWildcardSubAvail, 1,
		propKeySubIDAvail, 1,
		propKeySharedSubAvail, 1,

		// user props
		propKeyUserProps, 0, 2, 'M', 'Q', 0, 2, 'M', 'Q',
		propKeyUserProps, 0, 2, 'M', 'Q', 0, 2, 'T', 'T',
		propKeyUserProps, 0, 2, 'T', 'T', 0, 4, 'M', 'Q', 'T', 'T',
	}

	payload := []byte{0, 0, 0, 0, 0, 0, 0, 0}

	buf := &bytes.Buffer{}
	// prop length
	writeVarInt(len(props), buf)
	buf.Write(props)
	buf.Write(payload)

	rawProps, next, err := getRawProps(buf.Bytes())
	if err != nil {
		t.Error(err)
	}

	if bytes.Compare(payload, next) != 0 {
		t.Error("payload decode error")
		t.Log(next)
	}

	if v, ok := rawProps[propKeyPayloadFormatIndicator]; ok {
		if len(v) != 1 {
			t.Error("propKeyPayloadFormatIndicator error", v)
		}
	} else {
		t.Error("propKeyPayloadFormatIndicator not decoded")
	}

	if v, ok := rawProps[propKeyMessageExpiryInterval]; ok {
		if len(v) != 4 {
			t.Error("propKeyMessageExpiryInterval error", v)
		}
	} else {
		t.Error("propKeyMessageExpiryInterval not decoded")
	}

	if v, ok := rawProps[propKeyContentType]; ok {
		str, next, err := getStringData(v)
		if err != nil {
			t.Error("propKeyContentType err != nil", err)
		}
		if len(next) != 0 {
			t.Error("propKeyContentType len(next) != 0", next)
		}
		if str != "MQTT" {
			t.Error("propKeyContentType str error", str)
		}
	} else {
		t.Error("propKeyContentType not decoded")
	}

	if v, ok := rawProps[propKeyRespTopic]; ok {
		str, next, err := getStringData(v)
		if err != nil {
			t.Error("propKeyRespTopic err != nil", err)
		}
		if len(next) != 0 {
			t.Error("propKeyRespTopic len(next) != 0", next)
		}
		if str != "MQTT" {
			t.Error("propKeyRespTopic str error", str)
		}
	} else {
		t.Error("propKeyRespTopic not decoded")
	}

	if v, ok := rawProps[propKeyCorrelationData]; ok {
		data, next, err := getBinaryData(v)
		if err != nil {
			t.Error("propKeyCorrelationData err != nil", err)
		}
		if len(next) != 0 {
			t.Error("propKeyCorrelationData len(next) != 0", next)
		}
		if string(data) != "MQTT" {
			t.Error("propKeyCorrelationData data error", data)
		}
	} else {
		t.Error("propKeyCorrelationData not decoded")
	}

	if v, ok := rawProps[propKeySubID]; ok {
		if len(v) != 1 {
			t.Error("propKeySubID error", v)
		}
	} else {
		t.Error("propKeySubID not decoded")
	}

	if v, ok := rawProps[propKeySessionExpiryInterval]; ok {
		if len(v) != 4 {
			t.Error("propKeySessionExpiryInterval error", v)
		}
	} else {
		t.Error("propKeySessionExpiryInterval not decoded")
	}

	if v, ok := rawProps[propKeyAssignedClientID]; ok {
		str, next, err := getStringData(v)
		if err != nil {
			t.Error("propKeyAssignedClientID err != nil", err)
		}
		if len(next) != 0 {
			t.Error("propKeyAssignedClientID len(next) != 0", next)
		}
		if str != "MQTT" {
			t.Error("propKeyAssignedClientID str error", str)
		}
	} else {
		t.Error("propKeyAssignedClientID not decoded")
	}

	if v, ok := rawProps[propKeyServerKeepalive]; ok {
		if len(v) != 2 {
			t.Error("propKeyServerKeepalive error", v)
		}
	} else {
		t.Error("propKeyServerKeepalive not decoded")
	}

	if v, ok := rawProps[propKeyAuthMethod]; ok {
		str, next, err := getStringData(v)
		if err != nil {
			t.Error("propKeyAuthMethod err != nil", err)
		}
		if len(next) != 0 {
			t.Error("propKeyAuthMethod len(next) != 0", next)
		}
		if str != "MQTT" {
			t.Error("propKeyAuthMethod str error", str)
		}
	} else {
		t.Error("propKeyAuthMethod not decoded")
	}

	if v, ok := rawProps[propKeyAuthData]; ok {
		data, next, err := getBinaryData(v)
		if err != nil {
			t.Error("propKeyAuthData err != nil", err)
		}
		if len(next) != 0 {
			t.Error("propKeyAuthData len(next) != 0", next)
		}
		if string(data) != "MQTT" {
			t.Error("propKeyAuthData data error", data)
		}
	} else {
		t.Error("propKeyAuthData not decoded")
	}

	if v, ok := rawProps[propKeyReqProblemInfo]; ok {
		if len(v) != 1 {
			t.Error("propKeyReqProblemInfo error", v)
		}
	} else {
		t.Error("propKeyReqProblemInfo not decoded")
	}

	if v, ok := rawProps[propKeyWillDelayInterval]; ok {
		if len(v) != 4 {
			t.Error("propKeyWillDelayInterval error", v)
		}
	} else {
		t.Error("propKeyWillDelayInterval not decoded")
	}

	if v, ok := rawProps[propKeyReqRespInfo]; ok {
		if len(v) != 1 {
			t.Error("propKeyReqRespInfo error", v)
		}
	} else {
		t.Error("propKeyReqRespInfo not decoded")
	}

	if v, ok := rawProps[propKeyRespInfo]; ok {
		str, next, err := getStringData(v)
		if err != nil {
			t.Error("propKeyRespInfo err != nil", err)
		}
		if len(next) != 0 {
			t.Error("propKeyRespInfo len(next) != 0", next)
		}
		if str != "MQTT" {
			t.Error("propKeyRespInfo str error", str)
		}
	} else {
		t.Error("propKeyRespInfo not decoded")
	}

	if v, ok := rawProps[propKeyServerRef]; ok {
		str, next, err := getStringData(v)
		if err != nil {
			t.Error("propKeyServerRef err != nil", err)
		}
		if len(next) != 0 {
			t.Error("propKeyServerRef len(next) != 0", next)
		}
		if str != "MQTT" {
			t.Error("propKeyServerRef str error", str)
		}
	} else {
		t.Error("propKeyServerRef not decoded")
	}

	if v, ok := rawProps[propKeyReasonString]; ok {
		str, next, err := getStringData(v)
		if err != nil {
			t.Error("propKeyReasonString err != nil", err)
		}
		if len(next) != 0 {
			t.Error("propKeyReasonString len(next) != 0", next)
		}
		if str != "MQTT" {
			t.Error("propKeyReasonString str error", str)
		}
	} else {
		t.Error("propKeyReasonString not decoded")
	}

	if v, ok := rawProps[propKeyMaxRecv]; ok {
		if len(v) != 2 {
			t.Error("propKeyMaxRecv error", v)
		}
	} else {
		t.Error("propKeyMaxRecv not decoded")
	}

	if v, ok := rawProps[propKeyMaxTopicAlias]; ok {
		if len(v) != 2 {
			t.Error("propKeyMaxTopicAlias error", v)
		}
	} else {
		t.Error("propKeyMaxTopicAlias not decoded")
	}

	if v, ok := rawProps[propKeyTopicAlias]; ok {
		if len(v) != 2 {
			t.Error("propKeyTopicAlias error", v)
		}
	} else {
		t.Error("propKeyTopicAlias not decoded")
	}

	if v, ok := rawProps[propKeyMaxQos]; ok {
		if len(v) != 1 {
			t.Error("propKeyMaxQos error", v)
		}
	} else {
		t.Error("propKeyMaxQos not decoded")
	}

	if v, ok := rawProps[propKeyRetainAvail]; ok {
		if len(v) != 1 {
			t.Error("propKeyRetainAvail error", v)
		}
	} else {
		t.Error("propKeyRetainAvail not decoded")
	}

	if v, ok := rawProps[propKeyUserProps]; ok {
		uProps := getUserProps(v)
		if mq, ok := uProps["MQ"]; !ok || len(mq) != 2 {
			t.Error(mq)
		}
		if tt, ok := uProps["TT"]; !ok || len(tt) != 1 {
			t.Error(tt)
		}
	} else {
		t.Error("propKeyUserProps not decoded")
	}

	if v, ok := rawProps[propKeyMaxPacketSize]; ok {
		if len(v) != 4 {
			t.Error("propKeyMaxPacketSize error", v)
		}
	} else {
		t.Error("propKeyMaxPacketSize not decoded")
	}

	if v, ok := rawProps[propKeyWildcardSubAvail]; ok {
		if len(v) != 1 {
			t.Error("propKeyWildcardSubAvail error", v)
		}
	} else {
		t.Error("propKeyWildcardSubAvail not decoded")
	}

	if v, ok := rawProps[propKeySubIDAvail]; ok {
		if len(v) != 1 {
			t.Error("propKeySubIDAvail error", v)
		}
	} else {
		t.Error("propKeySubIDAvail not decoded")
	}

	if v, ok := rawProps[propKeySharedSubAvail]; ok {
		if len(v) != 1 {
			t.Error("propKeySharedSubAvail error", v)
		}
	} else {
		t.Error("propKeySharedSubAvail not decoded")
	}
}
