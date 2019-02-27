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
)

func TestEncodeRemainLength(t *testing.T) {
	buf := &bytes.Buffer{}

	for i := 0; i <= 127; i++ {
		writeVarInt(i, buf)
		result := buf.Bytes()
		if len(result) != 1 {
			t.Error("fail at level 1 target:", i, ", result:", result)
		}
		buf.Reset()
	}

	for i := 128; i <= 16383; i++ {
		writeVarInt(i, buf)
		result := buf.Bytes()
		if len(result) != 2 {
			t.Error("fail at level 2 target:", i, ", result:", result)
		}
		buf.Reset()
	}

	for i := 16384; i <= 2097151; i++ {
		writeVarInt(i, buf)
		result := buf.Bytes()
		if len(result) != 3 {
			t.Error("fail at level 3 target:", i, ", result:", result)
		}
		buf.Reset()
	}

	//for i := 2097152; i <= 268435455; i++ {
	//	writeVarInt(i, buf)
	//	result := buf.Bytes()
	//	if len(result) != 4 {
	//		t.Error("fail at level 4 target:", i, ", result:", result)
	//	}
	//	buf.Reset()
	//}
}

func TestEncodeOnePacket(t *testing.T) {

}

func TestEncodeOneV311Packet(t *testing.T) {

}

func TestEncodeOneV5Packet(t *testing.T) {

}

func BenchmarkFuncDecode(b *testing.B) {
	buf := &bytes.Buffer{}
	pkt := testPubMsgs[0]

	b.ReportAllocs()
	b.N = 100000000
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := Encode(pkt, buf); err != nil {
			b.Log(err)
			b.Fail()
		}
	}
}
