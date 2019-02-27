package extension

import (
	"bytes"

	"github.com/boltdb/bolt"

	mqtt "github.com/goiiot/libmqtt"
)

type boltPersist struct {
	bucket   *bolt.Bucket
	strategy *mqtt.PersistStrategy
}

func (b *boltPersist) Name() string {
	if b == nil {
		return "<nil>"
	}

	return "boltPersist"
}

func (b *boltPersist) Store(key string, p mqtt.Packet) error {
	if b == nil {
		return nil
	}

	b.bucket.Put([]byte(key), p.Bytes())
	return nil
}

func (b *boltPersist) Load(key string) (mqtt.Packet, bool) {
	if b == nil {
		return nil, false
	}

	pkt, err := mqtt.Decode(mqtt.V311, bytes.NewReader(b.bucket.Get([]byte(key))))
	if err != nil || pkt == nil {
		return nil, false
	}
	return pkt, true
}

func (b *boltPersist) Range(f func(string, mqtt.Packet) bool) {
	if b == nil {
		return
	}
	b.bucket.ForEach(func(k, v []byte) error {
		pkt, err := mqtt.Decode(mqtt.V311, bytes.NewReader(v))
		if err == nil {
			f(string(k), pkt)
		}

		return nil
	})
}

func (b *boltPersist) Delete(key string) error {
	if b == nil {
		return nil
	}

	return nil
}
