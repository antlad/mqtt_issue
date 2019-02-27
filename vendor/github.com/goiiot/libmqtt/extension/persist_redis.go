package extension

import (
	"bytes"
	"strings"

	"github.com/go-redis/redis"
	mqtt "github.com/goiiot/libmqtt"
)

// redisPersist defines the persist method with redis
type redisPersist struct {
	conn    *redis.Client
	buf     *bytes.Buffer
	mainKey string
}

// Name of redisPersist is "redisPersist"
func (r *redisPersist) Name() string {
	if r == nil {
		return "<nil>"
	}

	return "redisPersist"
}

// Store a packet with key
func (r *redisPersist) Store(key string, p mqtt.Packet) error {
	if p == nil || r.conn == nil {
		return nil
	}

	if ok, err := r.conn.HSet(r.mainKey, key, p.Bytes()).Result(); !ok {
		return err
	}

	return nil
}

// Load a packet from stored data according to the key
func (r *redisPersist) Load(key string) (mqtt.Packet, bool) {
	if r == nil || r.conn == nil {
		return nil, false
	}

	if rs, err := r.conn.HGet(r.mainKey, key).Result(); err != nil {
		if pkt, err := mqtt.Decode(mqtt.V311, strings.NewReader(rs)); err != nil {
			// delete wrong packet
			r.Delete(key)
		} else {
			return pkt, true
		}
	}

	return nil, false
}

// Range over data stored, return false to break the range
func (r *redisPersist) Range(f func(string, mqtt.Packet) bool) {
	if r == nil || r.conn == nil {
		return
	}

	if set, err := r.conn.HGetAll(r.mainKey).Result(); err == nil {
		for k, v := range set {
			if pkt, err := mqtt.Decode(mqtt.V311, strings.NewReader(v)); err != nil {
				r.Delete(k)
				continue
			} else {
				if !f(k, pkt) {
					break
				}
			}
		}
	}
}

// Delete a persisted packet with key
func (r *redisPersist) Delete(key string) error {
	if r == nil || r.conn == nil {
		return nil
	}
	_, err := r.conn.HDel(r.mainKey, key).Result()
	return err
}

// Destroy stored data
func (r *redisPersist) Destroy() error {
	if r == nil || r.conn == nil {
		return nil
	}
	_, err := r.conn.Del(r.mainKey).Result()
	return err
}
