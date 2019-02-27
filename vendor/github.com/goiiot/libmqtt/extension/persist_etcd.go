package extension

import (
	mqtt "github.com/goiiot/libmqtt"
)

type EtcdPersist struct {
}

// Name of redisPersist is "EtcdPersist"
func (r *EtcdPersist) Name() string {
	if r == nil {
		return "<nil>"
	}

	return "EtcdPersist"
}

func (r *EtcdPersist) Store(key string, p mqtt.Packet) error {
	if r == nil {
		return nil
	}

	return nil
}

func (r *EtcdPersist) Load(key string) (mqtt.Packet, bool) {
	if r == nil {
		return nil, false
	}

	return nil, false
}

func (r *EtcdPersist) Range(f func(string, mqtt.Packet) bool) {
	if r == nil {
		return
	}
}

func (r *EtcdPersist) Delete(key string) error {
	if r == nil {
		return nil
	}

	return nil
}

func (r *EtcdPersist) Destroy() error {
	if r == nil {
		return nil
	}
	return nil
}
