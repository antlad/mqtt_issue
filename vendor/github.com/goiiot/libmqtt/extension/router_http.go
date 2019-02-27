package extension

import (
	mqtt "github.com/goiiot/libmqtt"
)

// HttpRouter is a HTTP URL style router
type HttpRouter struct {
}

// Name of HttpRouter is "HttpRouter"
func (r *HttpRouter) Name() string {
	if r == nil {
		return "<nil>"
	}

	return "HttpRouter"
}

// Handle the topic with TopicHandler h
func (r *HttpRouter) Handle(topic string, h mqtt.TopicHandler) {

}

// Dispatch the received packet
func (r *HttpRouter) Dispatch(p *mqtt.PublishPacket) {

}
