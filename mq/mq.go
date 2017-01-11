package mq

import "github.com/streadway/amqp"

// Config rabbit mq config
type Config struct {
	Username string
	Password string
	Host     string
	Port     string
}

// DeliveryProcessor process a rabbitmq delivery then respond a delivery
type DeliveryProcessor func(amqp.Delivery) amqp.Delivery

// DeliveryForwarder just forward a delivery to next stop
type DeliveryForwarder func(amqp.Delivery)

// URL rabbit mq connect url
func (c *Config) URL() string {
	return "amqp://" + config.MQUsername + ":" + config.MQPassword + "@" + config.MQHost + ":" + config.MQPort
}
