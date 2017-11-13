package amqpProxy

import (
	"github.com/streadway/amqp"
	"fmt"

	"github.com/paveloborin/multiproducer-rabbit-microservice/pkg/config"
)

type AmqpProxy struct {
	connection *amqp.Connection
	config     *config.ResourceConfig
}

func InitAmqpConnection(amqpConfig *config.ResourceConfig) (*AmqpProxy, error) {
	db, err := createConnect(amqpConfig)
	return &AmqpProxy{connection: db, config: amqpConfig}, err

}

/**
создаем коннект к реббиту
 */
func createConnect(amqpConfig *config.ResourceConfig) (*amqp.Connection, error) {

	amqpConn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%v/", amqpConfig.User, amqpConfig.Pass, amqpConfig.Host, amqpConfig.Port))

	return amqpConn, err
}

func (amqpProxy *AmqpProxy) Channel() (*amqp.Channel, error) {

	//open an amqp channel
	return amqpProxy.connection.Channel()
}

func (amqpProxy *AmqpProxy) Close() error {
	return amqpProxy.connection.Close()
}
