package main

import (
	"log"

	"github.com/streadway/amqp"
	_ "github.com/go-sql-driver/mysql"

	"github.com/paveloborin/multiproducer-rabbit-microservice/pkg/config"
	"github.com/paveloborin/multiproducer-rabbit-microservice/pkg/dbProxy"
	"github.com/paveloborin/multiproducer-rabbit-microservice/pkg/collector"
	"github.com/paveloborin/multiproducer-rabbit-microservice/pkg/amqpProxy"
	"github.com/paveloborin/multiproducer-rabbit-microservice/pkg/messageProcessing"
)

var conf *config.Configuration
var dbConnection *dbProxy.DbProxy
var amqpConnection *amqpProxy.AmqpProxy
var amqpChannel *amqp.Channel
var amqpQueue amqp.Queue

func init() {
	var err error
	//get a configuration
	conf, err = config.GetConfig()
	if nil != err {
		log.Fatalf("error reading config file: %s", err)
	}

	//init a connect to dbConnection
	dbConnection, err = dbProxy.InitDbConnection(&conf.Db)
	if nil != err {
		log.Fatalf("error db connect: %s", err)
	}

	//init an amqp connect
	amqpConnection, err = amqpProxy.InitAmqpConnection(&conf.Rabbit)
	if nil != err {
		log.Fatalf("error rabbit connect: %s", err)
	}

	//open an amqp channel
	amqpChannel, err = amqpConnection.Channel()
	if nil != err {
		log.Fatalf("failed to open a channel: %s", err)
	}

	//declare a queue
	amqpQueue, err = amqpChannel.QueueDeclare(
		conf.Producer.ExchangeName,
		false,
		false,
		false,
		false,
		nil,
	)

	if nil != err {
		log.Fatalf("failed to declare a queue: %s", err)
	}
}

func main() {
	defer dbConnection.Close()
	defer amqpConnection.Close()
	defer amqpChannel.Close()

	done := make(chan bool)
	defer close(done)

	//сообщения из всех коллекторов сливаются в одну очередь
	collectorsChannel := collector.GetCollectorChannel(done, conf.Collectors, dbConnection)
	//горутина перекладывает сообщения из общей коллекторной очереди в кеш, для возможности последующей сортировки сообщений по времени
	cacheStorage := messageProcessing.SendMessagesToCache(done, collectorsChannel)
	//горутина на основании времени обработки вынимает сообщение из кеша и помещает его в очередь продъюсера
	producerChannel := messageProcessing.SendMessageFromCacheToProducer(done, cacheStorage)

	//сообщения из продъюсера отправляются в реббит
	messageProcessing.SendMessageFromProducerToRabbit(done, amqpChannel, producerChannel, amqpQueue.Name, )

	select {
	//внешний сигнал закрытия горутины
	case <-done:
		return
	}

}
