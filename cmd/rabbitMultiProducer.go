package main

import (
	"fmt"
	"sync"
	"encoding/json"
	"time"
	"log"

	"github.com/streadway/amqp"
	_ "github.com/go-sql-driver/mysql"

	"github.com/paveloborin/multiproducer-rabbit-microservice/pkg/cache"
	"github.com/paveloborin/multiproducer-rabbit-microservice/pkg/config"
	"github.com/paveloborin/multiproducer-rabbit-microservice/pkg/dbProxy"
)

func main() {
	configuration, err := config.GetConfig()
	if nil != err {
		log.Fatalf("error reading config file: %s", err)
	}

	done := make(chan bool)
	defer close(done)

	var collectors = []<-chan cache.Message{}

	dbProxy := dbProxy.GetConfig(&configuration.Db)
	defer dbProxy.Close()

	amqpConn := createAmqpConnect(configuration.Rabbit)
	defer amqpConn.Close()

	amqpChannel, err := amqpConn.Channel()
	if nil != err {
		log.Fatalf("failed to open a channel: %s", err)
	}
	defer amqpChannel.Close()

	queue, err := amqpChannel.QueueDeclare(
		configuration.Producer.ExchangeName, // name
		false,                               // durable
		false,                               // delete when unused
		false,                               // exclusive
		false,                               // no-wait
		nil,                                 // arguments
	)

	if nil != err {
		log.Fatalf("Failed to declare a queue: %s", err)
	}

	//запускаем для каждого коллектора горутину
	for _, collectorConfig := range configuration.Collectors {
		//Коллекторы наполняют свои очереди
		collectors = append(collectors, runCollector(collectorConfig, done, dbProxy))
	}

	//все очереди коллекторов собираются в одну
	cacheIn := fanIn(done, collectors...)
	//и отдельная горутина служит для обработки входа кеша - перекладывает все из общей коллекторной очереди в кеш
	cacheStorage := cache.NewStorage()
	sendCollectorsMessagesToCache(done, cacheIn, cacheStorage)
	//на выходе из кеша стоит горутина, которая на основании времени вынимает сообщение из кеша и помещает его в очередь продъюсера (пролъюсеров может быть несколько)
	cacheOut := make(chan cache.Message)

	go func(done <-chan bool, cacheOut chan<- cache.Message) {
		for {
			select {
			//внешний сигнал закрытия горутины
			case <-done:
				return
			default:
				for _, mes := range cacheStorage.Get(int(time.Now().Unix())) {
					cacheOut <- mes
				}
				time.Sleep(100 * time.Millisecond)

			}
		}
	}(done, cacheOut)

	//продъюсеры умеют пересылать получаесое сообщение в RabbitMQ
	go func(done <-chan bool, ch *amqp.Channel, cacheOut <-chan cache.Message) {
		for {
			select {
			//внешний сигнал закрытия горутины
			case <-done:
				return
			case mes := <-cacheOut:
				body, _ := json.Marshal(mes)
				if err != nil {
					fmt.Println(err)
					break
				}
				err = ch.Publish(
					"",         // exchange
					queue.Name, // routing key
					false,      // mandatory
					false,      // immediate
					amqp.Publishing{
						ContentType: "application/json",
						Body:        []byte(body),
					})
				log.Printf("Message publish to rabbit: %s", body)

				if nil != err {
					log.Fatalf("Failed to publish a message: %s", err)
				}
			}
		}
	}(done, amqpChannel, cacheOut)

	select {
	//внешний сигнал закрытия горутины
	case <-done:
		return
	}

}

/**
создаем коннект к реббиту
 */
func createAmqpConnect(amqpConfig config.ResourceConfig) *amqp.Connection {

	amqpConn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%v/", amqpConfig.User, amqpConfig.Pass, amqpConfig.Host, amqpConfig.Port))
	if nil != err {
		log.Fatalf("error rabbit connect: %s", err)
	}

	return amqpConn
}

func sendCollectorsMessagesToCache(done <-chan bool, cacheIn <-chan cache.Message, cacheStorage *cache.Storage) {
	go func(<-chan cache.Message, <-chan bool, *cache.Storage) {
		for {
			select {
			//внешний сигнал закрытия горутины
			case <-done:
				return
			default:
				//обращение к базе
				cacheStorage.Add(<-cacheIn)
			}
		}

	}(cacheIn, done, cacheStorage)
}

func runCollector(collectorConfig config.CollectorConfig, done <-chan bool, db *dbProxy.DbProxy) (chan cache.Message) {
	cacheMessageChan := make(chan cache.Message)

	go func(chan cache.Message, <-chan bool, *dbProxy.DbProxy, config.CollectorConfig) {
		for {
			//Горутина крутит в цикле  запрос в базу, отправка результата запроса в кеш
			select {
			//внешний сигнал закрытия горутины
			case <-done:
				return
			default:
				start := time.Now()
				rows, err := db.Query(collectorConfig.SqlQuery)
				if nil != err {
					log.Fatalf("can't read from db %s", err)
				}

				for rows.Next() {
					var id int
					var timestamp int

					err = rows.Scan(&id, &timestamp)
					if nil != err {
						log.Fatalf("can't parse sql-query result: %s", err)
					}

					cacheMessageChan <- cache.Message{Id: id, TimeStamp: timestamp, HandlerName: collectorConfig.HandlerName, Params: collectorConfig.Params}
				}
				//Новый цикл не начинается пока не закончится предыддущий и не выйдет время
				elapsed := time.Since(start)
				if elapsed < collectorConfig.TimePeriod {
					time.Sleep(collectorConfig.TimePeriod)
				}

			}
		}

	}(cacheMessageChan, done, db, collectorConfig)

	return cacheMessageChan
}

func fanIn(done <-chan bool, channels ...<-chan cache.Message, ) <-chan cache.Message {
	var wg sync.WaitGroup
	multiplexedStream := make(chan cache.Message)

	multiplex := func(c <-chan cache.Message) {
		defer wg.Done()
		for i := range c {
			select {
			case <-done:
				return
			case multiplexedStream <- i:
			}
		}
	}

	wg.Add(len(channels))
	for _, c := range channels {
		go multiplex(c)
	}

	go func() {
		wg.Wait()
		close(multiplexedStream)
	}()

	return multiplexedStream
}
