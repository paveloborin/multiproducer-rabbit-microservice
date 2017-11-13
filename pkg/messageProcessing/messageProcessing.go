package messageProcessing

import (
	"github.com/streadway/amqp"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/paveloborin/multiproducer-rabbit-microservice/pkg/cache"
)

func SendMessageFromProducerToRabbit(done <-chan bool, ch *amqp.Channel, cacheOut <-chan cache.Message, queueName string) {
	//продъюсеры умеют пересылать получаемое сообщение в RabbitMQ
	go func(done <-chan bool, ch *amqp.Channel, cacheOut <-chan cache.Message) {
		for {
			select {
			//внешний сигнал закрытия горутины
			case <-done:
				return
			case mes := <-cacheOut:
				body, err := json.Marshal(mes)
				if err != nil {
					fmt.Println(err)
					break
				}
				err = ch.Publish(
					"",
					queueName,
					false,
					false,
					amqp.Publishing{
						ContentType: "application/json",
						Body:        []byte(body),
					})
				log.Printf("message publish to rabbit: %s", body)

				if nil != err {
					log.Fatalf("Failed to publish a message: %s", err)
				}
			}
		}
	}(done, ch, cacheOut)

}

func SendMessageFromCacheToProducer(done <-chan bool, cacheStorage *cache.Storage) <-chan cache.Message {
	producerChannel := make(chan cache.Message)

	go func(done <-chan bool, producerChannel chan<- cache.Message, cacheStorage *cache.Storage) {
		defer close(producerChannel)
		for {
			select {
			//внешний сигнал закрытия горутины
			case <-done:
				return
			default:
				for _, mes := range cacheStorage.Get(int(time.Now().Unix())) {
					producerChannel <- mes
				}
				time.Sleep(100 * time.Millisecond)

			}
		}
	}(done, producerChannel, cacheStorage)

	return producerChannel
}

func SendMessagesToCache(done <-chan bool, cacheIn <-chan cache.Message) *cache.Storage {
	cacheStorage := cache.NewStorage()
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
	return cacheStorage
}
