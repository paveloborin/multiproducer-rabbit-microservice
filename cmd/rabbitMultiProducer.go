package main

import (
	"fmt"
	"sync"
	"github.com/paveloborin/multiproducer-rabbit-microservice/pkg/cache"
	"github.com/paveloborin/multiproducer-rabbit-microservice/pkg/config"
)

func main() {

	configuration, err := config.GetConfig()
	if nil != err {
		fmt.Errorf("error reading config file: %s", err)
	}

	//fmt.Println(configuration)

	done := make(chan bool)
	defer close(done)

	var collectors = []<-chan cache.Message{}

	//TODO создаем коннект к базе

	//TODO создаем коннект к реббиту

	//запускаем для каждого коллектора горутину

	for _, collectorConfig := range configuration.Collectors {
		//Коллекторы наполняют свои очереди
		//TODO пробросить коннект к базе
		collectors = append(collectors, runCollector(collectorConfig, done))
	}

	//все очереди коллекторов собираются в одну
	cacheIn := fanIn(done, collectors...)
	//и отдельная горутина служит для обработки входа кеша - перекладывает все из общей коллекторной очереди в кеш
	sendCollectorsMessagesToCache(done, cacheIn)
	//на выходе из кеша стоит горутина, которая на основании времени вынимает сообщение из кеша и помещает его в очередь продъюсера (пролъюсеров может быть несколько)
	//продъюсеры умеют пересылать получаесое сообщение в RabbitMQ

}

func sendCollectorsMessagesToCache(done <-chan bool, cacheIn <-chan cache.Message) {
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
}

func runCollector(config config.CollectorConfig, done <-chan bool) (chan cache.Message) {
	cacheMessageChan := make(chan cache.Message)

	//Горутина крутит в цикле следующее: запрос в базу, отправка в кеш
	//Новый цикл не начинается пока не закончится предыддущий и не выйдет время
	go func(chan cache.Message, <-chan bool) {
		for {
			select {
			//внешний сигнал закрытия горутины
			case <-done:
				return
			default:
				//обращение к базе
				cacheMessageChan <- cache.Message{}
			}
		}

	}(cacheMessageChan, done)

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
