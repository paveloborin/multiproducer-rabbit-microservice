package collector

import (
	"time"
	"log"

	"github.com/paveloborin/multiproducer-rabbit-microservice/pkg/dbProxy"
	"github.com/paveloborin/multiproducer-rabbit-microservice/pkg/config"
	"github.com/paveloborin/multiproducer-rabbit-microservice/pkg/cache"
	"sync"
)

func GetCollectorChannel(done <-chan bool, collectorConfigs []config.CollectorConfig, dbProxy *dbProxy.DbProxy) <-chan cache.Message {
	//запускаем для каждого коллектора горутину
	collectors := startCollectors(done, collectorConfigs, dbProxy)
	return fanIn(done, collectors...)
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

func startCollectors(done <-chan bool, collectorConfigs []config.CollectorConfig, dbProxy *dbProxy.DbProxy) []<-chan cache.Message {
	//запускаем для каждого коллектора горутину
	log.Println("start collectors")
	var collectors = []<-chan cache.Message{}
	for _, collectorConfig := range collectorConfigs {
		//Коллекторы наполняют свои очереди
		collectors = append(collectors, runCollectorGoroutine(collectorConfig, done, dbProxy))
	}

	return collectors
}

func runCollectorGoroutine(collectorConfig config.CollectorConfig, done <-chan bool, db *dbProxy.DbProxy) (chan cache.Message) {
	cacheMessageChan := make(chan cache.Message)

	go func(chan cache.Message, <-chan bool, *dbProxy.DbProxy, config.CollectorConfig) {
		defer log.Println("stop collector workers")
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

					log.Printf("collect entity with id:%v, handlerName:%s, time:%v \n", id, collectorConfig.HandlerName, timestamp)
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
