package main

import (
	"fmt"
	"sync"
	"github.com/paveloborin/multiproducer-rabbit-microservice/pkg/cache"
	"github.com/paveloborin/multiproducer-rabbit-microservice/pkg/config"
	"database/sql"
	"time"
	"log"
	_"github.com/go-sql-driver/mysql"
)

func main() {
	configuration, err := config.GetConfig()
	if nil != err {
		log.Fatalf("error reading config file: %s", err)
	}

	done := make(chan bool)
	defer close(done)

	var collectors = []<-chan cache.Message{}

	db, err := createDbConnect(&configuration.Db)
	if nil != err {
		log.Fatalf("error db connect: %s", err)
	}

	//TODO создаем коннект к реббиту

	//запускаем для каждого коллектора горутину
	for _, collectorConfig := range configuration.Collectors {
		//Коллекторы наполняют свои очереди
		collectors = append(collectors, runCollector(collectorConfig, done, db))
	}

	//все очереди коллекторов собираются в одну
	cacheIn := fanIn(done, collectors...)
	//и отдельная горутина служит для обработки входа кеша - перекладывает все из общей коллекторной очереди в кеш
	cacheStorage := cache.NewStorage()
	sendCollectorsMessagesToCache(done, cacheIn, cacheStorage)
	//на выходе из кеша стоит горутина, которая на основании времени вынимает сообщение из кеша и помещает его в очередь продъюсера (пролъюсеров может быть несколько)
	//продъюсеры умеют пересылать получаесое сообщение в RabbitMQ

	select {
	//внешний сигнал закрытия горутины
	case <-done:
		return
	}

}

/**
создаем коннект к базе
 */
func createDbConnect(configDb *config.ResourceConfig) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%v)/%s?charset=utf8", configDb.User, configDb.Pass, configDb.Host, configDb.Port, configDb.DbName)

	db, err := sql.Open("mysql", dsn)
	if nil != err {
		return nil, err
	}

	return db, nil
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

func runCollector(collectorConfig config.CollectorConfig, done <-chan bool, db *sql.DB) (chan cache.Message) {
	cacheMessageChan := make(chan cache.Message)

	go func(chan cache.Message, <-chan bool, *sql.DB, config.CollectorConfig) {
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
