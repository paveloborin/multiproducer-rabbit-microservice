package config

import (
	"github.com/spf13/viper"
	"strconv"
	"fmt"
	"os"
)

//Конфигурации
//Конфигурация коннетов к базе, к реббиту
type ResourceConfig struct {
	Host string
	Port int
	User string
	Pass string
}

//Конфигурация продъюсеров: название обменника, кол-во одновременно запущенных продьюсеров
type ProducerConfig struct {
	ExchangeName  string
	InstanceCount int
}

//Конфигурация коллекторов: название (оно же название обработчика, sql запрос, частота запуска
type CollectorConfig struct {
	CollectorName string
	HandlerName   string
	TimePeriod    int
	SqlQuery      string
	Params        map[string]string
}

type Configuration struct {
	Db         ResourceConfig
	Rabbit     ResourceConfig
	Producer   ProducerConfig
	Collectors []CollectorConfig
}

func GetConfig() (*Configuration, error) {
	viper.AddConfigPath(fmt.Sprintf("config/%s", os.Getenv("APP_ENV")))
	viper.SetConfigName("app")
	err := viper.ReadInConfig()
	if nil != err {
		return &Configuration{}, err
	}

	dbConfig := ResourceConfig{Host: viper.GetString("database.host"), Port: viper.GetInt("database.ports"), User: viper.GetString("database.user"), Pass: viper.GetString("database.password")}
	rabbitConfig := ResourceConfig{Host: viper.GetString("rabbit.host"), Port: viper.GetInt("rabbit.ports"), User: viper.GetString("rabbit.user"), Pass: viper.GetString("rabbit.password")}
	producerConfig := ProducerConfig{ExchangeName: viper.GetString("producer.exchange_name"), InstanceCount: viper.GetInt("producer.instance_count")}

	var collectors []CollectorConfig

	for name, data := range viper.GetStringMap("collectors") {
		dataConvertedToMap := data.(map[string]interface{})

		params := make(map[string]string)
		for k, v := range dataConvertedToMap["params"].(map[string]interface{}) {
			params[k] = fmt.Sprint(v)
		}

		timePeriod, err := strconv.Atoi(fmt.Sprint(dataConvertedToMap["repeat_period"]))
		if nil != err {
			return &Configuration{}, err
		}

		collector := CollectorConfig{CollectorName: name, TimePeriod: timePeriod, Params: params, SqlQuery: fmt.Sprint(dataConvertedToMap["sql_query_file"]), HandlerName: fmt.Sprint(dataConvertedToMap["handler"])}
		collectors = append(collectors, collector)
	}

	return &Configuration{Db: dbConfig, Rabbit: rabbitConfig, Producer: producerConfig, Collectors: collectors}, nil
}
