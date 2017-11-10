# multiproducer-rabbit-microservice
Микросервис отправки сообщений в RabbitMQ, поддерживающий множество продъюсеров и отложенную отправку в очередь

Run

go get github.com/streadway/amqp

docker run -d --hostname my-rabbit --name some-rabbit rabbitmq:3-management