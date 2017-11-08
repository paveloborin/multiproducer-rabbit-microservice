all: build

APP?=rabbitMultiProducerService

build:
	#dep ensure
	CGO_ENABLED=0 go build -a -installsuffix cgo \
		-o ./bin/${APP} ./cmd/


local: build

run: local
	./bin/${APP}