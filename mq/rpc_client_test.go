package mq

import (
	"strconv"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
)

func TestRpcClient(t *testing.T) {
	log.Info("start RPCClient test")
	rpcClient := RPCClient{
		Config: Config{
			Username: "guest",
			Password: "guest",
			Host:     host,
			Port:     "5672",
		},
		Queue: "veinlib_test_rpc",
	}
	go rpcClient.Start()
	rpc := RPC{
		Config: Config{
			Username: "guest",
			Password: "guest",
			Host:     host,
			Port:     "5672",
		},
		Queue:     "veinlib_test_rpc",
		Processor: rpcReply,
	}
	go rpc.Start()
	time.Sleep(time.Second)
	log.Info("start sent rpc to veinlib_test_queue")
	for i := 0; i < 100; i++ {
		reply, err := rpcClient.Send([]byte(strconv.Itoa(i)))
		if err != nil {
			t.Errorf("rpc_client_test error : %s", err)
		}
		j, err := strconv.Atoi(string(reply))
		if err != nil || j != i {
			t.Error("value changed")
		}
	}
}

func rpcReply(delivery amqp.Delivery) amqp.Publishing {
	reply := amqp.Publishing{
		ContentType:   "text/plain",
		CorrelationId: delivery.CorrelationId,
		Body:          delivery.Body,
		Timestamp:     time.Now(),
	}
	return reply
}
