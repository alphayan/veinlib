package mq

import (
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
			Host:     "localhost",
			Port:     "5672",
		},
		Queue: "veinlib_test_rpc",
	}
	go rpcClient.Start()
	rpc := RPC{
		Config: Config{
			Username: "guest",
			Password: "guest",
			Host:     "localhost",
			Port:     "5672",
		},
		Queue:     "veinlib_test_rpc",
		Processor: rpcReply,
	}
	go rpc.Start()
	time.Sleep(time.Second)
	log.Info("start sent rpc to veinlib_test_queue")
	reply, err := rpcClient.Send([]byte("test msg"))
	if err != nil {
		log.Errorf("rpc_client_test error : %s", err)
	}
	log.Debugf("rpc_client_test success, reply is %s", string(reply))
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
