package mq

import (
	"os"
	"testing"

	log "github.com/Sirupsen/logrus"
)

var host string

func TestMain(m *testing.M) {
	log.SetLevel(log.DebugLevel)
	host = os.Getenv("MQ_HOST")
	if host == "" {
		host = "localhost"
	}
	os.Exit(m.Run())
}
