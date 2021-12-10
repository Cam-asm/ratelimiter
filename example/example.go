package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/Cam-asm/ratelimiter"
)

func main() {
	log.SetFlags(log.Lshortfile)

	// How to use the rate limiter
	vaUrlPattern := "http://localhost:6060/pkg/%s"
	vaQueueName := "VA"

	// One ratelimiter sending one transaction per second.
	v := ratelimiter.TPS{
		Processor: va{
			Url: &vaUrlPattern,
		},
		SendEvery: time.Second,
		QueueName: &vaQueueName,
	}
	go v.Start()

	// A second rate limiter sending 5 transactions per second.
	queueName := "PAYID"
	pid := ratelimiter.TPS{
		Processor: payId{
			UrlPattern: "http://localhost:6060/%s%s",
		},
		SendEvery: 200 * time.Millisecond,
		QueueName: &queueName,
	}
	pid.Start()
}

// randomError simulates random errors being generated.
func randomError(id interface{}, funcName string) error {
	if rand.Intn(4) == 3 {
		return fmt.Errorf("something went wrong. ID: %v %s", id, funcName)
	}

	return nil
}
