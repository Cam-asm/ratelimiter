package main

import (
	"fmt"
	"math/rand"

	"github.com/Cam-asm/ratelimiter"
)

func main() {
	// How to use the rate limiter
	vaUrlPattern := "/virtual-accounts/something/id"
	vaQueueName := "VA"

	v := ratelimiter.TPS{
		ReadChannelSize: 10,
		RequeueChanSize: 10,
		Interface: va{
			Url: &vaUrlPattern,
		},
		QueueName: &vaQueueName,
	}
	go v.Start()

	queueName := "PAYID"
	pid := ratelimiter.TPS{
		ReadChannelSize: 10,
		RequeueChanSize: 10,
		Interface: payid{
			UrlPattern: "http://localhost:6060/%s%s",
		},
		QueueName: &queueName,
	}
	pid.Start()
}

func randomError(id uint, funcName string) error {
	if rand.Intn(4) == 3 {
		return fmt.Errorf("something went wrong. ID: %d %s", id, funcName)
	}

	return nil
}
