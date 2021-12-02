package main

import (
	"errors"
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
		Interface:       va{},
		Url:             &vaUrlPattern,
		QueueName:       &vaQueueName,
	}
	go v.Start()

	urlPattern := "/payid/something/id"
	queueName := "PAYID"
	pid := ratelimiter.TPS{
		ReadChannelSize: 10,
		RequeueChanSize: 10,
		Interface:       payid{},
		Url:             &urlPattern,
		QueueName:       &queueName,
	}
	pid.Start()
}

func randomError() error {
	if rand.Intn(4) == 3 {
		return errors.New("something went wrong")
	}

	return nil
}
