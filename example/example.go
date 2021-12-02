package main

import (
	"errors"
	"fmt"
	"github.com/Cam-asm/ratelimiter"
	"math/rand"
	"net/http"
)

func main() {
	// How to use the rate limiter
	urlPattern := "/payid/something/id"
	queueName := "PAYID"

	t := ratelimiter.TPS{
		ReadChannelSize: 10,
		RequeueChanSize: 10,
		Interface:       tps{},
		Url:             &urlPattern,
		QueueName:       &queueName,
	}
	t.Start()
}

//
type tps struct{}

func (t tps) ReceiveAndDeleteMsgs() ([]ratelimiter.SqsMessage, error) {
	fmt.Println("Batch read 10 sqs messages")

	return []ratelimiter.SqsMessage{
		{Id: 1, Body: []byte("message: 1")},
		{Id: 2, Body: []byte("message: 2")},
		{Id: 3, Body: []byte("message: 3")},
		{Id: 4, Body: []byte("message: 4")},
		{Id: 5, Body: []byte("message: 5")},
		{Id: 6, Body: []byte("message: 6")},
		{Id: 7, Body: []byte("message: 7")},
		{Id: 8, Body: []byte("message: 8")},
		{Id: 9, Body: []byte("message: 9")},
		{Id: 10, Body: []byte("message: 10")},
	}, randomError()
}
func (t tps) ProcessMessage(message ratelimiter.SqsMessage, urlPattern *string) (cr ratelimiter.CuscalRequest, err error) { // maybe change url *string to type url?
	return cr, randomError()
}

func (t tps) ProcessResponse(cr ratelimiter.CuscalRequest) error {
	return randomError()
}

func (t tps) SendRequest(request ratelimiter.CuscalRequest) (err error) {
	fmt.Println(http.MethodPost, request.Id, string(request.Body))
	//r.Set.Headers("", "")
	//r.Set.Headers("", "")

	return randomError()
}

func randomError() error {
	if rand.Intn(4) == 3 {
		return errors.New("something went wrong")
	}

	return nil
}
