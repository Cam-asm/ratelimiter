package main

import (
	"fmt"
	"net/http"

	"github.com/Cam-asm/ratelimiter"
)

type va struct {
	Url *string
}

func (v va) ReceiveAndDeleteMessages() ([]ratelimiter.SqsMessage, error) {
	fmt.Println("va: Batch read 10 sqs messages")

	return []ratelimiter.SqsMessage{
		{Id: 1, Body: []byte("message: A")},
		{Id: 2, Body: []byte("message: B")},
		{Id: 3, Body: []byte("message: C")},
		{Id: 4, Body: []byte("message: D")},
		{Id: 5, Body: []byte("message: E")},
		{Id: 6, Body: []byte("message: F")},
		{Id: 7, Body: []byte("message: G")},
		{Id: 8, Body: []byte("message: H")},
		{Id: 9, Body: []byte("message: I")},
		{Id: 10, Body: []byte("message: J")},
	}, randomError(9998, "va.ReceiveAndDeleteMessages")
}

func (v va) ProcessMessage(message ratelimiter.SqsMessage) (cr ratelimiter.CuscalRequest, err error) { // maybe change url *string to type url?
	return cr, randomError(message.Id, "va.ProcessMessage")
}

func (v va) ProcessResponse(cr ratelimiter.CuscalRequest) error {
	return randomError(cr.Id, "va.ProcessResponse")
}

func (v va) SendRequest(request ratelimiter.CuscalRequest) (err error) {
	fmt.Println("va:", http.MethodPost, request.Id, string(request.Body))
	// r.Set.Headers("", "")
	// r.Set.Headers("", "")

	return randomError(request.Id, "va.SendRequest")
}
