package main

import (
	"fmt"
	"net/http"

	"github.com/Cam-asm/ratelimiter"
	"github.com/google/uuid"
)

// payId is a different rate limiter implementation to va (virtual accounts).
type payId struct {
	UrlPattern string
}

// ReceiveAndDeleteMessages queries the SQS database for the next batch of messages to be processed by ProcessMessage.
func (p payId) ReceiveAndDeleteMessages() ([]ratelimiter.SqsMessage, error) {
	fmt.Println("pid: Batch read 10 sqs messages")

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
	}, randomError(9999, "pid.ReceiveAndDeleteMessages")
}

// ProcessMessage converts the SQS Message into a CuscalRequest and completes all processing to a ready state to be sent by SendRequest.
func (p payId) ProcessMessage(message ratelimiter.SqsMessage) (cr ratelimiter.CuscalRequest, err error) { // maybe change url *string to type url?
	traceId, err := uuid.NewUUID()
	if err != nil {
		return cr, fmt.Errorf("pid: error generating traceId: %w", err)
	}

	return ratelimiter.CuscalRequest{
		Id:      message.Id,
		Url:     fmt.Sprintf(p.UrlPattern, "src", "time"),
		Headers: []ratelimiter.Header{{Header: "traceId", Value: traceId.String()}},
		Body:    message.Body,
		Retries: message.Retries,
	}, randomError(message.Id, "pid.ProcessMessage")
}

// ProcessResponse completes any remaining tasks asynchronously for successful or erroneous requests sent be SendRequest.
func (p payId) ProcessResponse(cr ratelimiter.CuscalRequest) error {
	return randomError(cr.Id, "pid.ProcessResponse")
}

// SendRequest is the rate limited function called every TPS.SendEvery. The response is handled by ProcessResponse.
func (p payId) SendRequest(request ratelimiter.CuscalRequest) (err error) {
	/*var r *http.Request
	r, err = http.NewRequest(http.MethodPost, request.Url, bytes.NewBuffer(request.Body))
	if err != nil {
		return fmt.Errorf("error creating new http request: %w", err)
	}

	r.Header.Set("Content-Type", "application/json")
	for i := range request.Headers {
		r.Header.Set(request.Headers[i].Header, request.Headers[i].Value)
	}*/

	fmt.Println(http.MethodPost, request.Id, string(request.Body))
	return randomError(request.Id, "pid.SendRequest")
}
