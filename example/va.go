package main

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"net/http"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/google/uuid"
)

type va struct {
	Url    *string
	Client http.Client
}

// ReceiveAndDeleteMessages queries the SQS database for the next batch of messages to be processed by ProcessMessage.
func (v va) ReceiveAndDeleteMessages() ([]*sqs.Message, error) {
	log.Println("va: Batch read 10 sqs messages")

	return []*sqs.Message{
		{MessageId: aws.String("1"), Body: aws.String("message: A")},
		{MessageId: aws.String("2"), Body: aws.String("message: B")},
		{MessageId: aws.String("3"), Body: aws.String("message: C")},
		{MessageId: aws.String("4"), Body: aws.String("message: D")},
		{MessageId: aws.String("5"), Body: aws.String("message: E")},
		{MessageId: aws.String("6"), Body: aws.String("message: F")},
		{MessageId: aws.String("7"), Body: aws.String("message: G")},
		{MessageId: aws.String("8"), Body: aws.String("message: H")},
		{MessageId: aws.String("9"), Body: aws.String("message: I")},
		{MessageId: aws.String("10"), Body: aws.String("message: J")},
	}, randomError(5555, "va.ReceiveAndDeleteMessages")
}

// ProcessMessage converts the SQS Message into a http.Request and completes all processing to a ready state to be sent by SendRequest.
func (v va) ProcessMessage(body *string) (r *http.Request, err error) { // maybe change url *string to type url?
	traceId, err := uuid.NewUUID()
	if err != nil {
		return nil, fmt.Errorf("va: error generating traceId: %w", err)
	}

	r, err = http.NewRequest(
		http.MethodPost,
		fmt.Sprintf(*v.Url, "net/"),
		bytes.NewBuffer([]byte(*body)),
	)
	if err != nil {
		return nil, fmt.Errorf("error creating new http request: %w", err)
	}

	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("traceId", traceId.String())

	/*return ratelimiter.Request{
		Id:      message.Id,
		Url:     fmt.Sprintf(*v.Url, "src", "time"),
		Headers: []ratelimiter.Header{{Header: "traceId", Value: traceId.String()}},
		Body:    message.Body,
		Retries: message.Retries,
	}*/
	return r, randomError(*body, "va.ProcessMessage")
}

// ProcessResponse completes any remaining tasks asynchronously for successful or erroneous requests sent be SendRequest.
func (v va) ProcessResponse(body []byte) error {
	return randomError(-1, "va.ProcessResponse")
}

func (v va) ResponseErr(err error) error {
	if er, ok := err.(net.Error); ok && er.Timeout() {
		return TimeoutError{}
	}

	return fmt.Errorf("error sending http request: %w", err)
}

type RateLimitError struct{}

func (n RateLimitError) Error() string {
	return "timeout registering account with Cuscal"
}

type TimeoutError struct{}

func (n TimeoutError) Error() string {
	return "timeout registering account with Cuscal"
}
