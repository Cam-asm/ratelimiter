package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"log"
	"net/http"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// payId is a different rate limiter implementation to va (virtual accounts).
type payId struct {
	UrlPattern string
}

// ReceiveAndDeleteMessages queries the SQS database for the next batch of messages to be processed by ProcessMessage.
func (p payId) ReceiveAndDeleteMessages() ([]*sqs.Message, error) {
	log.Println("pid: Batch read 10 sqs messages")

	return []*sqs.Message{
		{MessageId: aws.String("1"), Body: aws.String(`{"message": "A"}`)},
		{MessageId: aws.String("2"), Body: aws.String(`{"message": "B"}`)},
		{MessageId: aws.String("3"), Body: aws.String(`{"message": "C"}`)},
		{MessageId: aws.String("4"), Body: aws.String(`{"message": "D"}`)},
		{MessageId: aws.String("5"), Body: aws.String(`{"message": "E"}`)},
		{MessageId: aws.String("6"), Body: aws.String(`{"message": "F"}`)},
		{MessageId: aws.String("7"), Body: aws.String(`{"message": "G"}`)},
		{MessageId: aws.String("8"), Body: aws.String(`{"message": "H"}`)},
		{MessageId: aws.String("9"), Body: aws.String(`{"message": "I"}`)},
		{MessageId: aws.String("10"), Body: aws.String(`{"message": "J"}`)},
	}, randomError(9999, "pid.ReceiveAndDeleteMessages")
}

type myType struct {
	Message string `json:"message"`
}

// ProcessMessage converts the SQS Message into a Request and completes all processing to a ready state to be sent by SendRequest.
func (p payId) ProcessMessage(body *string) (r *http.Request, err error) { // maybe change url *string to type url?
	newType := myType{}
	err = json.Unmarshal([]byte(*body), &newType)
	if err != nil {
		log.Println(err)
		return
	}

	//TODO do something with newType...
	log.Print("\t\tnewType:", newType, "\n")

	traceId, err := uuid.NewUUID()
	if err != nil {
		return nil, fmt.Errorf("pid: error generating traceId: %w", err)
	}

	r, err = http.NewRequest(
		http.MethodPost,
		fmt.Sprintf(p.UrlPattern, "io/", "ioutil"),
		bytes.NewBuffer([]byte(*body)),
	)
	if err != nil {
		return nil, fmt.Errorf("error creating new http request: %w", err)
	}

	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("traceId", traceId.String())

	//ratelimiter.Request{
	//	Id:      request.Id,
	//	Url:     fmt.Sprintf(p.UrlPattern, "src", "time"),
	//	Headers: []ratelimiter.Header{{Header: "traceId", Value: traceId.String()}},
	//	Body:    request.Body,
	//	Retries: request.Retries,
	//},
	return r, randomError(*body, "pid.ProcessMessage")
}

// ProcessResponse completes any remaining tasks asynchronously for successful or erroneous requests sent be SendRequest.
func (p payId) ProcessResponse(body []byte) error {
	return randomError(body, "pid.ProcessResponse")
}

func (p payId) ResponseErr(err error) error { return err }
