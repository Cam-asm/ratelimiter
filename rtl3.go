package main

import (
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

const (
	queuedBatches    = 6
	requeueBatchSize = 10
	workerName       = "VA"
)

var (
	wgSendRequest = sync.WaitGroup{}
	// quit = has the graceful shutdown process been toggled?
	quit         bool
	quitRequeue  bool
	delayRequest bool                                       // not required - only used for demo situation.
	readyToQueue = make(chan *sqsMessage, requeueBatchSize) // separate channel for requeuing messages to SQS.
)

/* MISSION: To send rate limited HTTP requests as consistently as possible.

This implementation performs pre-processing and cleanup for each request in separate go routines, limiting
interference of any long-running processes, network timeouts or slow database queries.

TODO	- Some HTTP errors can be handled gracefully and can be requested straight away,
		  but JSON unmarshalling errors need to be re-queued.
		- Provide an easy way to setup multiple rate limiters
		- Can we provide or override the functions/methods called in the rate limiter?
		  Perhaps an interface would be a good fit - a struct that implements set methods?

*/

func main() {
	listenToQuit()

	// readyToSend channel contains a list of Cuscal request ready to be sent
	readyToSend := make(chan cuscalRequest, queuedBatches)

	// query sqs queue in a go routine
	go getAndProcessMessages(readyToSend)

	go listenRequeue()

	// blocks main() from exiting while there are no
	RateLimitSendMessages(readyToSend)

	// Waits for all sendRequest goroutines to finish when RateLimitSendMessages() returns
	// This allows each sendRequest() call to complete (wait for Cuscal response and
	// save to the database)
	wgSendRequest.Wait()
	fmt.Println("ALL GO ROUTINES CLOSED")

	cleanOutRequeue()
}

// Gracefully trigger the server to start its shutdown procedure
func listenToQuit() {
	q := make(chan os.Signal)
	signal.Notify(q, os.Interrupt, syscall.SIGTERM)
	go func() {
		// block until q channel receives an interrupt
		<-q
		fmt.Println("\n\nTRIGGER SHUTDOWN\n\n")
		quit = true
	}()
}

// worker to query sqs queue
func getAndProcessMessages(readyToSend chan<- cuscalRequest) {
	myUrl := "stuff"
	var u uint

	for {
		//call sqs to get the next 10 requests
		messages, _ := getMessagesFromQueue()
		// loop over each request
		for i := range messages {
			u++
			fmt.Println("processed message:", u)
			// process each message - marshal the json payload ready for sending.
			request, err := processMessage(messages[i], u, &myUrl)
			// if there wasn't an issue processing the message
			if err == nil {
				// then send the message to the channel
				// if the channel doesn't have any space available then
				// this line blocks any further processing
				readyToSend <- request
			}
		}

		// this could use the q channel from listenToQuit - but I want to keep this simple,
		// and I don't want to forcefully quit from this loop.
		// This can take several seconds to return - due to the if statement placement below the blocking `readyToSend <- request`
		// and wanting to complete up to 10 outstanding sqs messages read.
		if quit {
			// Close the channel
			close(readyToSend)
			fmt.Println("\n\n\n\t\tquit ==", u, "\n\n\n")
			return
		}
	}
}

func (c *cuscalRequest) waitResponse() {
	// This WaitGroup will let the go routines sendRequest to continue processing
	// until wgSendRequest.Done is called.
	wgSendRequest.Add(1)

	// Simulate a long round trip time for the response from Cuscal
	if delayRequest {
		time.Sleep(5 * time.Second)
	}
	delayRequest = !delayRequest

	// TODO the response is very loosely coupled
	// I don't think there's enough info here at present to save???
	fmt.Println("save to database message:", c.id)

	wgSendRequest.Done()
}

func (c *cuscalRequest) sendRequest() {
	// Send HTTP request.
	_, err := fmt.Fprintln(os.Stdout, http.MethodPost, c.id, string(c.body))
	// Intentionally cause random failures to trigger requeuing
	if err != nil || rand.Intn(3) <= 2 {
		fmt.Println("HTTP call failed to send. ID:", c.id, string(c.body))
		c.requeueMessage()
		return
	}

	go c.waitResponse()
}

func RateLimitSendMessages(readyToSend <-chan cuscalRequest) {
	// limit the rate that we receive from the channel readyToSend to once a second
	for range time.NewTicker(time.Second).C {
		// wait to receive a message in readyToSend.
		// If the channel doesn't contain any items, then it blocks and waits on this line.
		cr, ok := <-readyToSend
		if !ok {
			/* I never encountered this problem - so it's disabled.
			if !cr.IsEmpty() {
				log.Println("cr is not meant to be populated")
				go sendRequest(cr, ok)
			}*/
			// if the channel was closed then return.
			return
		}

		// asynchronously make the http request and wait for the response. Then save to DB
		cr.sendRequest()
	}
}

const (
	ContentType = "Content-Type"
	JSON        = "application/json"
	TraceId     = "traceId"
)

type cuscalRequest struct {
	id      uint
	url     *string
	body    []byte
	headers []header
	retries uint8
}
type header struct {
	header, value string
}

func (c *cuscalRequest) requeueMessage() {
	// Additional steps might be required here.
	readyToQueue <- &sqsMessage{
		id:      c.id,
		body:    c.body,
		retries: c.retries + 1,
		typ:     workerName,
	}
}

type sqsMessage struct {
	id      uint
	body    []byte
	retries uint8
	typ     string
}

func listenRequeue() {
	for {
		// wait for 10 items from readyToQueue
		toRequeue := [requeueBatchSize]*sqsMessage{<-readyToQueue, <-readyToQueue, <-readyToQueue, <-readyToQueue, <-readyToQueue, <-readyToQueue, <-readyToQueue, <-readyToQueue, <-readyToQueue, <-readyToQueue}
		for i := range toRequeue {
			if toRequeue[i] == nil {
				// At shutdown send nil to trigger the rest of the messages to be requeued.
				// I don't think there's a better way to do it?
				continue
			}

			toRequeue[i].Requeue()
		}

		if quitRequeue {
			fmt.Print("\n\nEND listenRequeue\n\n")
			return
		}
	}
}

func cleanOutRequeue() {
	quitRequeue = true
	for i := 1; i <= requeueBatchSize; i++ {
		fmt.Println("Send requeue nil", i)
		readyToQueue <- nil
	}
}

func (m *sqsMessage) Requeue() {
	fmt.Println("Requeue", m.id, string(m.body))
}

func (c cuscalRequest) IsEmpty() bool {
	return c.id == 0 && c.url == nil && c.body == nil && c.headers == nil
}

func getMessagesFromQueue() ([]string, error) {
	fmt.Println("Batch read 10 sqs messages")
	return []string{
		"message: A",
		"message: B",
		"message: C",
		"message: D",
		"message: E",
		"message: F",
		"message: G",
		"message: H",
		"message: I",
		"message: J",
	}, nil
}

func processMessage(message string, id uint, myUrl *string) (cuscalRequest, error) {
	return cuscalRequest{
		id:      id,
		url:     myUrl,
		body:    []byte(message),
		headers: []header{{ContentType, JSON}, {TraceId, "newUuid"}},
	}, nil
}
