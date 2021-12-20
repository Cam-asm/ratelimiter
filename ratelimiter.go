// Package ratelimiter implements a way to send and requeue messages at a given rate of TPS.SendEvery.
package ratelimiter

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/service/sqs"
)

const (
	readSize           = 18 // Maximum quantity of SQS messages that can be processed before being sent.
	requeueSize        = 10 // The maximum quantity of SQS messages that can be batched together in one request.
	checkXTimesPerWait = 8
	minRequeueWait     = checkXTimesPerWait * time.Second
	maxCheckTime       = 15 * time.Second
)

// Start creates the required channels and wait groups on TPS and then starts the necessary go routines.
func (t *TPS) Start() {
	// Set realistic defaults.
	if t.SendEvery < 2*time.Millisecond {
		log.Fatalln("t.SendEvery is too low, expecting a value => 2ms")
	}
	// There's no benefit to using TPS if the channel size is < 2.
	if t.ReadChannelSize <= 1 {
		t.ReadChannelSize = readSize
	}
	if t.MaxRequeueWait < minRequeueWait {
		t.MaxRequeueWait = minRequeueWait
	}
	t.checkEvery = t.MaxRequeueWait / checkXTimesPerWait
	if t.checkEvery > maxCheckTime {
		t.checkEvery = maxCheckTime
	}

	t.listenToQuit()

	// The read buffered channel contains requests that are ready to be sent.
	t.chanRead = make(chan unit, t.ReadChannelSize)
	// The requeue unbuffered channel receives problematic SQS messages that need to be reinserted into the SQS Queue.
	t.chanRequeue = make(chan *sqs.Message)

	// query sqs queue in a go routine
	go t.getAndProcessMessages()

	// Listens to t.chanRequeue.
	go t.listenRequeue()

	// Blocks main() from exiting while the channel t.chanRead is open.
	t.channelThroughput()

	// Once t.channelThroughput has finished, close the requeue channel t.chanRequeue.
	t.requeueShutdown()
}

// TPS (transactions per second).
type TPS struct {
	ReadChannelSize uint8
	QueueName       *string
	SendEvery       time.Duration
	MaxRequeueWait  time.Duration
	Client          http.Client
	Processor

	chanRequeue   chan *sqs.Message
	chanRead      chan unit
	wgSendRequest sync.WaitGroup
	wgRequeue     sync.WaitGroup
	quit          chan os.Signal
	checkEvery    time.Duration
}

// Gracefully trigger the server to start its shutdown procedure.
func (t *TPS) listenToQuit() {
	// Waits for all sendRequest goroutines to finish when channelThroughput() returns
	// This allows each sendAndProcess() call to complete (wait for Cuscal response and
	// save to the database).
	t.wgSendRequest.Wait()
	log.Println("ALL GO ROUTINES CLOSED")

	t.quit = make(chan os.Signal)
	signal.Notify(t.quit, os.Interrupt, syscall.SIGTERM)
	// Print out a response to the interrupt signal.
	// getAndProcessMessages may take several seconds to print a response while it's processing a batch.
	go func() {
		// block until quit channel receives an interrupt
		<-t.quit
		log.Print("\n\nTRIGGER SHUTDOWN\n\n")
		// Once the interrupt has been received, keep sending it to the quit channel.
		// Each TPS.getAndProcessMessages doesn't listen to the TPS.quit channel while processing a batch.
		for {
			t.quit <- os.Interrupt
		}
	}()
}

func (t *TPS) requeueShutdown() {
	log.Println(*t.QueueName, "Close requeue channel")
	close(t.chanRequeue)
	// Wait for all remaining messages to be requeued.
	t.wgRequeue.Wait()
}

// worker to query sqs queue
func (t *TPS) getAndProcessMessages() {
	log.Println("getAndProcessMessages")
	var c uint // Used only for logging purposes.

	for {
		// call sqs to get the next 10 requests
		messages, err := t.ReceiveAndDeleteMessages()
		if err != nil {
			log.Println(err)
			continue
		}

		// loop over each request
		for i := range messages {
			if messages[i].Body == nil {
				log.Println("Message body is empty :(")
				continue
			}

			c++
			var request *http.Request
			request, err = t.ProcessMessage(messages[i].Body)
			if err != nil || request == nil {
				log.Println(err)
				continue
			}

			// Retrieve the retry counter.
			var u unit
			_ = json.Unmarshal([]byte(*messages[i].Body), &u)
			// The error is not important because the retry counter is optional.

			u.Id = c
			u.Message = messages[i]
			u.Request = request

			// if there wasn't an issue processing the message,
			// then send the message to the channel.
			// If the channel doesn't have any space available then
			// this line blocks any further processing
			log.Println(*t.QueueName, "processing message:", u.Id)
			t.chanRead <- u
		}

		// this could use the q channel from listenToQuit - but I want to keep this simple,
		// and I don't want to forcefully quit from this loop.
		// This can take several seconds to return - due to the if statement placement below the blocking `t.chanRead <- request`
		// and wanting to complete up to 10 outstanding sqs messages read.
		select {
		case <-t.quit:
			// Close the channel
			close(t.chanRead)
			log.Print("\n\n\n\t\tquit ", *t.QueueName, ", LastMessageID ==", c, "\n\n\n")
			return
		default:
			log.Println("\t\t\t\t\t\t\t\tCONTINUE!!!")
			continue
		}
	}
}

func (t *TPS) channelThroughput() {
	// limit the rate that we receive from the channel readyToSend to once a second
	for range time.NewTicker(t.SendEvery).C {
		// wait to receive a message in readyToSend.
		// If the channel doesn't contain any items, then it blocks and waits on this line.
		cr, ok := <-t.chanRead
		log.Println("\t\t\t\t\t\t>>>:", cr.Id)
		if !ok {
			// if the channel was closed then return.
			log.Print("\n\nEXIT channelThroughput\n\n")
			return
		}

		// asynchronously make the http request and wait for the response. Then save to DB
		go t.sendAndProcess(cr)
	}
}

func (t *TPS) sendAndProcess(u unit) {
	// This WaitGroup will let the go routines sendRequest to continue processing
	// until wgSendRequest.Done is called.
	t.wgSendRequest.Add(1)
	defer t.wgSendRequest.Done()

	body, err := t.sendRequest(u.Request)
	if err != nil {
		// requeue message
		log.Println("\t\t\t\t\t\t", *t.QueueName, "Send to requeue:", *u.Message.MessageId)
		t.chanRequeue <- u.Message
		return
	}

	// Simulate a long round trip time for the response from Cuscal
	time.Sleep(time.Duration(rand.Intn(5)) * time.Second)

	err = t.ProcessResponse(body)
	if err != nil {
		log.Println("Process Response err", err)
		t.chanRequeue <- u.Message
		return
	}

	log.Println("\t\t\t\t\t\tDone:", u.Id)
}

func (t *TPS) sendRequest(r *http.Request) (body []byte, _ error) {
	response, err := t.Client.Do(r)
	if err != nil {
		return body, t.ResponseErr(err)
	}

	body, err = io.ReadAll(response.Body)
	if err != nil {
		_ = response.Body.Close()
		// return body, fmt.Errorf("error reading response body: %w", err)
		return
	}

	//TODO do we need to wrap these errors?
	//err = response.Body.Close()
	//if err != nil {
	//	//return body, fmt.Errorf("error closing response body: %w", err)
	//	return
	//}

	return body, response.Body.Close()
}

// Processor provides an interface to create custom implementation per API requirement.
type Processor interface {
	// ReceiveAndDeleteMessages queries the SQS database for the next batch of messages to be processed by ProcessMessage.
	ReceiveAndDeleteMessages() ([]*sqs.Message, error)

	// ProcessMessage uses the sqs.Message.Body to complete all pre-processing and returns a http.Request ready to be sent.
	ProcessMessage(data *string) (*http.Request, error)

	// ProcessResponse completes any remaining tasks asynchronously for successful or erroneous requests sent be SendRequest.
	ProcessResponse([]byte) error

	// ResponseErr provides a way to modify errors returned from http.Response.
	// If err == nil, then this function is not called.
	// The error returned must always be non-nil.
	ResponseErr(err error) error
	// If your implementation doesn't require this, simply implement with: `func (y yourType)ResponseErr(err error)error{return err}`
}

type unit struct {
	Request *http.Request `json:"-"`
	Message *sqs.Message  `json:"-"`
	// TODO remove Id -- used only for logging purposes.
	Id      uint  `json:"-"`
	Retries uint8 `json:"retry_count,omitempty"`
}

func (t *TPS) listenRequeue() {
	t.wgRequeue.Add(1)

	var (
		qty         uint8
		ok          bool
		toRequeue   = [requeueSize]*sqs.Message{}
		firstExpiry = time.Now().Add(t.MaxRequeueWait)
		timesUp     = make(chan time.Time)
	)

	// When a message is received it could take many hours to receive enough sqs.Message's
	// to fill the batch size t.RequeueChanSize. So this ticker has been added to bypass
	// that wait and ensure every message doesn't wait longer than TPS.MaxRequeueWait + TPS.checkEvery.
	go func() {
		for now := range time.NewTicker(t.checkEvery).C {
			timesUp <- now
		}
	}()

	for {
		select {
		// Wait for a new message to be received in t.chanRequeue or ticker timesUp to be triggered.
		case toRequeue[qty], ok = <-t.chanRequeue:
			qty++

			switch {
			case !ok:
				t.requeue(&toRequeue, &qty)
				log.Print("\n\nEND listenRequeue\n\n")
				t.wgRequeue.Done()
				return
			case qty == 1:
				firstExpiry = time.Now().Add(t.MaxRequeueWait)
			case qty == requeueSize:
				t.requeue(&toRequeue, &qty)
			}

		case now := <-timesUp:
			if qty >= 1 && now.After(firstExpiry) {
				t.requeue(&toRequeue, &qty)
			}
		}
	}
}

func (t *TPS) requeue(messages *[requeueSize]*sqs.Message, qty *uint8) {
	l := fmt.Sprintf("\t\t\t\t\t\t\t\t\t\t\t\tRequeued: %s - Requeued: ", *t.QueueName)

	for _, m := range *messages {
		if m == nil {
			l += "NIL, "
			continue
		}

		l += fmt.Sprintf("%s, ", *m.MessageId)
	}
	log.Println(l)

	// Clear all messages to prevent double requeued.
	*messages = [requeueSize]*sqs.Message{}
	// Set the quantity of messages
	*qty = 0
}
