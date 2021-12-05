// Package ratelimiter implements a way to send and requeue messages at a given rate of TPS.SendEvery.
package ratelimiter

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

const (
	readSize           = 18 // Maximum quantity of SQS messages that can be processed before being sent.
	requeueSize        = 10 // The maximum quantity of SQS messages that can be batched together in one request.
	checkXTimesPerWait = 8
	minRequeueWait     = checkXTimesPerWait * time.Second
	maxCheckTime       = 15 * time.Second
)

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

	// readyToSend channel contains a list of Cuscal request ready to be sent
	t.chanRead = make(chan CuscalRequest, t.ReadChannelSize)
	t.chanRequeue = make(chan *SqsMessage)

	// query sqs queue in a go routine
	go t.getAndProcessMessages()

	go t.listenRequeue()

	// blocks main() from exiting while there are no
	t.rateLimitSendMessages()

	// Waits for all sendRequest goroutines to finish when rateLimitSendMessages() returns
	// This allows each sendRequest() call to complete (wait for Cuscal response and
	// save to the database)
	t.wgSendRequest.Wait()
	log.Println("ALL GO ROUTINES CLOSED")

	t.requeueShutdown()
}

type TPS struct {
	ReadChannelSize uint8
	QueueName       *string
	SendEvery       time.Duration
	MaxRequeueWait  time.Duration
	Interface

	chanRequeue   chan *SqsMessage
	chanRead      chan CuscalRequest
	wgSendRequest sync.WaitGroup
	wgRequeue     sync.WaitGroup
	quit          chan os.Signal
	checkEvery    time.Duration
}

// Gracefully trigger the server to start its shutdown procedure.
func (t *TPS) listenToQuit() {
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
	var u uint // Used only for logging purposes.

	for {
		// call sqs to get the next 10 requests
		messages, err := t.ReceiveAndDeleteMessages()
		if err != nil {
			log.Println(err)
			continue
		}

		// loop over each request
		for i := range messages {
			u++
			messages[i].Id = u
			// process each message - marshal the json payload ready for sending.
			var request CuscalRequest
			request, err = t.ProcessMessage(messages[i])
			if err != nil {
				log.Println(err)
				continue
			}

			// if there wasn't an issue processing the message,
			// then send the message to the channel
			// if the channel doesn't have any space available then
			// this line blocks any further processing
			log.Println(*t.QueueName, "processing message:", request.Id)
			t.chanRead <- request

		}

		// this could use the q channel from listenToQuit - but I want to keep this simple,
		// and I don't want to forcefully quit from this loop.
		// This can take several seconds to return - due to the if statement placement below the blocking `t.chanRead <- request`
		// and wanting to complete up to 10 outstanding sqs messages read.
		select {
		case <-t.quit:
			// Close the channel
			close(t.chanRead)
			log.Print("\n\n\n\t\tquit ", *t.QueueName, ", LastMessageID ==", u, "\n\n\n")
			return
		default:
			log.Println("\t\t\t\t\t\t\t\tCONTINUE!!!")
			continue
		}
	}
}

func (t *TPS) rateLimitSendMessages() {
	// limit the rate that we receive from the channel readyToSend to once a second
	for range time.NewTicker(t.SendEvery).C {
		// wait to receive a message in readyToSend.
		// If the channel doesn't contain any items, then it blocks and waits on this line.
		cr, ok := <-t.chanRead
		log.Println("\t\t\t\t\t\t>>>:", cr.Id)
		if !ok {
			// if the channel was closed then return.

			/*// I've never encountered this problem - so it's disabled.
			if !cr.IsEmpty() {
				log.Println("cr is not meant to be populated")
				go sendRequest(cr, ok)
			}*/
			log.Print("\n\nEXIT rateLimitSendMessages\n\n")
			return
		}

		// asynchronously make the http request and wait for the response. Then save to DB
		// cr.sendRequest()
		if err := t.SendRequest(cr); err != nil {
			// requeue message
			log.Println("\t\t\t\t\t\t", *t.QueueName, "Send to requeue:", cr.Id)
			t.chanRequeue <- cr.toMessage(t.QueueName)
			continue
		}

		go t.waitResponse(cr)
		log.Println("\t\t\t\t\t\tDone:", cr.Id)
	}
}

func (t *TPS) waitResponse(c CuscalRequest) {
	// This WaitGroup will let the go routines sendRequest to continue processing
	// until wgSendRequest.Done is called.
	t.wgSendRequest.Add(1)
	defer t.wgSendRequest.Done()

	// Simulate a long round trip time for the response from Cuscal
	if rand.Intn(4) == 3 {
		time.Sleep(5 * time.Second)
	}

	err := t.ProcessResponse(c)
	if err != nil {
		log.Println("Process Response err", err)
		return
	}

	log.Println("save to database message:", c.Id)
}

type Interface interface {
	ReceiveAndDeleteMessages() ([]SqsMessage, error)
	ProcessMessage(SqsMessage) (CuscalRequest, error)
	SendRequest(CuscalRequest) error
	ProcessResponse(CuscalRequest) error
}

func (c *CuscalRequest) toMessage(Type *string) *SqsMessage {
	// Additional steps might be required here.
	return &SqsMessage{
		Id:      c.Id,
		Body:    c.Body,
		Retries: c.Retries + 1,
		Type:    Type,
	}
}

type SqsMessage struct {
	Id      uint
	Body    []byte
	Retries uint8
	Type    *string
}

type CuscalRequest struct {
	Id      uint
	Url     string
	Body    []byte
	Headers []Header
	Retries uint8
}

type Header struct {
	Header, Value string
}

func (t *TPS) listenRequeue() {
	t.wgRequeue.Add(1)

	var (
		qty         uint8
		ok          bool
		toRequeue   = [requeueSize]*SqsMessage{}
		firstExpiry = time.Now().Add(t.MaxRequeueWait)
		timesUp     = make(chan time.Time)
	)

	// When a message is received it could take many hours to receive enough SqsMessage's
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
				t.Requeue(&toRequeue, &qty)
				log.Print("\n\nEND listenRequeue\n\n")
				t.wgRequeue.Done()
				return
			case qty == 1:
				firstExpiry = time.Now().Add(t.MaxRequeueWait)
			case qty == requeueSize:
				t.Requeue(&toRequeue, &qty)
			}

		case now := <-timesUp:
			if qty >= 1 && now.After(firstExpiry) {
				t.Requeue(&toRequeue, &qty)
			}
		}
	}
}

func (t *TPS) Requeue(messages *[requeueSize]*SqsMessage, qty *uint8) {
	l := fmt.Sprintf("\t\t\t\t\t\t\t\t\t\t\t\tRequeued: %s - Requeued: ", *t.QueueName)

	for _, m := range *messages {
		if m == nil {
			l += "NIL, "
			continue
		}

		l += fmt.Sprintf("%d, ", m.Id)
	}
	log.Println(l)

	// Clear all messages to prevent double requeued.
	*messages = [requeueSize]*SqsMessage{}
	// Set the quantity of messages
	*qty = 0
}
