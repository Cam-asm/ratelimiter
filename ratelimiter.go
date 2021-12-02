package ratelimiter

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func (t *TPS) Start() {
	t.listenToQuit()

	// readyToSend channel contains a list of Cuscal request ready to be sent
	t.chanRead = make(chan CuscalRequest, t.ReadChannelSize)

	// query sqs queue in a go routine
	go t.getAndProcessMessages()

	go t.listenRequeue()

	// blocks main() from exiting while there are no
	t.rateLimitSendMessages()

	// Waits for all sendRequest goroutines to finish when rateLimitSendMessages() returns
	// This allows each sendRequest() call to complete (wait for Cuscal response and
	// save to the database)
	t.wgSendRequest.Wait()
	fmt.Println("ALL GO ROUTINES CLOSED")

	t.requeueShutdown()
}

type TPS struct {
	ReadChannelSize uint8
	RequeueChanSize uint8
	chanQueue       chan *SqsMessage
	chanRead        chan CuscalRequest
	Interface
	wgSendRequest sync.WaitGroup
	Url           *string
	QueueName     *string
	quitRequeue   bool
	quitRead      bool
}

// Gracefully trigger the server to start its shutdown procedure
func (t *TPS) listenToQuit() {
	q := make(chan os.Signal)
	signal.Notify(q, os.Interrupt, syscall.SIGTERM)
	go func() {
		// block until q channel receives an interrupt
		<-q
		fmt.Print("\n\nTRIGGER SHUTDOWN\n\n")
		t.quitRead = true
	}()
}

func (t *TPS) requeueShutdown() {
	t.quitRequeue = true
	var i uint8
	for i = 1; i <= t.RequeueChanSize; i++ {
		fmt.Println("Send requeue nil", i)
		t.chanQueue <- nil
	}
}

// worker to query sqs queue
func (t *TPS) getAndProcessMessages() {
	var u uint // Used only for logging purposes.

	for {
		// call sqs to get the next 10 requests
		messages, err := t.ReceiveAndDeleteMsgs()
		if err != nil {
			fmt.Println(err)
			continue
		}

		// loop over each request
		for i := range messages {
			fmt.Println("processing message:", u)
			// process each message - marshal the json payload ready for sending.
			var request CuscalRequest
			request, err = t.ProcessMessage(messages[i], t.Url)
			if err != nil {
				fmt.Println(err)
				continue
			}

			// if there wasn't an issue processing the message,
			// then send the message to the channel
			// if the channel doesn't have any space available then
			// this line blocks any further processing
			t.chanRead <- request

			u++
		}

		// this could use the q channel from listenToQuit - but I want to keep this simple,
		// and I don't want to forcefully quit from this loop.
		// This can take several seconds to return - due to the if statement placement below the blocking `t.chanRead <- request`
		// and wanting to complete up to 10 outstanding sqs messages read.
		if t.quitRead {
			// Close the channel
			close(t.chanRead)
			fmt.Print("\n\n\n\t\tquit ==", u, "\n\n\n")
			return
		}
	}
}

func (t *TPS) rateLimitSendMessages() {
	// limit the rate that we receive from the channel readyToSend to once a second
	for range time.NewTicker(time.Second).C {
		// wait to receive a message in readyToSend.
		// If the channel doesn't contain any items, then it blocks and waits on this line.
		cr, ok := <-t.chanRead
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
		// cr.sendRequest()
		if err := t.SendRequest(cr); err != nil {
			// requeue message
			t.chanQueue <- cr.convertMessage(t.QueueName)
		} else {
			go t.waitResponse(cr)
		}
	}
}

var delaySomeRequests bool

func (t *TPS) waitResponse(c CuscalRequest) {
	// This WaitGroup will let the go routines sendRequest to continue processing
	// until wgSendRequest.Done is called.
	t.wgSendRequest.Add(1)
	defer t.wgSendRequest.Done()

	// Simulate a long round trip time for the response from Cuscal
	if delaySomeRequests {
		time.Sleep(5 * time.Second)
	}
	delaySomeRequests = !delaySomeRequests

	err := t.ProcessResponse(c)
	if err != nil {
		fmt.Println("Process Response err", err)
		return
	}

	fmt.Println("save to database message:", c.Id)
}

type Interface interface {
	ReceiveAndDeleteMsgs() ([]SqsMessage, error)
	ProcessMessage(message SqsMessage, urlPattern *string) (CuscalRequest, error) // maybe change Url *string to type Url?
	SendRequest(request CuscalRequest) error
	ProcessResponse(request CuscalRequest) error
}

func (c *CuscalRequest) convertMessage(Type *string) *SqsMessage {
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
	Id   uint
	Url  *string
	Body []byte
	// Headers []header
	Retries uint8
}

//type header struct {
//	header, value string
//}

/*type ActivateVACommand struct {
	Id         uuid.UUID      `json:"Id" validate:"required"`
	EntityType string         `json:"entity_type" validate:"required"`
	Type       string         `json:"type" validate:"required"`
	Data       ActivateVAData `json:"data" validate:"required"`
	RetryCount int            `json:"retry_count"`
}*/

func (t *TPS) listenRequeue() {
	for {
		// wait for 10 items from readyToQueue
		toRequeue := []*SqsMessage{<-t.chanQueue, <-t.chanQueue, <-t.chanQueue, <-t.chanQueue, <-t.chanQueue, <-t.chanQueue, <-t.chanQueue, <-t.chanQueue, <-t.chanQueue, <-t.chanQueue}
		for i := range toRequeue {
			if toRequeue[i] == nil {
				// At shutdown send nil to trigger the rest of the messages to be requeued.
				// I don't think there's a better way to do it?
				continue
			}

			toRequeue[i].Requeue2(t.QueueName)
		}

		if t.quitRequeue {
			fmt.Print("\n\nEND listenRequeue\n\n")
			return
		}
	}
}

func (m *SqsMessage) Requeue2(queueName *string) {
	fmt.Println("Requeue", m.Id, "in", *queueName, string(m.Body))
}
