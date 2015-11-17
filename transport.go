package amqp

import (
	"errors"
	"fmt"
	"sync"

	"code.google.com/p/go-uuid/uuid"

	"github.com/achilleasa/usrv"
	amqpDrv "github.com/streadway/amqp"
)

type amqpTransport struct {
	logger usrv.Logger

	amqpEndpoint string
	connection   *amqpDrv.Connection
	channel      *amqpDrv.Channel

	sendQueueChan chan *amqpMessage

	// A waitgroup used to wait for all bind-spawned gofuncs to exit
	wg sync.WaitGroup

	// A mutex for preventing multiple connection attempts
	sync.Mutex
}

type AmqpConfig map[string]string

func NewAmqpConfig(endpoint string) AmqpConfig {
	return AmqpConfig{
		"endpoint": endpoint,
	}
}

func NewAmqp() usrv.Transport {
	return &amqpTransport{
		logger:        usrv.NullLogger,
		sendQueueChan: make(chan *amqpMessage, 0),
	}
}

func (t *amqpTransport) isConnected() bool {
	t.Lock()
	defer t.Unlock()

	return t.connection != nil
}

func (t *amqpTransport) dial() error {
	t.Lock()
	defer t.Unlock()

	// Already connected
	if t.connection != nil {
		return nil
	}

	var err error
	t.connection, err = amqpDrv.Dial(t.amqpEndpoint)
	if err != nil {
		return err
	}

	t.channel, err = t.connection.Channel()
	if err != nil {
		t.connection.Close()
		t.connection = nil
		return err
	}

	// Allocate a private queue for receiving replies
	replyQueue, err := t.channel.QueueDeclare(
		"",
		false, // durable
		true,  // delete when unused
		false, // exclusive
		false, // noWait
		nil,   // args
	)
	if err != nil {
		t.channel.Close()
		t.connection.Close()
		t.connection = nil
		t.channel = nil
		return err
	}

	// Create queue consumer
	deliveryChan, err := t.channel.Consume(
		replyQueue.Name, // name
		replyQueue.Name, // consumerTag (use same as queue name)
		false,           // noAck
		false,           // exclusive
		false,           // noLocal
		false,           // noWait
		nil,             // arguments
	)
	if err != nil {
		t.channel.Close()
		t.connection.Close()
		t.connection = nil
		t.channel = nil
		return err
	}

	t.wg.Add(1)
	go t.sendQueue(deliveryChan, replyQueue.Name)

	t.logger.Info("Connected", "endpoint", t.amqpEndpoint)

	return nil
}

func (t *amqpTransport) SetLogger(logger usrv.Logger) {
	t.logger = logger
}

func (t *amqpTransport) Config(params map[string]string) error {
	needsReset := false

	endpoint, exists := params["endpoint"]
	if exists {
		t.amqpEndpoint = endpoint
		needsReset = true
	}

	if needsReset {
		t.logger.Info("Configuration changed", "endpoint", t.amqpEndpoint)
		if t.isConnected() {
			err := t.Close()
			if err != nil {
				return err
			}
		}

		return t.dial()
	}

	return nil
}

func (t *amqpTransport) Close() error {
	t.Lock()
	defer t.Unlock()

	// Not connected
	if t.connection == nil {
		return nil
	}

	t.logger.Info("Shutting down")

	// Shutdown amqp. This will kill all delivery channels
	err := t.channel.Close()
	if err != nil {
		t.logger.Error("Shut down failed", "error", err.Error())
		return err
	}

	// Wait for all binding-spawned gofuncs to exit
	t.wg.Wait()

	t.connection.Close()
	t.connection = nil
	t.channel = nil

	t.logger.Info("Shut down complete")
	return nil
}

func (t *amqpTransport) Bind(service string, endpoint string) (<-chan usrv.Message, error) {
	err := t.dial()
	if err != nil {
		return nil, err
	}

	queueName := fmt.Sprintf("%s.%s", service, endpoint)

	// Declare queue
	queue, err := t.channel.QueueDeclare(
		queueName,
		false, // durable
		true,  // delete when unused
		false, // exclusive
		false, // noWait
		nil,   // args
	)
	if err != nil {
		return nil, err
	}

	// Create queue consumer
	deliveryChan, err := t.channel.Consume(
		queue.Name, // name
		queue.Name, // consumerTag (use same as queue name)
		false,      // noAck
		false,      // exclusive
		false,      // noLocal
		false,      // noWait
		nil,        // arguments
	)
	if err != nil {
		return nil, err
	}

	// Allocate msg channel and spawn a go func to process incoming messages
	msgChan := make(chan usrv.Message, 0)

	t.wg.Add(1)
	go t.handleMessage(deliveryChan, msgChan)

	return msgChan, nil
}

func (t *amqpTransport) Send(m usrv.Message, expectReply bool) <-chan usrv.Message {
	msg, ok := m.(*amqpMessage)
	if !ok {
		panic("Unsupported message type")
	}

	// Handle replies
	if msg.isReply {
		// No reply endpoint specified; nothing to do
		if msg.replyTo == "" {
			return nil
		}

		// Setup properties
		headers := make(amqpDrv.Table, 0)
		for k, v := range msg.property {
			headers[k] = v
		}

		// Setup message content
		content, err := msg.Content()
		if err != nil {
			headers[usrv.PropertyHasError] = err.Error()
			content = nil
		}

		t.channel.Publish(
			"",
			msg.replyTo,
			false,
			false,
			amqpDrv.Publishing{
				Headers:       headers,
				CorrelationId: msg.correlationId,
				AppId:         msg.from,
				Body:          content,
			},
		)

		return nil
	}

	// Allocate response channel and pass message to our send queue
	if expectReply {
		msg.replyChan = make(chan usrv.Message, 0)
	}
	t.sendQueueChan <- msg

	return msg.replyChan
}

// Create a message to be delivered to a target endpoint
func (t *amqpTransport) MessageTo(from string, toService string, toEndpoint string) usrv.Message {
	return &amqpMessage{
		from:          from,
		to:            fmt.Sprintf("%s.%s", toService, toEndpoint),
		property:      make(usrv.Property, 0),
		correlationId: uuid.New(),
	}
}

func (t *amqpTransport) ReplyTo(msg usrv.Message) usrv.Message {
	reqMsg, ok := msg.(*amqpMessage)
	if !ok {
		panic("Unsupported message type")
	}

	return &amqpMessage{
		from:     reqMsg.to,
		to:       reqMsg.from,
		property: make(usrv.Property, 0),
		// Copy correlationId and reply address from from req message
		correlationId: reqMsg.correlationId,
		replyTo:       reqMsg.replyTo,
		isReply:       true,
	}
}

func (t *amqpTransport) sendQueue(amqpReplyChan <-chan amqpDrv.Delivery, replyQueueName string) {
	defer t.wg.Done()

	returns := make(chan amqpDrv.Return)
	returns = t.channel.NotifyReturn(returns)

	pendingReplies := make(map[string]chan usrv.Message, 0)
	for {
		select {
		case msg := <-t.sendQueueChan:
			// Setup properties
			headers := make(amqpDrv.Table, 0)
			for k, v := range msg.property {
				headers[k] = v
			}

			// Setup message content
			content, err := msg.Content()
			if err != nil {
				headers[usrv.PropertyHasError] = err.Error()
				content = nil
			}

			// Add to pending reply queue if a reply channel is specified
			replyTo := ""
			if msg.replyChan != nil {
				pendingReplies[msg.correlationId] = msg.replyChan
				replyTo = replyQueueName
			}

			t.channel.Publish(
				"",
				msg.to,
				true, // server should immediately report undelived messages
				false,
				amqpDrv.Publishing{
					Headers:       headers,
					CorrelationId: msg.correlationId,
					AppId:         msg.from,
					Body:          content,
					ReplyTo:       replyTo,
				},
			)
		case amqpDelivery, chanOpen := <-amqpReplyChan:
			// If channel closes we need to exit
			if !chanOpen {
				return
			}

			// Check correlationId against our pending reply list.
			// If no match is found, then this message will be discarded.
			replyChan, found := pendingReplies[amqpDelivery.CorrelationId]
			if !found {
				continue
			}
			delete(pendingReplies, amqpDelivery.CorrelationId)

			resMsg := &amqpMessage{
				from:          amqpDelivery.AppId,
				to:            amqpDelivery.RoutingKey,
				property:      make(usrv.Property, 0),
				replyTo:       amqpDelivery.ReplyTo,
				correlationId: amqpDelivery.CorrelationId,
			}

			// Parse properties
			content := amqpDelivery.Body
			var err error
			for k, v := range amqpDelivery.Headers {
				if k == usrv.PropertyHasError {
					err = errors.New(v.(string))
					content = nil
					continue
				}
				resMsg.property.Set(k, v.(string))
			}

			resMsg.SetContent(content, err)

			replyChan <- resMsg
			close(replyChan)
		case amqpDelivery, chanOpen := <-returns:
			// If channel closes we need to exit
			if !chanOpen {
				return
			}

			// Check correlationId against our pending reply list.
			// If no match is found, then this message will be discarded.
			replyChan, found := pendingReplies[amqpDelivery.CorrelationId]
			if !found {
				continue
			}
			delete(pendingReplies, amqpDelivery.CorrelationId)

			resMsg := &amqpMessage{
				from:          amqpDelivery.AppId,
				to:            amqpDelivery.RoutingKey,
				property:      make(usrv.Property, 0),
				replyTo:       amqpDelivery.ReplyTo,
				correlationId: amqpDelivery.CorrelationId,
			}
			resMsg.SetContent(nil, usrv.ErrServiceUnavailable)
			replyChan <- resMsg
			close(replyChan)
		}
	}
}

func (t *amqpTransport) handleMessage(amqpDeliveryChan <-chan amqpDrv.Delivery, usrvMsgChan chan usrv.Message) {
	defer t.wg.Done()

	for {
		select {
		case amqpDelivery, chanOpen := <-amqpDeliveryChan:
			// If channel closes we need to exit
			if !chanOpen {
				return
			}

			reqMsg := &amqpMessage{
				from:          amqpDelivery.AppId,
				to:            amqpDelivery.RoutingKey,
				property:      make(usrv.Property, 0),
				replyTo:       amqpDelivery.ReplyTo,
				correlationId: amqpDelivery.CorrelationId,
			}

			// Parse properties
			content := amqpDelivery.Body
			var err error
			for k, v := range amqpDelivery.Headers {
				if k == usrv.PropertyHasError {
					err = errors.New(v.(string))
					content = nil
					continue
				}
				reqMsg.property.Set(k, v.(string))
			}

			reqMsg.SetContent(content, err)

			// Emit usrv message
			usrvMsgChan <- reqMsg
		}
	}
}
