package transport

import (
	"time"

	"github.com/achilleasa/usrv"
)

// The internal message type used by the amqp transport.
type amqpMessage struct {
	from          string
	to            string
	property      usrv.Property
	content       []byte
	err           error
	correlationId string

	isReply   bool
	replyTo   string
	replyChan chan usrv.Message
	timeout   time.Duration
}

func (m *amqpMessage) From() string {
	return m.from
}
func (m *amqpMessage) To() string {
	return m.to
}
func (m *amqpMessage) Property() usrv.Property {
	return m.property
}
func (m *amqpMessage) CorrelationId() string {
	return m.correlationId
}
func (m *amqpMessage) Content() ([]byte, error) {
	return m.content, m.err
}
func (m *amqpMessage) SetContent(content []byte, err error) {
	m.content, m.err = content, err
}
