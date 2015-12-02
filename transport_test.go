package transport

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/achilleasa/usrv"
)

func getTransportConfig() AmqpConfig {
	endpoint := os.Getenv("AMQP_ENDPOINT")
	if endpoint == "" {
		endpoint = "amqp://guest:guest@localhost:5672/"
	}

	return NewAmqpConfig(endpoint)
}

func getTransport(t *testing.T) usrv.Transport {
	tr := NewAmqp()
	err := tr.Config(getTransportConfig())
	if err != nil {
		t.Fatal(err)
	}

	return tr
}

func TestAmqpTransport(t *testing.T) {
	tr := getTransport(t)
	defer tr.Close()

	reqChan, err := tr.Bind("srv", "ep1")
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		select {
		case reqMsg := <-reqChan:
			propVal := reqMsg.Property().Get("foo")
			if propVal != "bar" {
				t.Fatalf("Expected property 'foo' to have value 'bar'; got %s", propVal)
			}

			if reqMsg.From() != "test" {
				t.Fatalf("Expected request sender to be 'test'; got %s", reqMsg.From())
			}
			if reqMsg.To() != "srv.ep1" {
				t.Fatalf("Expected request recipient to be 'srv.ep1'; got %si", reqMsg.To())
			}

			resMsg := tr.ReplyTo(reqMsg)
			resMsg.Property().Set("foo", "bar")
			resMsg.SetContent([]byte("OK"), nil)
			tr.Send(resMsg, 0, false)
		}
	}()

	reqMsg := tr.MessageTo("test", "srv", "ep1")
	reqMsg.Property().Set("foo", "bar")
	resChan := tr.Send(reqMsg, 0, true)

	resMsg := <-resChan
	content, err := resMsg.Content()
	if err != nil {
		t.Fatal(err)
	}
	exp := "OK"
	if !bytes.Equal([]byte(exp), content) {
		t.Fatalf("Expected response to be %s; got %s\n", exp, string(content))
	}
	if reqMsg.CorrelationId() != resMsg.CorrelationId() {
		t.Fatalf("Expected res msg corellation id to be %s; got %s", reqMsg.CorrelationId(), resMsg.CorrelationId())
	}
	if resMsg.Property().Get("foo") != "bar" {
		t.Fatalf("Expected res msg 'foo' property to equal 'bar'; got %s", resMsg.Property().Get("foo"))
	}
}

func TestAmqpTransportTimeout(t *testing.T) {
	tr := getTransport(t)
	defer tr.Close()

	reqChan, err := tr.Bind("srv", "ep1")
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		select {
		case <-reqChan:
		}
	}()

	reqMsg := tr.MessageTo("test", "srv", "ep1")
	resChan := tr.Send(reqMsg, 1*time.Millisecond, true)

	resMsg := <-resChan
	content, err := resMsg.Content()
	if err != usrv.ErrServiceUnavailable {
		t.Fatalf("Expected to get ErrServiceUnavailable; got %v", err)
	}
	if content != nil {
		t.Fatalf("Expected content to be nil; got %v", content)
	}
}

func TestAmqpTransportError(t *testing.T) {
	tr := getTransport(t)
	tr.SetLogger(usrv.NullLogger)
	defer tr.Close()

	reqChan, err := tr.Bind("srv", "ep1")
	if err != nil {
		t.Fatal(err)
	}

	expErr := fmt.Errorf("An error")

	go func() {
		select {
		case msg := <-reqChan:
			res := tr.ReplyTo(msg)
			res.SetContent(nil, expErr)
			tr.Send(res, 0, false)
		}
	}()

	reqMsg := tr.MessageTo("test", "srv", "ep1")
	resChan := tr.Send(reqMsg, 0, true)

	resMsg := <-resChan
	content, err := resMsg.Content()
	if err.Error() != expErr.Error() {
		t.Fatalf("Expected to get error %v; got %v", expErr, err)
	}
	if content != nil {
		t.Fatalf("Expected content to be nil; got %v", content)
	}
}

func TestAmqpTransportUnknownEndpoint(t *testing.T) {
	tr := getTransport(t)
	defer tr.Close()

	reqMsg := tr.MessageTo("test", "srv", "ep1")
	resChan := tr.Send(reqMsg, 0, true)

	resMsg := <-resChan
	content, err := resMsg.Content()
	if err != usrv.ErrServiceUnavailable {
		t.Fatalf("Expected to get ErrServiceUnavailable; got %v", err)
	}
	if content != nil {
		t.Fatalf("Expected content to be nil; got %v", content)
	}
}

func TestAmqpTransportConfig(t *testing.T) {
	tr := getTransport(t)
	defer tr.Close()

	reqChan, err := tr.Bind("srv", "ep1")
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		select {
		case reqMsg := <-reqChan:
			resMsg := tr.ReplyTo(reqMsg)
			resMsg.SetContent([]byte("OK"), nil)
			tr.Send(resMsg, 0, false)
		}
	}()

	// Force reconnect
	tr.Config(getTransportConfig())

	// Message should be able to go through using the previous bindings
	reqMsg := tr.MessageTo("test", "srv", "ep1")
	reqMsg.Property().Set("foo", "bar")
	resChan := tr.Send(reqMsg, 0, true)

	resMsg := <-resChan
	content, err := resMsg.Content()
	if err != nil {
		t.Fatal(err)
	}
	exp := "OK"
	if !bytes.Equal([]byte(exp), content) {
		t.Fatalf("Expected response to be %s; got %s\n", exp, string(content))
	}
}

func TestAmqpTransportDualBinding(t *testing.T) {
	tr := getTransport(t)
	defer tr.Close()

	chan1, err := tr.Bind("srv", "ep1")
	if err != nil {
		t.Fatal(err)
	}

	chan2, err := tr.Bind("srv", "ep1")
	if err != nil {
		t.Fatal(err)
	}

	if chan1 != chan2 {
		t.Fatal("Expected Bind() to return the same channel for the same service, endpoint tuple")
	}
}
