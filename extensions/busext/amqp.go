package busext

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/spf13/viper"
	"github.com/streadway/amqp"

	"github.com/shanbay/gobay"
)

var (
	errNotConnected  = errors.New("not connected to a server")
	errAlreadyClosed = errors.New("already closed: not connected to the server")
	errShutdown      = errors.New("BusExt is closed")
)

const (
	defaultResendDelay    = "1s"
	defaultReconnectDelay = "2s"
	defaultReinitDelay    = "1s"
	defaultPrefetch       = 100
	defaultPublishRetry   = 3
)

type customLoggerInterface interface {
	Printf(string, ...interface{})
	Println(...interface{})
	Fatal(...interface{})
	Fatalf(string, ...interface{})
}

type BusExt struct {
	NS              string
	app             *gobay.Application
	connection      *amqp.Connection
	channel         *amqp.Channel
	done            chan bool
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation
	isReady         bool
	config          *viper.Viper
	consumers       map[string]Handler
	consumeChannels map[string]<-chan amqp.Delivery
	publishRetry    int
	prefetch        int
	resendDelay     time.Duration
	reconnectDelay  time.Duration
	reinitDelay     time.Duration
	logger          customLoggerInterface
}

func (b *BusExt) Object() interface{} {
	return b.channel
}

func (b *BusExt) Application() *gobay.Application {
	return b.app
}

func (b *BusExt) Init(app *gobay.Application) error {
	if b.NS == "" {
		return errors.New("lack of NS")
	}
	b.app = app
	config := app.Config()
	b.config = gobay.GetConfigByPrefix(config, b.NS, true)
	setDefaultConfig(b.config)
	b.consumers = make(map[string]Handler)
	b.consumeChannels = make(map[string]<-chan amqp.Delivery)
	b.prefetch = b.config.GetInt("prefetch")
	b.publishRetry = b.config.GetInt("publish_retry")
	b.resendDelay = b.config.GetDuration("resend_delay")
	b.reconnectDelay = b.config.GetDuration("reconnect_delay")
	b.reinitDelay = b.config.GetDuration("reinit_delay")
	brokerUrl := b.config.GetString("broker_url")
	go b.handleReconnect(brokerUrl)
	for {
		if !b.isReady {
			continue
		} else {
			break
		}
	}
	b.logger.Println("BusExt init done")
	return nil
}

func (b *BusExt) Close() error {
	if !b.isReady {
		return errAlreadyClosed
	}
	if err := b.channel.Close(); err != nil {
		b.logger.Fatalf("close channel failed: %v\n", err)
		return err
	}
	if err := b.connection.Close(); err != nil {
		b.logger.Fatalf("close connection failed: %v\n", err)
		return err
	}
	close(b.done)
	b.isReady = false
	b.logger.Println("BusExt closed")
	return nil
}

func (b *BusExt) Push(exchange, routingKey string, data amqp.Publishing) error {
	b.logger.Printf("Trying to publish: %+v\n", data)
	if !b.isReady {
		err := errors.New("BusExt is not ready")
		b.logger.Fatalf("Can not publish message: %v\n", err)
		return err
	}
	for i := 0; i < b.publishRetry; i++ {
		err := b.UnsafePush(exchange, routingKey, data)
		if err != nil {
			b.logger.Fatalf("UnsafePush failed: %v\n", err)
			select {
			case <-b.done:
				b.logger.Println("BusExt closed during publishing message")
				return errShutdown
			case <-time.After(b.resendDelay):
			}
			continue
		}
		select {
		case confirm := <-b.notifyConfirm:
			if confirm.Ack {
				b.logger.Println("Publish confirmed!")
				return nil
			}
		case <-time.After(b.resendDelay):
		}
		b.logger.Printf("Publish not confirmed after %f seconds. Retrying...\n",
			b.resendDelay.Seconds())
	}
	err := fmt.Errorf(
		"publishing message failed after retry %d times", b.publishRetry)
	b.logger.Fatal(err)
	return err
}

func (b *BusExt) UnsafePush(exchange, routingKey string, data amqp.Publishing) error {
	if !b.isReady {
		return errNotConnected
	}
	return b.channel.Publish(
		exchange,   // Exchange
		routingKey, // Routing key
		false,      // Mandatory
		false,      // Immediate
		data,
	)
}

func (b *BusExt) Register(routingKey string, handler Handler) {
	b.consumers[routingKey] = handler
}

func (b *BusExt) Consume() error {
	if !b.isReady {
		b.logger.Fatal("can not consume. BusExt is not ready")
		return errNotConnected
	}
	if err := b.channel.Qos(b.prefetch, 0, false); err != nil {
		b.logger.Fatalf("set qos failed: %v\n", err)
	}
	hostName, err := os.Hostname()
	if err != nil {
		b.logger.Fatalf("get host name failed: %v\n", err)
	}
	for _, queue := range b.config.GetStringSlice("queues") {
		ch, err := b.channel.Consume(
			queue,
			hostName,
			false,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			b.logger.Fatalf("StartWorker queue: %v failed: %v\n", queue, err)
			return err
		}
		b.consumeChannels[queue] = ch
	}
	wg := sync.WaitGroup{}
	for name, ch := range b.consumeChannels {
		chName := name
		channel := ch
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-b.done:
					return
				case delivery := <-channel:
					b.deliveryAck(delivery)
					b.logger.Printf("Receive delivery: %+v from queue: %v\n",
						delivery, chName)
					var handler Handler
					var ok bool
					if delivery.Headers == nil {
						b.logger.Fatal("Not support v1 celery protocol yet")
					} else if delivery.ContentType != "application/json" {
						b.logger.Fatal("Only json encoding is allowed")
					} else if delivery.ContentEncoding != "utf-8" {
						b.logger.Fatal("Unsupported content encoding")
					} else if handler, ok = b.consumers[delivery.RoutingKey]; !ok {
						b.logger.Fatal("Receive unregistered message")
					} else {
						var payload []json.RawMessage
						if err := json.Unmarshal(delivery.Body, &payload); err != nil {
							b.logger.Fatalf("json decode error: %v\n", err)
						} else if err := handler.ParsePayload(payload[0],
							payload[1]); err != nil {
							b.logger.Fatalf("handler parse payload error: %v\n", err)
						} else if err := handler.Run(context.Background()); err != nil {
							b.logger.Fatalf("handler run task failed: %v\n", err)
						}
					}
				}
			}
		}()
	}
	wg.Wait()
	return nil
}

func (b *BusExt) handleReconnect(brokerUrl string) {
	for {
		b.isReady = false
		b.logger.Printf("Attempting to connect to %v\n", brokerUrl)

		conn, err := b.connect(brokerUrl)

		if err != nil {
			b.logger.Fatalf("Failed to connect: %v. Retrying...\n", err)
			select {
			case <-b.done:
				return
			case <-time.After(b.reconnectDelay):
			}
			continue
		}

		if done := b.handleReInit(conn); done {
			break
		}
	}
}

func (b *BusExt) connect(brokerUrl string) (*amqp.Connection, error) {
	conn, err := amqp.Dial(brokerUrl)

	if err != nil {
		return nil, err
	}

	b.changeConnection(conn)
	b.logger.Println("Connected!")
	return conn, nil
}

func (b *BusExt) handleReInit(conn *amqp.Connection) bool {
	for {
		b.isReady = false

		err := b.init(conn)

		if err != nil {
			b.logger.Fatalf("Failed to initialize channel: %v. Retrying...\n", err)

			select {
			case <-b.done:
				return true
			case <-time.After(b.reinitDelay):
			}
			continue
		}

		select {
		case <-b.done:
			return true
		case <-b.notifyConnClose:
			b.logger.Println("Connection closed. Reconnecting...")
			return false
		case <-b.notifyChanClose:
			b.logger.Println("channel closed. Rerunning init...")
		}
	}
}

func (b *BusExt) init(conn *amqp.Connection) error {
	ch, err := conn.Channel()

	if err != nil {
		b.logger.Fatalf("create channel failed: %v\n", err)
		return err
	}

	err = ch.Confirm(false)

	if err != nil {
		b.logger.Fatalf("change to confirm mod failed: %v\n", err)
		return err
	}

	for _, exchange := range b.config.GetStringSlice("exhanges") {
		err = ch.ExchangeDeclare(
			exchange,
			amqp.ExchangeTopic,
			true,
			false,
			false,
			false,
			nil)

		if err != nil {
			b.logger.Fatalf("declare exchange: %v failed: %v\n", exchange, err)
			return err
		}
		b.logger.Printf("declare exchange: %v succeeded\n", exchange)
	}

	for _, queue := range b.config.GetStringSlice("queues") {
		_, err = ch.QueueDeclare(
			queue,
			true,  // Durable
			false, // Delete when unused
			false, // Exclusive
			false, // No-wait
			nil,   // Arguments
		)

		if err != nil {
			b.logger.Fatalf("declare queue: %v failed: %v\n", queue, err)
			return err
		}
		b.logger.Printf("declare queue: %v succeeded\n", queue)
	}

	var bs []map[string]string
	if err := b.config.UnmarshalKey("bindings", &bs); err != nil {
		b.logger.Fatalf("unmarshal bindings failed: %v\n", err)
		return err
	}
	for _, binding := range bs {
		if err := ch.QueueBind(
			binding["queue"],
			binding["binding_key"],
			binding["exchange"],
			false,
			nil); err != nil {
			b.logger.Fatalf("declare binding: %v failed: %v\n", binding, err)
			return err
		}
		b.logger.Printf("declare binding: %v succeeded\n", binding)
	}

	b.changeChannel(ch)
	b.isReady = true
	if len(b.consumers) > 0 {
		b.consumeChannels = make(map[string]<-chan amqp.Delivery)
		go func() {
			err := b.Consume()
			if err != nil {
				b.logger.Fatalf("errors occur when consume: %v\n", err)
			}
		}()
	}
	b.logger.Println("init finished")

	return nil
}

func (b *BusExt) changeConnection(connection *amqp.Connection) {
	b.connection = connection
	b.notifyConnClose = make(chan *amqp.Error)
	b.connection.NotifyClose(b.notifyConnClose)
	b.logger.Println("connection changed")

}

func (b *BusExt) changeChannel(channel *amqp.Channel) {
	b.channel = channel
	b.notifyChanClose = make(chan *amqp.Error)
	b.notifyConfirm = make(chan amqp.Confirmation, 1)
	b.channel.NotifyClose(b.notifyChanClose)
	b.channel.NotifyPublish(b.notifyConfirm)
	b.logger.Println("channel changed")
}

func (b *BusExt) deliveryAck(delivery amqp.Delivery) {
	var err error
	for retryCount := 3; retryCount > 0; retryCount-- {
		if err = delivery.Ack(false); err == nil {
			break
		}
	}
	if err != nil {
		b.logger.Fatalf("failed to ack delivery: %+v"+
			": %+v\n",
			delivery.MessageId, err)
	}
}

func setDefaultConfig(v *viper.Viper) {
	v.SetDefault("prefetch", defaultPrefetch)
	v.SetDefault("publish_retry", defaultPublishRetry)
	v.SetDefault("resend_delay", defaultResendDelay)
	v.SetDefault("reconnect_delay", defaultReconnectDelay)
	v.SetDefault("reinit_delay", defaultReinitDelay)
}
