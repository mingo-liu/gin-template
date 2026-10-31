package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"gin-template/internal/model"
	mq "github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

type Queue struct {
	Producer mq.Producer
	Consumer mq.PushConsumer
	Topic    string
}

func New(nameserver, topic string, apply func(context.Context, model.Event) error) (*Queue, error) {
	addresses := primitive.NamesrvAddr(strings.Split(nameserver, ","))
	p, err := mq.NewProducer(producer.WithNameServer(addresses), producer.WithGroupName("like-producer"), producer.WithRetry(2), producer.WithSendMsgTimeout(3*time.Second))
	if err != nil {
		return nil, err
	}
	c, err := mq.NewPushConsumer(consumer.WithNameServer(addresses), consumer.WithGroupName("like-persistence"), consumer.WithConsumeFromWhere(consumer.ConsumeFromFirstOffset), consumer.WithConsumeGoroutineNums(4), consumer.WithConsumeMessageBatchMaxSize(1), consumer.WithMaxReconsumeTimes(16))
	if err != nil {
		return nil, err
	}
	err = c.Subscribe(topic, consumer.MessageSelector{}, func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, msg := range msgs {
			var e model.Event
			err := json.Unmarshal(msg.Body, &e)
			if err == nil {
				err = e.Validate()
			}
			if err == nil {
				work, cancel := context.WithTimeout(ctx, 10*time.Second)
				err = apply(work, e)
				cancel()
			}
			if err != nil {
				slog.Error("MQ consumption failed; retry/dead-letter requires attention", "event_id", e.EventID, "reconsume", msg.ReconsumeTimes, "error", err)
				return consumer.ConsumeRetryLater, nil
			}
		}
		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		return nil, err
	}
	return &Queue{Producer: p, Consumer: c, Topic: topic}, nil
}

func (q *Queue) Start() error {
	if err := q.Producer.Start(); err != nil {
		return err
	}
	if err := q.Consumer.Start(); err != nil {
		q.Producer.Shutdown()
		return err
	}
	return nil
}
func (q *Queue) Close() {
	if err := q.Consumer.Shutdown(); err != nil {
		slog.Error("consumer shutdown", "error", err)
	}
	if err := q.Producer.Shutdown(); err != nil {
		slog.Error("producer shutdown", "error", err)
	}
}

// Send requires a successful broker flush result before callers acknowledge the Stream entry.
func (q *Queue) Send(ctx context.Context, e model.Event) error {
	if err := e.Validate(); err != nil {
		return err
	}
	body, err := json.Marshal(e)
	if err != nil {
		return err
	}
	msg := primitive.NewMessage(q.Topic, body)
	msg.WithKeys([]string{e.EventID})
	result, err := q.Producer.SendSync(ctx, msg)
	if err != nil {
		return err
	}
	if result == nil {
		return errors.New("empty send result")
	}
	if result.Status != primitive.SendOK {
		return fmt.Errorf("broker confirmation status: %v", result.Status)
	}
	return nil
}
