package pubsub

import (
	"context"
	"github.com/go-redis/redis/v8"
	"github.com/qmstar0/eio"
	"github.com/qmstar0/lock"
)

type RedisPublisher interface {
	eio.Publisher
}

type RedisSubscriber interface {
	eio.Subscriber
}

type redisPublisher struct {
	cli *redis.Client
}

type redisSubscriber struct {
	cli    *redis.Client
	closer lock.Closer
}

func NewRedisPublisher(client *redis.Client) RedisPublisher {
	return &redisPublisher{cli: client}
}

func NewRedisSubscriber(client *redis.Client) RedisSubscriber {
	return &redisSubscriber{
		cli:    client,
		closer: lock.NewCloser(),
	}
}

func (r *redisPublisher) Publish(ctx context.Context, topic string, message eio.Message) error {
	return r.cli.Publish(ctx, topic, []byte(message)).Err()
}

func (r *redisPublisher) Close() {}

func (r *redisSubscriber) Subscribe(ctx context.Context, topic string) (<-chan eio.Message, error) {
	subscribe := r.cli.Subscribe(ctx, topic)
	_, err := subscribe.Receive(ctx)
	if err != nil {
		return nil, err
	}

	eioChannel := make(chan eio.Message)

	go r.startForward(ctx, subscribe.Channel(), eioChannel)

	return eioChannel, nil
}

func (r *redisSubscriber) startForward(ctx context.Context, inChan <-chan *redis.Message, outChan chan<- eio.Message) {
	defer close(outChan)
	defer r.Close()

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.closer.Closing():
			return
		case msg := <-inChan:
			outChan <- eio.Message(msg.Payload)
		}
	}
}

func (r *redisSubscriber) Close() {
	_ = r.closer.CloseAndRunOnce(nil)
}
