package pubsub_test

import (
	"context"
	"github.com/go-redis/redis/v8"
	"github.com/qmstar0/eio"
	"github.com/qmstar0/eio-redis/pubsub"
	"testing"
	"time"
)

var (
	encoder = eio.NewGobCodec()
	decoder = eio.NewGobCodec()
)

type Test struct {
	Name string
	Age  int
}

func gobEncodeMessage() eio.Message {
	encode, _ := encoder.Encode(Test{
		Name: "QMstar",
		Age:  20,
	})
	return encode
}

func gobDecodeMessage(msg eio.Message) (*Test, error) {
	var t Test
	err := decoder.Decode(msg, &t)
	if err != nil {
		return nil, err
	}
	return &t, err
}

func TestNewRedisPublisherAndRedisSubscriber(t *testing.T) {

	redisCli := redis.NewClient(&redis.Options{
		Addr:     "192.168.1.10:6379", //Testing in LAN environment
		Password: "",
		DB:       0,
	})

	var (
		ctx, cc = context.WithTimeout(context.Background(), time.Second*5)
	)
	defer cc()

	pub := pubsub.NewRedisPublisher(redisCli)
	defer pub.Close()
	sub := pubsub.NewRedisSubscriber(redisCli)
	defer sub.Close()

	msgCh, err := sub.Subscribe(ctx, "test")
	if err != nil {
		t.Fatal("err on subscriber", err)
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				err := pub.Publish(ctx, "test", gobEncodeMessage())
				if err != nil {
					t.Error("err on publish", err)
				}
				time.Sleep(time.Millisecond * 200)
			}
		}
	}()

	for message := range msgCh {
		decodeData, err := gobDecodeMessage(message)
		if err != nil {
			t.Error(err)
		}
		t.Log(message, decodeData)
	}

}
