package tasker

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/samber/lo"
	kafka "github.com/segmentio/kafka-go"
)

func Test_Tasker(t *testing.T) {
	var zLogger = GetLogger()
	zerolog.DefaultContextLogger = &zLogger

	cleanupFunc, port, err := InitKafkaForTesting()
	if err != nil {
		t.Fatalf("failed to start kafka: %s", err)
	}
	defer cleanupFunc()

	topic := "topic-1"
	err = CreateTopic(port, topic)
	if err != nil {
		t.Fatalf("failed to create topic: %s", err)
	}
	// Verify topic getting created using /opt/bitnami/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9093

	ctx := context.Background()
	brokers := "localhost:" + port
	writer := InitKafkaWriter(ctx, brokers, topic)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  strings.Split(brokers, ","),
		GroupID:  "cid-1",
		Topic:    topic,
		MaxBytes: 10e6, // 10MB
	})

	tasks := []Task{{Id: "g1-1"}, {Id: "g1-2"}, {Id: "g2-1"}, {Id: "g3-1"}, {Id: "g4-1"}, {Id: "g4-2"}, {Id: "g3-3"}, {Id: "g3-2"}}
	kMsgs := lo.Map(tasks, func(item Task, index int) kafka.Message {
		_bytes, _ := json.Marshal(item)
		return kafka.Message{
			Key:   []byte(strings.Split(item.Id, "-")[0]),
			Value: _bytes,
		}
	})

	err = writer.WriteMessages(ctx, kMsgs...)
	if err != nil {
		t.Fatalf("failed to create topic: %s", err)
	}

	ex := Executor{}
	tasker, _ := NewKafkaMultiTasker(ctx, reader, &SimpleCommitter{Reader: reader}, partFn, ex.processFn, errorFn, 10)
	go tasker.Start(ctx)

	for {
		time.Sleep(1 * time.Second)
		if len(tasks) == len(ex.processed) {
			break
		}
	}
}

type Task struct {
	Id string `json:"id"`
}

func partFn(ctx context.Context, kMsg kafka.Message) (string, string, any) {
	taskDetails := map[string]string{}
	json.Unmarshal(kMsg.Value, &taskDetails)
	taskId := taskDetails["id"]
	return taskId, string(kMsg.Key), taskDetails
}

type Executor struct {
	processed []kafka.Message
}

func (ex *Executor) processFn(ctx context.Context, kMsg kafka.Message) (bool, error) {
	logger := zerolog.Ctx(ctx)
	noopFn(logger)

	taskDetails := map[string]string{}
	json.Unmarshal(kMsg.Value, &taskDetails)

	ex.processed = append(ex.processed, kMsg)
	return true, nil
}

func errorFn(ctx context.Context, kMsg kafka.Message) error {
	noopFn()
	return nil
}
