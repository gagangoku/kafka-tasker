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

func Test_TaskerNoFailures(t *testing.T) {
	ctx, reader, writer, cleanupFunc := setupKafka(t)
	defer cleanupFunc()

	tasks := []Task{{Id: "g1-1"}, {Id: "g1-2"}, {Id: "g2-1"}, {Id: "g3-1"}, {Id: "g4-1"}, {Id: "g4-2"}, {Id: "g3-3"}, {Id: "g3-2"}}
	kMsgs := lo.Map(tasks, func(item Task, index int) kafka.Message {
		_bytes, _ := json.Marshal(item)
		return kafka.Message{
			Key:   []byte(strings.Split(item.Id, "-")[0]),
			Value: _bytes,
		}
	})

	err := writer.WriteMessages(ctx, kMsgs...)
	if err != nil {
		t.Fatalf("failed to create topic: %s", err)
	}

	ex := TestExecutor{}
	ex.Init()
	config := MultiTaskerConfig{
		reader:        reader,
		partFn:        partFn,
		processFn:     ex.processFn,
		errorFn:       ex.errorFn,
		nTasksPerPart: 10,
	}
	tasker, _ := NewKafkaMultiTasker(ctx, config)
	go tasker.Start(ctx)

	for {
		time.Sleep(1 * time.Second)

		if len(tasks) == len(ex.processed) {
			break
		}
	}

	for _, aList := range ex.taskAttempts {
		for _, att := range aList {
			if att.Status != TASK_STATUS_SUCCESS {
				t.Fatalf("expected all task successes")
			}
		}
	}
	if len(ex.errored) != 0 {
		t.Fatalf("should not have received any errors")
	}
}

func Test_TaskerWithFailures(t *testing.T) {
	ctx, reader, writer, cleanupFunc := setupKafka(t)
	defer cleanupFunc()

	tasks := []Task{{Id: "g1-1"}, {Id: "g1-2"}, {Id: "g2-1"}, {Id: "g3-1"}, {Id: "g4-1"}, {Id: "g4-2"}, {Id: "g3-3"}, {Id: "g3-2"}}
	kMsgs := lo.Map(tasks, func(item Task, index int) kafka.Message {
		_bytes, _ := json.Marshal(item)
		partId := strings.Split(item.Id, "-")[0]
		return kafka.Message{
			Key:   []byte(partId),
			Value: _bytes,
		}
	})

	err := writer.WriteMessages(ctx, kMsgs...)
	if err != nil {
		t.Fatalf("failed to create topic: %s", err)
	}

	ex := TestExecutor{tasksToFailAlways: []string{"g3-1"}, tasksToFailOnce: []string{"g4-1"}}
	ex.Init()
	config := MultiTaskerConfig{
		reader:        reader,
		partFn:        partFn,
		processFn:     ex.processWithRetryFn,
		errorFn:       ex.errorFn,
		nTasksPerPart: 10,
	}
	tasker, _ := NewKafkaMultiTasker(ctx, config)
	go tasker.Start(ctx)

	for {
		time.Sleep(1 * time.Second)

		if len(tasks) == len(ex.taskAttempts) {
			x1 := ex.taskAttempts["g4-1"]
			y1 := ex.taskAttempts["g3-1"]
			noopFn(x1, y1)
			if len(x1) == 2 && x1[1].Status == TASK_STATUS_SUCCESS && len(y1) == 3 && y1[2].Status == TASK_STATUS_CANCELLED {
				break
			}
		}
	}

	if len(ex.errored) != 1 {
		t.Fatalf("expected only 1 task that errored")
	}
}

func setupKafka(t *testing.T) (context.Context, *kafka.Reader, *kafka.Writer, CleanupFunc) {
	var zLogger = GetLogger()
	zerolog.DefaultContextLogger = &zLogger

	cleanupFunc, port, err := InitKafkaForTesting()
	if err != nil {
		t.Fatalf("failed to start kafka: %s", err)
	}

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
	return ctx, reader, writer, cleanupFunc
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

type TestExecutor struct {
	tasksToFailOnce   []string
	tasksToFailAlways []string
	numTaskAttemps    map[string]int
	taskAttempts      map[string][]TaskAttempt
	processed         []kafka.Message
	errored           []kafka.Message
}

func (ex *TestExecutor) Init() {
	ex.numTaskAttemps = map[string]int{}
	ex.taskAttempts = map[string][]TaskAttempt{}
}

func (ex *TestExecutor) processWithRetryFn(ctx context.Context, kMsg kafka.Message) (bool, error) {
	err := ProcessFnWithRetryPersistence(ctx, kMsg, partFn, ex, ex, true, 3)
	return err == nil, err
}

func (ex *TestExecutor) FindPrevAttempts(ctx context.Context, taskId string) ([]TaskAttempt, error) {
	tasks := ex.taskAttempts[taskId]
	return tasks, nil
}

func (ex *TestExecutor) CreateNewAttempt(ctx context.Context, att TaskAttempt) error {
	ex.taskAttempts[att.TaskId] = append(ex.taskAttempts[att.TaskId], att)
	return nil
}

func (ex *TestExecutor) UpdateAttemptStatus(ctx context.Context, att TaskAttempt, status TaskStatus, msg string) error {
	tasks := ex.taskAttempts[att.TaskId]
	for idx, t := range tasks {
		if t.AttemptId == att.AttemptId && t.TimeMs == att.TimeMs {
			att.Status = status
			att.Msg = msg
			tasks[idx] = att
			break
		}
	}
	ex.taskAttempts[att.TaskId] = tasks
	return nil
}

func (ex *TestExecutor) Execute(ctx context.Context, taskId string, attemptId int, prevAttempts []TaskAttempt, taskDetails any) (bool, string, error) {
	_bytes, _ := json.Marshal(taskDetails)
	success, err := ex.processFn(ctx, kafka.Message{Value: _bytes})
	return success, "", err
}

func (ex *TestExecutor) processFn(ctx context.Context, kMsg kafka.Message) (bool, error) {
	taskDetails := map[string]string{}
	json.Unmarshal(kMsg.Value, &taskDetails)

	taskId := taskDetails["id"]
	ex.numTaskAttemps[taskId] += 1

	if lo.Contains(ex.tasksToFailAlways, taskId) {
		return false, nil
	}
	if lo.Contains(ex.tasksToFailOnce, taskId) && ex.numTaskAttemps[taskId] == 1 {
		return false, nil
	}

	ex.processed = append(ex.processed, kMsg)
	return true, nil
}

func (ex *TestExecutor) errorFn(ctx context.Context, kMsg kafka.Message) error {
	ex.errored = append(ex.errored, kMsg)
	return nil
}
