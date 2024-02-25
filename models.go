package tasker

import (
	"context"
	"sync"

	kafka "github.com/segmentio/kafka-go"
)

type MultiTaskerConfig struct {
	reader        *kafka.Reader
	partFn        TaskIdPartIdTaskDetailsFn
	processFn     ProcessFn
	errorFn       ErrorFn
	nTasksPerPart int
}

type TaskIdPartIdTaskDetailsFn func(ctx context.Context, kMsg kafka.Message) (string, string, any)
type ProcessFn func(ctx context.Context, kMsg kafka.Message) (bool, error)
type ErrorFn func(ctx context.Context, kMsg kafka.Message) error

type KafkaMultiTasker struct {
	reader        *kafka.Reader
	committer     CommitterInf
	partFn        TaskIdPartIdTaskDetailsFn
	processFn     ProcessFn
	errorHandler  ErrorFn
	messages      []kafka.Message
	channelMap    map[string]chan kafka.Message
	committedMsgs map[int64]struct{}
	nTasksPerPart int // buffer size of the channel
	messagesMutex *sync.RWMutex
	commitMutex   *sync.Mutex
}

type TaskAttempt struct {
	TaskId    string     `json:"taskId,omitempty"`
	AttemptId int        `json:"attemptId,omitempty"`
	TimeMs    int64      `json:"timeMs,omitempty"`
	Status    TaskStatus `json:"status,omitempty"`
	Msg       string     `json:"msg,omitempty"`
}

type TaskStatus int

const (
	TASK_STATUS_PRE       TaskStatus = 1
	TASK_STATUS_ATTEMPT   TaskStatus = 2
	TASK_STATUS_SUCCESS   TaskStatus = 3
	TASK_STATUS_FAILURE   TaskStatus = 4
	TASK_STATUS_CANCELLED TaskStatus = 5
)

type AttemptInf interface {
	FindPrevAttempts(ctx context.Context, taskId string) ([]TaskAttempt, error)
	CreateNewAttempt(ctx context.Context, att TaskAttempt) error
	UpdateAttemptStatus(ctx context.Context, att TaskAttempt, status TaskStatus, msg string) error
}
type ExecutionInf interface {
	Execute(ctx context.Context, taskId string, attemptId int, prevAttempts []TaskAttempt, taskDetails any) (bool, string, error)
}
