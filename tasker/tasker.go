package tasker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/samber/lo"
	kafka "github.com/segmentio/kafka-go"
)

// This is a simple distributed in-memory tasker built on Kafka. It listens to tasks from Kafka, and shards the task using PartitionFn
// to enable multiple tasks with different partId's to be run concurrently.

// Configs:
// - nTasksPerPart: the number of tasks to queue per partId. If more tasks
// - partFn: function to compute the partId for a task
// - processFn: function to actually process the task
// - errorFn: error function to handle failed tasks (after retries)

// Constraints:
// - tasks with different partId can be executed in parallel
// - only 1 task with the same partId must be running at a time
// - tasks with same partId will be executed in order in which they were inserted into Kafka (offset based)

// Flow:
// 1. read a message from kafka topic into memory
// 2. compute its partId, and push to the corresponding channel
// 3. each channel runs an even loop, where it reads a message and processes it
// 4. if processing was successful, store this success in memory
// 5. once all messages prior to this message are committed, commit to kafka everything upto and including the current message
//
// Usage:
// tasker := NewKafkaMultiTasker()
// go tasker.Start()

type CommitterInf interface {
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}

type TaskIdPartIdTaskDetailsFn func(ctx context.Context, kMsg kafka.Message) (string, string, any)
type ProcessFn func(ctx context.Context, kMsg kafka.Message) (bool, error)
type ErrorFn func(ctx context.Context, kMsg kafka.Message) error

type KafkaMultiTasker struct {
	reader        *kafka.Reader
	committer     CommitterInf
	partFn        TaskIdPartIdTaskDetailsFn
	processFn     ProcessFn
	errorFn       ErrorFn
	messages      []kafka.Message
	channelMap    map[string]chan kafka.Message
	committedMsgs map[int64]struct{}
	nTasksPerPart int // buffer size of the channel
	mutex         *sync.Mutex
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
	TASK_STATUS_ATTEMPT   TaskStatus = 1
	TASK_STATUS_SUCCESS   TaskStatus = 2
	TASK_STATUS_FAILURE   TaskStatus = 3
	TASK_STATUS_CANCELLED TaskStatus = 4
)

func NewKafkaMultiTasker(ctx context.Context, reader *kafka.Reader, committer CommitterInf, partFn TaskIdPartIdTaskDetailsFn, processFn ProcessFn, errorFn ErrorFn, nTasksPerPart int) (KafkaMultiTasker, error) {
	if nTasksPerPart < 0 {
		return KafkaMultiTasker{}, fmt.Errorf("nTasksPerPart must be >= 0")
	}

	obj := KafkaMultiTasker{
		reader:        reader,
		committer:     committer,
		partFn:        partFn,
		processFn:     processFn,
		errorFn:       errorFn,
		messages:      nil,
		channelMap:    map[string]chan kafka.Message{},
		committedMsgs: map[int64]struct{}{},
		nTasksPerPart: nTasksPerPart,
		mutex:         &sync.Mutex{},
	}
	return obj, nil
}

func (c *KafkaMultiTasker) EventLoop(ctx context.Context, partId string, channel chan kafka.Message) error {
	for {
		c.ProcessWithRetry(ctx, partId, channel)
	}
}

func (c *KafkaMultiTasker) ProcessWithRetry(ctx context.Context, partId string, channel chan kafka.Message) {
	logger := zerolog.Ctx(ctx)
	defer func() {
		if err := recover(); err != nil {
			logger.Error().Msgf("caught panic: %s", err)
		}
	}()

	kMsg := <-channel
	err := c.ProcessMsg(ctx, partId, kMsg)
	if err != nil {
		err := c.errorFn(ctx, kMsg)
		logger.Error().Msgf("error in errorFn: %s", err) // cant do much about this now
	}
}

func (c *KafkaMultiTasker) ProcessMsg(ctx context.Context, partId string, kMsg kafka.Message) error {
	logger := zerolog.Ctx(ctx)

	logger.Info().Msgf("processing message: partId, key: %s %s", partId, string(kMsg.Key))
	logger.Debug().Msgf("message details: partId, key, value: %s %s %s", partId, kMsg.Key, kMsg.Value)
	success, err := c.processFn(ctx, kMsg) // should be idempotent
	logger.Info().Msgf("message status: partId, key, success: %s %s %v", partId, string(kMsg.Key), success)

	if success {
		c.CommitIfPossible(ctx, kMsg.Offset)
		return nil
	}
	return err
}

func (c *KafkaMultiTasker) CommitIfPossible(ctx context.Context, offset int64) {
	logger := zerolog.Ctx(ctx)

	c.mutex.Lock()
	c.committedMsgs[offset] = struct{}{}
	lastIdx := -1
	for idx, o := range c.messages {
		lastIdx = idx
		if _, exists := c.committedMsgs[o.Offset]; !exists {
			break
		}
	}

	if lastIdx >= 0 {
		logger.Info().Msgf("committing upto: %d %d", offset, lastIdx)
		err := c.committer.CommitMessages(ctx, c.messages[0:lastIdx]...)
		if err != nil {
			logger.Error().Msgf("commit error: %d %d %s", offset, lastIdx, err)
		}
		logger.Info().Msgf("commit done: %d %d", offset, lastIdx)

		c.messages = c.messages[lastIdx:]
	}
	c.mutex.Unlock()
}

func (c *KafkaMultiTasker) Start(ctx context.Context) {
	logger := zerolog.Ctx(ctx)

	for {
		kMsg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			logger.Error().Msgf("ERROR fetching next kafka message, stopping the consumer: %s", err)
			break
		}
		_, partId, _ := c.partFn(ctx, kMsg)
		logger.Info().Msgf("received new message: offset, partId, key: %d %s %s", kMsg.Offset, partId, string(kMsg.Key))
		logger.Debug().Msgf("[DEBUG] received message: %s", kMsg.Value)

		c.mutex.Lock()
		c.messages = append(c.messages, kMsg)
		if _, exists := c.channelMap[partId]; !exists {
			// create a new channel
			newChannel := make(chan kafka.Message, c.nTasksPerPart)
			c.channelMap[partId] = newChannel
			go func(partId string, channel chan kafka.Message) {
				c.EventLoop(ctx, partId, newChannel)
			}(partId, newChannel)
		}
		channel := c.channelMap[partId]
		c.mutex.Unlock()

		channel <- kMsg
	}

	if err := c.reader.Close(); err != nil {
		logger.Fatal().Msgf("failed to close reader: %s", err)
	}
}

type AttemptInf interface {
	FindPrevAttempts(ctx context.Context, taskId string) ([]TaskAttempt, error)
	CreateNewAttempt(ctx context.Context, att TaskAttempt) error
	UpdateAttemptStatus(ctx context.Context, att TaskAttempt, status TaskStatus, msg string) error
}
type ExecutionInf interface {
	Execute(ctx context.Context, taskId string, attemptId int, prevAttempts []TaskAttempt, taskDetails any) (bool, string, error)
}

func ProcessFnWithPersistence(ctx context.Context, kMsg kafka.Message, partFn TaskIdPartIdTaskDetailsFn, dao AttemptInf, executor ExecutionInf, retryUnknown bool, maxTries int) error {
	logger := zerolog.Ctx(ctx)
	logger.Debug().Msgf("[DEBUG] ProcessFnWithPersistence: %s", kMsg.Value)

	var done bool
	var err error
	for tryId := 0; tryId < maxTries; tryId++ {
		done, err = ProcessIteration(ctx, kMsg, partFn, dao, executor, retryUnknown, maxTries)
		if done && err == nil {
			return nil
		}
	}
	return err
}

func ProcessIteration(ctx context.Context, kMsg kafka.Message, partFn TaskIdPartIdTaskDetailsFn, dao AttemptInf, executor ExecutionInf, retryUnknown bool, maxTries int) (bool, error) {
	taskId, _, taskDetails := partFn(ctx, kMsg)

	attempts, err := dao.FindPrevAttempts(ctx, taskId)
	if err != nil {
		return false, WrapError(err, "error in FindAttempts")
	}

	newAtt := TaskAttempt{
		TaskId:    taskId,
		AttemptId: len(attempts) + 1,
		TimeMs:    time.Now().UnixMilli(),
	}

	makeNewAttemptFn := func() (bool, error) {
		newAtt.Status = TASK_STATUS_ATTEMPT
		err := dao.CreateNewAttempt(ctx, newAtt)
		if err != nil {
			return false, WrapError(err, "error in CreateNewAttempt")
		}

		success, output, err := executor.Execute(ctx, taskId, newAtt.AttemptId, attempts, taskDetails)
		if success {
			err := dao.UpdateAttemptStatus(ctx, newAtt, TASK_STATUS_SUCCESS, output)
			return true, WrapError(err, "error in UpdateAttemptStatus")
		} else {
			err := dao.UpdateAttemptStatus(ctx, newAtt, TASK_STATUS_FAILURE, err.Error())
			return false, WrapError(err, "error in UpdateAttemptStatus")
		}
	}

	if len(attempts) == 0 {
		return makeNewAttemptFn()
	} else {
		lastAttempt := attempts[len(attempts)-1]

		switch lastAttempt.Status {
		case TASK_STATUS_SUCCESS:
			return true, nil

		case TASK_STATUS_CANCELLED:
			return true, nil

		case TASK_STATUS_FAILURE:
			numTries := len(lo.Filter(attempts, func(item TaskAttempt, index int) bool { return item.Status == TASK_STATUS_FAILURE }))
			failedEnough := numTries >= maxTries

			if failedEnough {
				return true, WrapError(dao.UpdateAttemptStatus(ctx, newAtt, TASK_STATUS_CANCELLED, ""), "error in UpdateAttemptStatus")
			} else {
				return makeNewAttemptFn()
			}

		case TASK_STATUS_ATTEMPT:
			// We dont know if the previous attempt was a success or failure
			if retryUnknown {
				return makeNewAttemptFn()
			} else {
				// dont retry
				return true, WrapError(dao.UpdateAttemptStatus(ctx, newAtt, TASK_STATUS_CANCELLED, ""), "error in UpdateAttemptStatus")
			}

		default:
			return false, fmt.Errorf("bad task status: %v", lastAttempt.Status)
		}
	}
}

func WrapError(err error, wrap string) error {
	if err != nil {
		return fmt.Errorf("%s: %s", wrap, err)
	}
	return nil
}

type SimpleCommitter struct {
	Reader *kafka.Reader
}

func (c *SimpleCommitter) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	logger := zerolog.Ctx(ctx)

	err := c.Reader.CommitMessages(ctx, msgs...)
	if err != nil {
		logger.Error().Msgf("[COMMIT] failed to commit %d messages: %s", len(msgs), err)
		return err
	} else {
		logger.Info().Msgf("[COMMIT] committed %d messages", len(msgs))
	}
	return nil
}

type NoopCommitter struct {
}

func (c *NoopCommitter) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	noopFn("does nothing")
	return nil
}

func noopFn(x ...any) {
	// Does nothing
}
