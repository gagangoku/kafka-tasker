package tasker

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/samber/lo"
	kafka "github.com/segmentio/kafka-go"
)

// This is a simple distributed in-memory tasker built on Kafka. It listens to tasks from Kafka, and shards the task
// using a user defined partition function to enable multiple tasks with different partId's to be run concurrently.
//
// Configuration:
// - reader: reader to read from kafka
// - partFn: function to compute the partId for a task
// - processFn: function to actually process the task
// - errorHandler: error function to handle failed tasks (after retries)
// - numTasksPerPart: the number of tasks to queue per partId. If more tasks are received, the publisher get blocked
//
// ProcessFnWithPersistence is an implementation of processFn which tracks task attempts (preferably in a DB).
//
// Constraints:
// - tasks with different partId can be executed in parallel
// - only 1 task with the same partId can run at a time
// - tasks with same partId will be executed in order in which they were inserted into Kafka (offset based)
// - processFn should be idempotent. It can be called multiple times in case there is an error saving task attempt details
//
// Flow:
// 1. read a message from kafka topic into memory
// 2. compute its partId, and push to the corresponding channel
// 3. each channel runs an even loop, where it reads a message and processes it
// 4. if processing was successful, store this success in memory
// 5. once all messages prior to this message are committed, commit to kafka everything upto and including the current message
//
// See tasker_test.go for usage

func NewKafkaMultiTasker(ctx context.Context, config MultiTaskerConfig) (KafkaMultiTasker, error) {
	if config.NumTasksPerPart < 0 {
		return KafkaMultiTasker{}, fmt.Errorf("nTasksPerPart must be >= 0")
	}

	obj := KafkaMultiTasker{
		reader:        config.Reader,
		committer:     &SimpleCommitter{Reader: config.Reader},
		partFn:        config.PartFn,
		processFn:     config.ProcessFn,
		errorHandler:  config.ErrorHandler,
		messages:      nil,
		channelMap:    map[string]chan kafka.Message{},
		committedMsgs: map[int64]struct{}{},
		nTasksPerPart: config.NumTasksPerPart,
		messagesMutex: &sync.RWMutex{},
		commitMutex:   &sync.Mutex{},
	}
	return obj, nil
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
		logger.Info().Msgf("received new message: offset, partId, key: %d %s %s", kMsg.Offset, partId, kMsg.Key)
		logger.Debug().Msgf("[DEBUG] received message: %s %s", kMsg.Key, kMsg.Value)

		c.messagesMutex.Lock()
		c.messages = append(c.messages, kMsg)
		if _, exists := c.channelMap[partId]; !exists {
			// create a new channel
			newChannel := make(chan kafka.Message, c.nTasksPerPart)
			c.channelMap[partId] = newChannel
			go func(arg1 string, arg2 chan kafka.Message) {
				c.eventLoop(ctx, arg1, arg2)
			}(partId, newChannel)
		}
		channel := c.channelMap[partId]
		c.messagesMutex.Unlock()

		channel <- kMsg
	}

	if err := c.reader.Close(); err != nil {
		logger.Fatal().Msgf("failed to close reader: %s", err)
	}
}

func (c *KafkaMultiTasker) eventLoop(ctx context.Context, partId string, channel chan kafka.Message) error {
	for {
		c.processWithRetry(ctx, partId, channel)
	}
}

func (c *KafkaMultiTasker) processWithRetry(ctx context.Context, partId string, channel chan kafka.Message) {
	logger := zerolog.Ctx(ctx)
	defer func() {
		if err := recover(); err != nil {
			logger.Error().Msgf("caught panic: %s %s", err, debug.Stack())
		}
	}()

	// Read a task
	kMsg := <-channel

	// Process the task
	logger.Info().Msgf("processing task: partId, key: %s %s", partId, string(kMsg.Key))
	logger.Debug().Msgf("task details: partId, key, value: %s %s %s", partId, kMsg.Key, kMsg.Value)
	success, err := c.processFn(ctx, kMsg) // should be idempotent

	var ehErr error = nil
	if success {
		logger.Info().Msgf("[SUCCESS] task: %s %s", partId, kMsg.Key)
	} else {
		logger.Warn().Msgf("[FAIL] task: %s %s %s", partId, kMsg.Key, err)

		// Call the error handler provided
		ehErr = c.errorHandler(ctx, kMsg)
		if ehErr != nil {
			logger.Error().Msgf("error in errorFn: %s", ehErr)
		}
	}

	// Commit task failures as well, unless error handler also failed
	if ehErr == nil {
		c.commitMutex.Lock()
		c.commitIfPossible(ctx, kMsg.Offset)
		c.commitMutex.Unlock()
	}
}

func (c *KafkaMultiTasker) commitIfPossible(ctx context.Context, offset int64) {
	logger := zerolog.Ctx(ctx)

	c.messagesMutex.RLock()
	c.committedMsgs[offset] = struct{}{}
	lastSuccessfulIdx := -1
	for idx, o := range c.messages {
		if _, exists := c.committedMsgs[o.Offset]; !exists {
			break
		} else {
			lastSuccessfulIdx = idx
		}
	}
	c.messagesMutex.RUnlock()

	if lastSuccessfulIdx >= 0 {
		c.messagesMutex.RLock()
		lastSuccessfulMsg := c.messages[lastSuccessfulIdx]
		msgsToCommit := c.messages[0 : lastSuccessfulIdx+1]
		c.messagesMutex.RUnlock()

		logger.Info().Msgf("[COMM] committing upto: %d %d %d", offset, lastSuccessfulIdx, lastSuccessfulMsg.Offset)
		err := c.committer.CommitMessages(ctx, msgsToCommit...)
		if err != nil {
			logger.Error().Msgf("[COMM] commit error: %d %d %s", offset, lastSuccessfulIdx, err)
			return
		}
		logger.Info().Msgf("[COMM] commit done: %d %d %d", offset, lastSuccessfulIdx, lastSuccessfulMsg.Offset)

		c.messagesMutex.Lock()
		c.messages = c.messages[lastSuccessfulIdx+1:]
		c.messagesMutex.Unlock()
	}
}

func ProcessFnWithRetryPersistence(ctx context.Context, kMsg kafka.Message, partFn TaskIdPartIdTaskDetailsFn, dao AttemptInf, executor ExecutionInf, retryUnknown bool, maxTries int) error {
	logger := zerolog.Ctx(ctx)
	logger.Debug().Msgf("[DEBUG] ProcessFnWithRetryPersistence: %s", kMsg.Value)

	var done bool
	var status TaskStatus
	var err error
	for tryId := 0; tryId < maxTries+1; tryId++ {
		done, status, err = processIteration(ctx, tryId, kMsg, partFn, dao, executor, retryUnknown, maxTries)
		if done {
			break
		}
	}
	if status != TASK_STATUS_SUCCESS {
		return fmt.Errorf("task unsuccessful: %s", err)
	}
	return nil
}

func processIteration(ctx context.Context, tryId int, kMsg kafka.Message, partFn TaskIdPartIdTaskDetailsFn, dao AttemptInf, executor ExecutionInf, retryUnknown bool, maxTries int) (bool, TaskStatus, error) {
	taskId, _, taskDetails := partFn(ctx, kMsg)

	attempts, err := dao.FindPrevAttempts(ctx, taskId)
	if err != nil {
		return false, TASK_STATUS_PRE, WrapError(err, "error in FindAttempts")
	}

	if len(attempts) == 0 {
		return makeNewAttempt(ctx, tryId, taskDetails, taskId, attempts, dao, executor)
	} else {
		lastAttempt := attempts[len(attempts)-1]

		switch lastAttempt.Status {
		case TASK_STATUS_SUCCESS:
			return true, TASK_STATUS_SUCCESS, nil

		case TASK_STATUS_CANCELLED:
			return true, TASK_STATUS_CANCELLED, fmt.Errorf("task cancelled")

		case TASK_STATUS_FAILURE:
			numTries := len(lo.Filter(attempts, func(item TaskAttempt, index int) bool { return item.Status == TASK_STATUS_FAILURE }))
			failedEnough := numTries >= maxTries

			if failedEnough {
				err := dao.UpdateAttemptStatus(ctx, lastAttempt, TASK_STATUS_CANCELLED, "")
				return true, TASK_STATUS_CANCELLED, WrapError(err, "error in UpdateAttemptStatus")
			} else {
				return makeNewAttempt(ctx, tryId, taskDetails, taskId, attempts, dao, executor)
			}

		case TASK_STATUS_ATTEMPT:
			// We dont know if the previous attempt was a success or failure
			if retryUnknown {
				return makeNewAttempt(ctx, tryId, taskDetails, taskId, attempts, dao, executor)
			} else {
				// dont retry
				err := dao.UpdateAttemptStatus(ctx, lastAttempt, TASK_STATUS_CANCELLED, "")
				return true, TASK_STATUS_CANCELLED, WrapError(err, "error in UpdateAttemptStatus")
			}

		default:
			return true, TASK_STATUS_PRE, fmt.Errorf("bad task status: %v", lastAttempt.Status)
		}
	}
}

func makeNewAttempt(ctx context.Context, tryId int, taskDetails any, taskId string, attempts []TaskAttempt, dao AttemptInf, executor ExecutionInf) (bool, TaskStatus, error) {
	logger := zerolog.Ctx(ctx)

	newAtt := TaskAttempt{
		TaskId:    taskId,
		AttemptId: len(attempts) + 1,
		TimeMs:    time.Now().UnixMilli(),
	}
	newAtt.Status = TASK_STATUS_ATTEMPT
	err := dao.CreateNewAttempt(ctx, newAtt)
	if err != nil {
		return false, TASK_STATUS_PRE, WrapError(err, "error in CreateNewAttempt")
	}

	logger.Info().Msgf("task attempt: %s %d", taskId, tryId)
	success, output, err := executor.Execute(ctx, taskId, newAtt.AttemptId, attempts, taskDetails)

	if success {
		logger.Info().Msgf("[SUCCESS] task attempt: partId, key: %s", taskId)
		err := dao.UpdateAttemptStatus(ctx, newAtt, TASK_STATUS_SUCCESS, output)
		return true, TASK_STATUS_SUCCESS, WrapError(err, "error in UpdateAttemptStatus")
	} else {
		errMsg := ""
		if err != nil {
			errMsg = err.Error()
		}
		logger.Warn().Msgf("[FAILURE] task attempt: partId, key: %s", taskId)
		err := dao.UpdateAttemptStatus(ctx, newAtt, TASK_STATUS_FAILURE, errMsg)
		return false, TASK_STATUS_FAILURE, WrapError(err, "error in UpdateAttemptStatus")
	}
}
