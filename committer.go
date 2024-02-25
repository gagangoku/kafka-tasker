package tasker

import (
	"context"

	"github.com/rs/zerolog"
	kafka "github.com/segmentio/kafka-go"
)

type CommitterInf interface {
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
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
	// does nothing
	return nil
}
