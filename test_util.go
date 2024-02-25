package tasker

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress"
	"github.com/testcontainers/testcontainers-go"
)

func InitKafkaWriter(ctx context.Context, kafkaBrokers, kafkaTopic string) *kafka.Writer {
	logger := zerolog.Ctx(ctx)

	logger.Info().Msgf("kafka config: brokers: %s, topic: %s", kafkaBrokers, kafkaTopic)

	// intialize the writer with the broker addresses, and the topic
	kafkaWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers: strings.Split(kafkaBrokers, ","),
		Topic:   kafkaTopic,

		// dont wait for more than 10 messages before writing
		BatchSize: 10,

		// maximum size of a request in bytes before being sent to a partition - 100KB
		BatchBytes: 102400,

		// no matter what happens, write all pending messages every 5 seconds
		BatchTimeout: 3 * time.Second,

		// 1 means lead kafka broker should acknowledge. In prod, try setting it as 2
		RequiredAcks: 1,

		Logger: log.New(os.Stdout, "kafka writer: ", 0),

		// ZstdCodec codec is the best
		// https://developer.ibm.com/articles/benefits-compression-kafka-messaging/
		// https://blog.cloudflare.com/squeezing-the-firehose/
		CompressionCodec: &compress.ZstdCodec,
	})
	logger.Info().Msgf("kafka writer created")
	return kafkaWriter
}

type CleanupFunc func()

// Inspired by https://franklinlindemberg.medium.com/how-to-use-kafka-with-testcontainers-in-golang-applications-9266c738c879
// Ingenious solution to start the kafka container with a custom cmd, read the exposed port, setup kafka using the exposed port, and then start kafka
func InitKafkaForTesting() (CleanupFunc, string, error) {
	ctx := context.Background()

	// Bring up kafka
	KAFKA_IMAGE := "docker.io/bitnami/kafka:3.4"
	kafkaContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        KAFKA_IMAGE,
			ExposedPorts: []string{"9093"},
			Env: map[string]string{
				"KAFKA_ENABLE_KRAFT":                       "yes",
				"KAFKA_CFG_PROCESS_ROLES":                  "broker,controller",
				"KAFKA_CFG_CONTROLLER_LISTENER_NAMES":      "CONTROLLER",
				"KAFKA_CFG_LISTENERS":                      "CLIENT://:9092,EXTERNAL://:9093,CONTROLLER://:9094",
				"KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP": "CONTROLLER:PLAINTEXT,CLIENT:PLAINTEXT,EXTERNAL:PLAINTEXT",
				"KAFKA_CFG_ADVERTISED_LISTENERS":           "CLIENT://kafka:9092,EXTERNAL://localhost:9093",
				"KAFKA_CFG_INTER_BROKER_LISTENER_NAME":     "CLIENT",
				"KAFKA_BROKER_ID":                          "1",
				"KAFKA_CFG_NODE_ID":                        "1",
				"KAFKA_CFG_CONTROLLER_QUORUM_VOTERS":       "1@127.0.0.1:9094",
				"ALLOW_PLAINTEXT_LISTENER":                 "yes",
				"KAFKA_JMX_PORT":                           "9997",
				"KAFKA_JMX_OPTS":                           "-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.rmi.server.hostname=kafka -Dcom.sun.management.jmxremote.rmi.port=9997",
			},
			// WaitingFor: wait.ForLog("Kafka Server started"),
			Cmd: []string{"sh", "-c", "while [ ! -f /testcontainers_start.sh ]; do sleep 0.1; done; /testcontainers_start.sh"},
		},
		Started: true,
	})

	if err != nil {
		return nil, "", err
	}

	p2, _ := kafkaContainer.MappedPort(ctx, "9093")
	fmt.Println("kafka external port: ", p2)
	port := strings.Split(string(p2), "/")[0]

	kafkaStartFile, err := ioutil.TempFile("", "testcontainers_start.sh")
	if err != nil {
		return nil, "", err
	}
	defer os.Remove(kafkaStartFile.Name())

	// needs to set KAFKA_ADVERTISED_LISTENERS with the exposed kafka port
	kafkaStartFile.WriteString("#!/bin/bash \n")
	kafkaStartFile.WriteString("export KAFKA_ADVERTISED_LISTENERS='CLIENT://kafka:9092,EXTERNAL://localhost:" + port + "'\n")
	kafkaStartFile.WriteString("/opt/bitnami/scripts/kafka/setup.sh\n")
	kafkaStartFile.WriteString("/opt/bitnami/scripts/kafka/run.sh\n")

	err = kafkaContainer.CopyFileToContainer(ctx, kafkaStartFile.Name(), "testcontainers_start.sh", 0777)
	if err != nil {
		return nil, "", err
	}

	// Give the script time to run
	time.Sleep(1 * time.Second)

	logConsumer := LogConsumer{}
	kafkaContainer.FollowOutput(&logConsumer)
	err = kafkaContainer.StartLogProducer(ctx)
	if err != nil {
		return nil, "", err
	}

	for {
		if logConsumer.end {
			kafkaContainer.StopLogProducer()
			break
		}
		time.Sleep(1 * time.Millisecond)
	}

	cleanupFunc := func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			fmt.Printf("failed to terminate container: %s", err)
		}
	}

	return cleanupFunc, port, nil
}

type LogConsumer struct {
	lines []string
	end   bool
}

// Accept prints the log to stdout
func (lc *LogConsumer) Accept(l testcontainers.Log) {
	str := string(l.Content)
	lc.lines = append(lc.lines, str)
	lc.end = strings.Contains(str, "Kafka Server started")
}

func GetLogger() zerolog.Logger {
	zerolog.CallerMarshalFunc = shortFileLog

	base := zlog.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.DateTime})
	return base.
		With().Caller().Logger().
		With().Timestamp().Logger()
}

func shortFileLog(pc uintptr, file string, line int) string {
	short := file
	for i := len(file) - 1; i > 0; i-- {
		if file[i] == '/' {
			short = file[i+1:]
			break
		}
	}
	file = short
	return file + ":" + strconv.Itoa(line)
}

func CreateTopic(port, topic string) error {
	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort("localhost", port))
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		return err
	}

	return nil
}
