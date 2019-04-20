package sqstask

import (
	"errors"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

var sqsService *sqs.SQS

// Consumer 는 SQS에서 메시지를 수신하였을 때 호출되는 콜백 함수 타입입니다.
// 반환하는 error가 nil이 아닌 경우, 해당 메시지는 consume에 실패한 것으로
// 판단하여 다시 consume을 시도합니다.
type Consumer func(string) error

// ErrorHandler 는 SQS에서 메시지를 수신하던 중 오류가 발생할 때 호출되는 콜백
// 함수 타입입니다.
type ErrorHandler func(error)

// SQSTask 는 SQS를 통해 데이터를 pub-sub 하는 task의 타입입니다.
type SQSTask struct {
	queueURL string
	options  *Options
}

// Options 는 SQSTask를 생성할 때 필요한 데이터 옵션입니다.
type Options struct {
	QueueName  string
	WorkerSize int
	AWSRegion  string

	Consume     Consumer
	HandleError ErrorHandler
}

// NewSQSTask 함수는 새로운 SQSTask를 생성합니다.
func NewSQSTask(opts *Options) (*SQSTask, error) {
	if opts.QueueName == "" {
		return nil, errors.New("QueueName must be provided")
	}
	if opts.AWSRegion == "" {
		return nil, errors.New("AWSRegion must be provided")
	}

	sess, err := session.NewSession(&aws.Config{Region: aws.String(opts.AWSRegion)})
	if err != nil {
		panic(err)
	}
	sqsService = sqs.New(sess)

	return &SQSTask{options: opts}, nil
}

// Produce 함수는 SQS로 메시지를 발송합니다.
func (task *SQSTask) Produce(message string) error {
	queueURL, err := task.GetQueueURL()
	if err != nil {
		return err
	}

	_, err = sqsService.SendMessage(&sqs.SendMessageInput{
		MessageBody: aws.String(message),
		QueueUrl:    aws.String(queueURL),
	})
	return err
}

// StartConsumer 함수는 SQS로부터 데이터를 불러와 컨슈밍을 시작합니다.
func (task *SQSTask) StartConsumer() error {
	// Worker의 개수를 정하되, 기본값으로는 코어의 갯수를 사용합니다.
	workerSize := task.options.WorkerSize
	if workerSize == 0 {
		workerSize = runtime.NumCPU()
	}

	// All workers loop forever until recieving SIGINT, SIGTERM or SIGKILL.
	var sigChan = make(chan os.Signal)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)

	var workerWg sync.WaitGroup
	var shutdownChan = make(chan bool)
	for i := 0; i < workerSize; i++ {
		go task.worker(shutdownChan, &workerWg)
	}

	<-sigChan

	// If all workers give terminal message through channel, exit program.
	for i := 0; i < workerSize; i++ {
		shutdownChan <- true
	}
	workerWg.Wait()

	return nil
}

func (task *SQSTask) worker(shutdownChan chan bool, workerWg *sync.WaitGroup) {
	workerWg.Add(1)

	queueURL, err := task.GetQueueURL()
	if err != nil {
		task.options.HandleError(err)
		return
	}

	for {
		output, err := sqsService.ReceiveMessage(&sqs.ReceiveMessageInput{
			MaxNumberOfMessages: aws.Int64(10),
			QueueUrl:            aws.String(queueURL),
			WaitTimeSeconds:     aws.Int64(20),
		})
		if err != nil {
			task.options.HandleError(err)
		}

		var wg sync.WaitGroup
		for _, m := range output.Messages {
			wg.Add(1)
			go func(m *sqs.Message) {
				defer wg.Done()

				err := task.options.Consume(*m.Body)
				if err == nil {
					sqsService.DeleteMessage(&sqs.DeleteMessageInput{
						QueueUrl:      aws.String(queueURL),
						ReceiptHandle: m.ReceiptHandle,
					})
				}
			}(m)
		}
		wg.Wait()

		select {
		case <-shutdownChan:
			workerWg.Done()
			break
		default:
			continue
		}
	}
}

// GetQueueURL 함수는 현재 task의 SQS 리소스 URL을 반환합니다.
func (task *SQSTask) GetQueueURL() (string, error) {
	if task.queueURL != "" {
		return task.queueURL, nil
	}

	output, err := sqsService.GetQueueUrl(&sqs.GetQueueUrlInput{QueueName: &task.options.QueueName})
	if err != nil {
		return "", err
	}
	task.queueURL = *output.QueueUrl

	return task.queueURL, nil
}
