package main

import (
	"fmt"

	"github.com/hellodhlyn/sqstask"
)

func main() {
	// Declare your task.
	task, err := sqstask.NewSQSTask(&sqstask.Options{
		QueueName: "my-awesome-queue",
		AWSRegion: "ap-northeast-2",
		Consume: func(message string) error {
			fmt.Printf("Message received: %s\n", message)
			return nil
		},
		HandleError: func(err error) {
			fmt.Println(err)
		},
	})
	if err != nil {
		panic(err)
	}

	// Produce some message to task.
	task.Produce("Hello!")

	// Start consumer.
	// This make blocking until recieving SIGINT, SIGTERM or SIGKILL.
	task.StartConsumer()
}
