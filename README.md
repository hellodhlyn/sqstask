# SQSTask

> Pubsub task using Amazon SQS

## Usage

###### Install

```bash
go get github.com/hellodhlyn/sqstask
```

###### Add to your code

```go
import (
    "fmt"

    "github.com/hellodhlyn/sqstask"
)

func main() {
    // Declare your task.
    task, _ := sqstask.NewSQSTask(&sqstask.Options{
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

    // Produce some message to task.
    task.Produce("Hello!")

    // Start consumer.
    // This make blocking until recieving SIGINT, SIGTERM or SIGKILL.
    task.StartConsumer()
}
```
