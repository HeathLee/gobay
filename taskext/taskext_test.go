package taskext

import (
	"log"
	"testing"
	"time"

	"github.com/shanbay/gobay"
)

var (
	app        *gobay.Application
	task       TaskExt
	taskTesult []int64
)

func init() {
	task = TaskExt{NS: "task"}

	app, _ = gobay.CreateApp(
		"../testdata",
		"testing",
		map[gobay.Key]gobay.Extension{
			"task": &task,
		},
	)
	if err := app.Init(); err != nil {
		log.Println(err)
	}
}


func TestPushConsume(t *testing.T) {
	var i int64
	for i = 0; i < 10; i++ {
		sign, err := BuildSign("add", []interface{}{i})
		if err != nil {
			t.Error(err)
		}
		if _, err := task.Push(sign); err != nil {
			t.Error(err)
		}
	}

	if err := task.Register("add", TaskAdd); err != nil {
		t.Error(err)
	}

	go task.Consume("gobay.task")
	time.Sleep(2 * time.Second)
	if len(taskTesult) != 10 {
		t.Error("consume length doesn't match publish'")
	}

}
func TaskAdd(args ...int64) error {
	sum := int64(0)
	for _, arg := range args {
		sum += arg
	}
	taskTesult = append(taskTesult, sum)
	return nil
}