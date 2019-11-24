package taskext

import (
	"os"
	"strings"

	"github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/backends/result"
	machineryConfig "github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/mitchellh/mapstructure"
	log "github.com/sirupsen/logrus"

	"github.com/shanbay/gobay"
)

var defaultRedis = &machineryConfig.RedisConfig{
	MaxIdle:                3,
	IdleTimeout:            240,
	ReadTimeout:            15,
	WriteTimeout:           15,
	ConnectTimeout:         15,
	NormalTasksPollPeriod:  1000,
	DelayedTasksPollPeriod: 500,
}

type TaskExt struct {
	NS      string
	app     *gobay.Application
	config  *machineryConfig.Config
	server  *machinery.Server
	workers []*machinery.Worker
}

func (t *TaskExt) Object() interface{} {
	return t
}

func (t *TaskExt) Application() *gobay.Application {
	return t.app
}

func (t *TaskExt) Init(app *gobay.Application) error {
	t.app = app
	config := app.Config()
	if t.NS != "" {
		config = config.Sub(t.NS)
	}
	t.config = &machineryConfig.Config{}
	if err := config.Unmarshal(t.config, func(config *mapstructure.DecoderConfig) {
		config.TagName = "yaml"
		// do anything your like
	}); err != nil {
		log.Panicf("parse config error: %v", err)
	}
	if strings.HasPrefix(t.config.Broker, "redis") && t.config.Redis == nil {
		t.config.Redis = defaultRedis
	}

	server, err := machinery.NewServer(t.config)
	if err != nil {
		return err
	}
	t.server = server
	return nil
}

func (t *TaskExt) Close() error {
	for _, worker := range t.workers {
		worker.Quit()
	}
	return nil
}

func (t *TaskExt) Register(name string, handler interface{}) error {
	return t.server.RegisterTask(name, handler)
}

func (t *TaskExt) Consume(queue string) error {
	hostName, err := os.Hostname()
	if err != nil {
		log.Warnf("get host name failed: %v", err)
	}
	worker := t.server.NewWorker(hostName, 0)
	worker.Queue = queue
	t.workers = append(t.workers, worker)
	return worker.Launch()
}

func (t *TaskExt) Push(sign *tasks.Signature) (*result.AsyncResult, error) {
	asyncResult, err := t.server.SendTask(sign)
	if err != nil {
		log.Errorf("send task failed: %v", err)
		return nil, err
	}
	return asyncResult, nil
}
