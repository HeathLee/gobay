package taskext

import (
	"os"
	machineryConfig "github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1"
	"github.com/shanbay/gobay"
	log "github.com/sirupsen/logrus"

)

type TaskExt struct {
	NS  string
	app *gobay.Application
	config *machineryConfig.Config
	server *machinery.Server
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
	t.config = &machineryConfig.Config {
		Broker: config.GetString("broker"),
		DefaultQueue: config.GetString("default_queue"),
		ResultBackend: config.GetString("result_backend"),
		ResultsExpireIn: config.GetInt("results_expire_in"),
		Redis: &machineryConfig.RedisConfig{
			MaxIdle:                3,
			IdleTimeout:            240,
			ReadTimeout:            15,
			WriteTimeout:           15,
			ConnectTimeout:         15,
			NormalTasksPollPeriod:  1000,
			DelayedTasksPollPeriod: 500,
		},
	}
	server, err := machinery.NewServer(t.config)
	if err != nil {
		return err
	}
	t.server = server
	return nil
}

func (t *TaskExt) Close() error {
	// TODO close
	return nil
}

func (t *TaskExt) Register(tasks map[string]interface{}) error {
	return t.server.RegisterTasks(tasks)
}

func (t *TaskExt) Consume() error {
	hostName, err := os.Hostname()
	if err != nil {
		log.Warnf("get host name failed: %v", err)
	}
	worker := t.server.NewWorker(hostName, 0)
	return worker.Launch()
}