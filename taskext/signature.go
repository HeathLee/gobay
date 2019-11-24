package taskext

import (
	"reflect"
	"time"

	"github.com/RichardKnop/machinery/v1/tasks"
)

var typesMap = map[string]reflect.Type{
	// base types
	"bool":    reflect.TypeOf(true),
	"int":     reflect.TypeOf(int(1)),
	"int8":    reflect.TypeOf(int8(1)),
	"int16":   reflect.TypeOf(int16(1)),
	"int32":   reflect.TypeOf(int32(1)),
	"int64":   reflect.TypeOf(int64(1)),
	"uint":    reflect.TypeOf(uint(1)),
	"uint8":   reflect.TypeOf(uint8(1)),
	"uint16":  reflect.TypeOf(uint16(1)),
	"uint32":  reflect.TypeOf(uint32(1)),
	"uint64":  reflect.TypeOf(uint64(1)),
	"float32": reflect.TypeOf(float32(0.5)),
	"float64": reflect.TypeOf(float64(0.5)),
	"string":  reflect.TypeOf(string("")),
	// slices
	"[]bool":    reflect.TypeOf(make([]bool, 0)),
	"[]int":     reflect.TypeOf(make([]int, 0)),
	"[]int8":    reflect.TypeOf(make([]int8, 0)),
	"[]int16":   reflect.TypeOf(make([]int16, 0)),
	"[]int32":   reflect.TypeOf(make([]int32, 0)),
	"[]int64":   reflect.TypeOf(make([]int64, 0)),
	"[]uint":    reflect.TypeOf(make([]uint, 0)),
	"[]uint8":   reflect.TypeOf(make([]uint8, 0)),
	"[]uint16":  reflect.TypeOf(make([]uint16, 0)),
	"[]uint32":  reflect.TypeOf(make([]uint32, 0)),
	"[]uint64":  reflect.TypeOf(make([]uint64, 0)),
	"[]float32": reflect.TypeOf(make([]float32, 0)),
	"[]float64": reflect.TypeOf(make([]float64, 0)),
	"[]byte":    reflect.TypeOf(make([]byte, 0)),
	"[]string":  reflect.TypeOf([]string{""}),
}

type Option func(*tasks.Signature)

func Queue(queue string) Option {
	return func(sign *tasks.Signature) {
		sign.RoutingKey = queue
	}
}

func ETA(t *time.Time) Option {
	return func(sign *tasks.Signature) {
		sign.ETA = t
	}
}

func Retry(count int) Option {
	return func(sign *tasks.Signature) {
		sign.RetryCount = count
	}
}

func validateSignArgs(args []tasks.Arg) error {
	for _, arg := range args {
		if _, ok := typesMap[arg.Type]; !ok {
			return tasks.NewErrUnsupportedType(arg.Type)
		}
	}
	return nil
}

func BuildSign(taskName string, args []interface{},
	setters ...Option) (*tasks.Signature, error) {
	signArgs := make([]tasks.Arg, 0, len(args))
	for _, arg := range args {
		signArgs = append(signArgs, tasks.Arg{
			Type:  reflect.TypeOf(arg).String(),
			Value: arg,
		})
	}
	if err := validateSignArgs(signArgs); err != nil {
		return nil, err
	}
	sign := &tasks.Signature{
		Name: taskName,
		Args: signArgs,
	}
	for _, setter := range setters {
		setter(sign)
	}
	return sign, nil
}
