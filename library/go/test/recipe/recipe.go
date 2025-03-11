// Package recipe contains helper function for implementation of ya
// make recipes. https://wiki.yandex-team.ru/yatool/test/recipes
package recipe

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/transferria/transferria/library/go/test/yatest"
)

type Recipe interface {
	Start() error
	Stop() error
}

// SetEnv modifies environment variables of the current test. Useful
// for passing dynamic configuration, e.g. ports.
func SetEnv(name, value string) {
	envFile := yatest.EnvFile()
	f, err := os.OpenFile(envFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	defer func() { _ = f.Close() }()

	err = json.NewEncoder(f).Encode(map[string]string{name: value})
	if err != nil {
		panic(err)
	}
}

func Run(r Recipe) {

	for _, arg := range os.Args {

		var action func() error

		if arg == "start" {
			action = r.Start
		} else if arg == "stop" {
			action = r.Stop
		}

		if action != nil {
			err := action()
			if err != nil {
				_, _ = fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			break
		}

	}
}
