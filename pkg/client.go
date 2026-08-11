package pkg

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/linuxsuren/api-testing/pkg/testing/remote"
)

var proxyEnvVars = []string{
	"HTTP_PROXY", "http_proxy",
	"HTTPS_PROXY", "https_proxy",
	"ALL_PROXY", "all_proxy",
	"NO_PROXY", "no_proxy",
}

func (s *remoteserver) getClient(ctx context.Context) (cli mqtt.Client, err error) {
	store := remote.GetStoreFromContext(ctx)
	if store == nil {
		err = errors.New("no MQTT store configuration found in context")
		return
	}

	opts := mqtt.NewClientOptions()
	opts.AddBroker(store.URL)
	opts.SetClientID(fmt.Sprintf("atest-store-mqtt-%d", time.Now().UnixNano()))
	opts.SetConnectTimeout(10 * time.Second)

	if store.Username != "" {
		opts.SetUsername(store.Username)
	}
	if store.Password != "" {
		opts.SetPassword(store.Password)
	}

	origEnv := make(map[string]string)
	if os.Getenv("MQTT_NO_PROXY") == "true" {
		for _, k := range proxyEnvVars {
			origEnv[k] = os.Getenv(k)
			os.Unsetenv(k)
		}
	}

	cli = mqtt.NewClient(opts)
	if token := cli.Connect(); token.Wait() && token.Error() != nil {
		for _, k := range proxyEnvVars {
			os.Setenv(k, origEnv[k])
		}
		err = token.Error()
		return
	}
	for _, k := range proxyEnvVars {
		os.Setenv(k, origEnv[k])
	}
	return
}
