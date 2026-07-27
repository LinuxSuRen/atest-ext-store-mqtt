package pkg

import (
	"context"
	"errors"
	"fmt"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/linuxsuren/api-testing/pkg/testing/remote"
)

func (s *remoteserver) getClient(ctx context.Context) (cli mqtt.Client, err error) {
	store := remote.GetStoreFromContext(ctx)
	if store == nil {
		err = errors.New("no MQTT store configuration found in context")
		return
	}

	opts := mqtt.NewClientOptions()
	opts.AddBroker(store.URL)
	opts.SetClientID(fmt.Sprintf("atest-store-mqtt-%d", time.Now().UnixNano()))
	opts.SetConnectTimeout(5 * time.Second)

	if store.Username != "" {
		opts.SetUsername(store.Username)
	}
	if store.Password != "" {
		opts.SetPassword(store.Password)
	}

	cli = mqtt.NewClient(opts)
	if token := cli.Connect(); token.Wait() && token.Error() != nil {
		err = token.Error()
		return
	}

	return
}
