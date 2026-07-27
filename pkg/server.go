package pkg

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/linuxsuren/api-testing/pkg/extension"
	"github.com/linuxsuren/api-testing/pkg/server"
	"github.com/linuxsuren/api-testing/pkg/testing/remote"
	"github.com/linuxsuren/api-testing/pkg/version"
)

type remoteserver struct {
	remote.UnimplementedLoaderServer
}

// NewRemoteServer creates a remote server instance
func NewRemoteServer() remote.LoaderServer {
	return &remoteserver{}
}

func (s *remoteserver) ListTestSuite(ctx context.Context, _ *server.Empty) (suites *remote.TestSuites, err error) {
	return
}
func (s *remoteserver) CreateTestSuite(ctx context.Context, testSuite *remote.TestSuite) (reply *server.Empty, err error) {
	return
}
func (s *remoteserver) GetTestSuite(ctx context.Context, suite *remote.TestSuite) (reply *remote.TestSuite, err error) {
	return
}
func (s *remoteserver) UpdateTestSuite(ctx context.Context, suite *remote.TestSuite) (reply *remote.TestSuite, err error) {
	return
}
func (s *remoteserver) DeleteTestSuite(ctx context.Context, suite *remote.TestSuite) (reply *server.Empty, err error) {
	return
}
func (s *remoteserver) ListTestCases(ctx context.Context, suite *remote.TestSuite) (reply *server.TestCases, err error) {
	return
}
func (s *remoteserver) CreateTestCase(ctx context.Context, testcase *server.TestCase) (reply *server.Empty, err error) {
	return
}
func (s *remoteserver) GetTestCase(ctx context.Context, input *server.TestCase) (reply *server.TestCase, err error) {
	return
}
func (s *remoteserver) UpdateTestCase(ctx context.Context, testcase *server.TestCase) (reply *server.TestCase, err error) {
	return
}
func (s *remoteserver) DeleteTestCase(ctx context.Context, testcase *server.TestCase) (reply *server.Empty, err error) {
	return
}
func (s *remoteserver) Verify(ctx context.Context, in *server.Empty) (reply *server.ExtensionStatus, err error) {
	reply = &server.ExtensionStatus{
		Version: version.GetVersion(),
		Ready:   true,
	}
	return
}
func (s *remoteserver) PProf(ctx context.Context, in *server.PProfRequest) (data *server.PProfData, err error) {
	log.Println("pprof", in.Name)

	data = &server.PProfData{
		Data: extension.LoadPProf(in.Name),
	}
	return
}

func (s *remoteserver) Query(ctx context.Context, query *server.DataQuery) (result *server.DataQueryResult, err error) {
	topic := query.Sql
	if topic == "" {
		err = fmt.Errorf("topic filter is required")
		return
	}

	var cli mqtt.Client
	cli, err = s.getClient(ctx)
	if err != nil {
		return
	}
	defer cli.Disconnect(250)

	result = &server.DataQueryResult{}
	var mu sync.Mutex
	messages := make(map[string]string)

	handler := func(_ mqtt.Client, msg mqtt.Message) {
		mu.Lock()
		defer mu.Unlock()
		messages[msg.Topic()] = string(msg.Payload())
	}

	if token := cli.Subscribe(topic, 1, handler); token.Wait() && token.Error() != nil {
		err = token.Error()
		return
	}

	// Wait for messages with a timeout
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case <-time.After(3 * time.Second):
		// timeout - collect what we got
	}

	cli.Unsubscribe(topic)

	mu.Lock()
	defer mu.Unlock()
	for k, v := range messages {
		result.Data = append(result.Data, &server.Pair{
			Key:   k,
			Value: v,
		})
	}

	return
}
