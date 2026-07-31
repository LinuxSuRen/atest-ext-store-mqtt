package web

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type BrokerType string

const (
	BrokerUnknown   BrokerType = "unknown"
	BrokerMosquitto BrokerType = "mosquitto"
	BrokerEMQX      BrokerType = "emqx"
)

type ClientInfo struct {
	ClientID  string `json:"clientId"`
	Username  string `json:"username,omitempty"`
	Connected bool   `json:"connected"`
	IP        string `json:"ip,omitempty"`
	KeepAlive int    `json:"keepAlive,omitempty"`
	ProtoVer  int    `json:"protoVer,omitempty"`
}

type BrokerClients struct {
	Type      BrokerType   `json:"type"`
	Clients   []ClientInfo `json:"clients"`
	Total     int          `json:"total"`
	UpdatedAt time.Time    `json:"updatedAt"`
}

func extractHost(mqttURL string) string {
	u, err := url.Parse(mqttURL)
	if err != nil {
		return ""
	}
	host, _, err := net.SplitHostPort(u.Host)
	if err != nil {
		return u.Host
	}
	return host
}

func detectBroker(cli mqtt.Client, brokerURL string) BrokerType {
	type result struct {
		typ BrokerType
	}
	ch := make(chan result, 2)

	go func() {
		if detectMosquitto(cli) {
			ch <- result{typ: BrokerMosquitto}
		}
	}()

	go func() {
		host := extractHost(brokerURL)
		if host != "" && detectEMQX(host) {
			ch <- result{typ: BrokerEMQX}
		}
	}()

	select {
	case r := <-ch:
		return r.typ
	case <-time.After(3 * time.Second):
		return BrokerUnknown
	}
}

func detectMosquitto(cli mqtt.Client) bool {
	detected := make(chan bool, 1)
	token := cli.Subscribe("$SYS/broker/version", 0, func(_ mqtt.Client, msg mqtt.Message) {
		log.Printf("[mqtt-web] $SYS/broker/version: %s", string(msg.Payload()))
		detected <- true
	})
	if !token.WaitTimeout(2 * time.Second) || token.Error() != nil {
		return false
	}
	select {
	case <-detected:
		return true
	case <-time.After(2 * time.Second):
		cli.Unsubscribe("$SYS/broker/version")
		return false
	}
}

func detectEMQX(host string) bool {
	for _, port := range []int{18083} {
		url := fmt.Sprintf("http://%s:%d/api/v5/status", host, port)
		resp, err := httpGet(url, 2*time.Second)
		if err == nil && resp != nil {
			return true
		}
	}
	return false
}

func queryBrokerClients(cli mqtt.Client, brokerURL string, typ BrokerType) *BrokerClients {
	result := &BrokerClients{Type: typ, UpdatedAt: time.Now()}

	switch typ {
	case BrokerMosquitto:
		result.Total, result.Clients = queryMosquittoClients(cli)
	case BrokerEMQX:
		result.Clients = queryEMQXClients(brokerURL)
		result.Total = len(result.Clients)
	default:
		result.Clients = nil
	}
	return result
}

func queryMosquittoClients(cli mqtt.Client) (int, []ClientInfo) {
	var mu sync.Mutex
	total := -1

	token := cli.Subscribe("$SYS/broker/clients/connected", 0, func(_ mqtt.Client, msg mqtt.Message) {
		mu.Lock()
		defer mu.Unlock()
		if _, err := fmt.Sscanf(string(msg.Payload()), "%d", &total); err != nil || total < 0 {
			total = -1
		}
	})
	if !token.WaitTimeout(2*time.Second) || token.Error() != nil {
		return 0, nil
	}

	time.Sleep(2 * time.Second)
	cli.Unsubscribe("$SYS/broker/clients/connected")

	mu.Lock()
	defer mu.Unlock()
	if total < 0 {
		return 0, nil
	}
	return total, nil
}

type emqxClient struct {
	ClientID  string `json:"clientid"`
	Username  string `json:"username"`
	Connected bool   `json:"connected"`
	IPAddr    string `json:"ip_address"`
	KeepAlive int    `json:"keepalive"`
	ProtoVer  int    `json:"proto_ver"`
}

type emqxClientsResponse struct {
	Data []emqxClient `json:"data"`
}

func queryEMQXClients(brokerURL string) []ClientInfo {
	host := extractHost(brokerURL)
	if host == "" {
		return nil
	}
	for _, port := range []int{18083} {
		clients := tryEMQXAPI(host, port)
		if clients != nil {
			return clients
		}
	}
	return nil
}

func tryEMQXAPI(host string, port int) []ClientInfo {
	apiURL := fmt.Sprintf("http://%s:%d/api/v5/clients", host, port)
	body, err := httpGet(apiURL, 3*time.Second)
	if err != nil {
		return nil
	}
	var resp emqxClientsResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil
	}
	var result []ClientInfo
	for _, c := range resp.Data {
		result = append(result, ClientInfo{
			ClientID:  c.ClientID,
			Username:  c.Username,
			Connected: c.Connected,
			IP:        c.IPAddr,
			KeepAlive: c.KeepAlive,
			ProtoVer:  c.ProtoVer,
		})
	}
	return result
}

func httpGet(url string, timeout time.Duration) ([]byte, error) {
	client := &http.Client{Timeout: timeout}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}
