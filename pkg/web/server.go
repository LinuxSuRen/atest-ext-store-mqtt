package web

import (
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/linuxsuren/atest-ext-store-mqtt/pkg"
)

//go:embed index.html
var indexHTML embed.FS

// SSEEvent represents a message pushed to the UI.
type SSEEvent struct {
	Topic     string `json:"topic"`
	Payload   string `json:"payload"`
	Timestamp string `json:"timestamp"`
	Format    string `json:"format,omitempty"`
}

// ConnectRequest is the payload for connecting to an MQTT broker.
type ConnectRequest struct {
	Broker   string `json:"broker"`
	Username string `json:"username"`
	Password string `json:"password"`
	ClientID string `json:"clientId"`
	UseProxy bool   `json:"useProxy"`
}

// SubscribeRequest is the payload for subscribing to a topic.
type SubscribeRequest struct {
	SessionID string `json:"sessionId"`
	Topic     string `json:"topic"`
}

// subscriber is an SSE client connection.
type subscriber struct {
	events chan SSEEvent
	done   chan struct{}
}

// Session holds one MQTT connection and its SSE subscribers.
type Session struct {
	ID          string
	Broker      string
	Client      mqtt.Client
	Topics      map[string]mqtt.MessageHandler
	subscribers map[string]*subscriber
	mu          sync.RWMutex
	done        chan struct{}

	BrokerType    BrokerType
	BrokerClients *BrokerClients
	clientsMu     sync.RWMutex

	UseProxy      bool
	ProtoRegistry *ProtoRegistry
	origProxyEnv  map[string]string
}

// SessionManager holds all active sessions.
type SessionManager struct {
	sessions map[string]*Session
	mu       sync.RWMutex
}

// NewSessionManager creates a new session manager.
func NewSessionManager() *SessionManager {
	return &SessionManager{
		sessions: make(map[string]*Session),
	}
}

// NewServer creates and returns the HTTP router.
func NewServer(sm *SessionManager) http.Handler {
	r := mux.NewRouter()

	// API routes (must be registered before catch-all)
	api := r.PathPrefix("/api").Subrouter()
	api.HandleFunc("/connect", sm.handleConnect).Methods("POST")
	api.HandleFunc("/disconnect", sm.handleDisconnect).Methods("POST")
	api.HandleFunc("/subscribe", sm.handleSubscribe).Methods("POST")
	api.HandleFunc("/unsubscribe", sm.handleUnsubscribe).Methods("POST")
	api.HandleFunc("/topics", sm.handleListTopics).Methods("GET")
	api.HandleFunc("/events", sm.handleSSE).Methods("GET")
	api.HandleFunc("/clients", sm.handleClients).Methods("GET")
	api.HandleFunc("/proto/upload", sm.handleProtoUpload).Methods("POST")
	api.HandleFunc("/proto/types", sm.handleProtoTypes).Methods("GET")
	api.HandleFunc("/proto/clear", sm.handleProtoClear).Methods("POST")

	// Serve the SPA
	r.PathPrefix("/").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" || r.URL.Path == "/index.html" {
			data, err := indexHTML.ReadFile("index.html")
			if err != nil {
				http.Error(w, "index not found", http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.Write(data)
			return
		}
		http.NotFound(w, r)
	})

	return r
}

// handleConnect creates a new MQTT session.
func (sm *SessionManager) handleConnect(w http.ResponseWriter, r *http.Request) {
	var req ConnectRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}
	if req.Broker == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "broker URL is required"})
		return
	}

	clientID := req.ClientID
	if clientID == "" {
		clientID = fmt.Sprintf("mqtt-web-%s", uuid.New().String()[:8])
	}

	opts := mqtt.NewClientOptions()
	opts.AddBroker(req.Broker)
	opts.SetClientID(clientID)
	opts.SetConnectTimeout(10 * time.Second)
	opts.SetAutoReconnect(true)
	opts.SetCleanSession(true)

	if req.Username != "" {
		opts.SetUsername(req.Username)
	}
	if req.Password != "" {
		opts.SetPassword(req.Password)
	}

	session := &Session{
		ID:            uuid.New().String(),
		Broker:        req.Broker,
		Topics:        make(map[string]mqtt.MessageHandler),
		subscribers:   make(map[string]*subscriber),
		done:          make(chan struct{}),
		UseProxy:      req.UseProxy,
		ProtoRegistry: NewProtoRegistry(),
	}

	if !req.UseProxy {
		session.origProxyEnv = saveProxyEnv()
		clearProxyEnv()
	}

	// Re-subscribe all known topics on (re)connect.
	opts.SetOnConnectHandler(func(c mqtt.Client) {
		log.Printf("[mqtt-web] session %s (re)connected to %s", session.ID, req.Broker)
		session.mu.RLock()
		topics := make(map[string]mqtt.MessageHandler, len(session.Topics))
		for t, h := range session.Topics {
			topics[t] = h
		}
		session.mu.RUnlock()
		for t, h := range topics {
			if token := c.Subscribe(t, 1, h); token.Wait() && token.Error() != nil {
				log.Printf("[mqtt-web] session %s failed to re-subscribe %s: %v", session.ID, t, token.Error())
			} else {
				log.Printf("[mqtt-web] session %s re-subscribed to %s", session.ID, t)
			}
		}
	})

	opts.SetConnectionLostHandler(func(c mqtt.Client, err error) {
		log.Printf("[mqtt-web] session %s connection lost: %v", session.ID, err)
	})

	cli := mqtt.NewClient(opts)
	if token := cli.Connect(); token.Wait() && token.Error() != nil {
		restoreProxyEnv(session.origProxyEnv)
		session.origProxyEnv = nil
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": token.Error().Error()})
		return
	}

	session.Client = cli

	sm.mu.Lock()
	sm.sessions[session.ID] = session
	sm.mu.Unlock()

	go func() {
		session.clientsMu.Lock()
		session.BrokerType = detectBroker(cli, req.Broker)
		session.clientsMu.Unlock()
		log.Printf("[mqtt-web] session %s broker type detected: %s", session.ID, session.BrokerType)
	}()

	writeJSON(w, http.StatusOK, map[string]string{"sessionId": session.ID})
	log.Printf("[mqtt-web] session %s connected to %s", session.ID, req.Broker)
}

// handleDisconnect tears down a session.
func (sm *SessionManager) handleDisconnect(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SessionID string `json:"sessionId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request"})
		return
	}

	sm.removeSession(req.SessionID)
	writeJSON(w, http.StatusOK, map[string]string{"status": "disconnected"})
}

// handleSubscribe subscribes to an MQTT topic for a session.
func (sm *SessionManager) handleSubscribe(w http.ResponseWriter, r *http.Request) {
	var req SubscribeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request"})
		return
	}

	sm.mu.RLock()
	session, ok := sm.sessions[req.SessionID]
	sm.mu.RUnlock()
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "session not found"})
		return
	}

	session.mu.Lock()
	if _, exists := session.Topics[req.Topic]; exists {
		session.mu.Unlock()
		writeJSON(w, http.StatusConflict, map[string]string{"error": "already subscribed to topic"})
		return
	}
	session.mu.Unlock()

	handler := func(_ mqtt.Client, msg mqtt.Message) {
		rawPayload := msg.Payload()
		event := SSEEvent{
			Topic:     msg.Topic(),
			Payload:   string(rawPayload),
			Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		}
		if typeName, jsonData := session.ProtoRegistry.Detect(rawPayload); typeName != "" {
			event.Payload = jsonData
			event.Format = "proto:" + typeName
		} else if pkg.IsProtobuf(rawPayload) {
			event.Format = "proto"
		}
		session.broadcast(event)
	}

	if token := session.Client.Subscribe(req.Topic, 1, handler); token.Wait() && token.Error() != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": token.Error().Error()})
		return
	}

	session.mu.Lock()
	session.Topics[req.Topic] = handler
	session.mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]string{"status": "subscribed", "topic": req.Topic})
	log.Printf("[mqtt-web] session %s subscribed to %s", session.ID, req.Topic)
}

// handleUnsubscribe removes a topic subscription.
func (sm *SessionManager) handleUnsubscribe(w http.ResponseWriter, r *http.Request) {
	var req SubscribeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request"})
		return
	}

	sm.mu.RLock()
	session, ok := sm.sessions[req.SessionID]
	sm.mu.RUnlock()
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "session not found"})
		return
	}

	session.Client.Unsubscribe(req.Topic)

	session.mu.Lock()
	delete(session.Topics, req.Topic)
	session.mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]string{"status": "unsubscribed", "topic": req.Topic})
}

// handleListTopics returns the subscribed topics for a session.
func (sm *SessionManager) handleListTopics(w http.ResponseWriter, r *http.Request) {
	sessionID := r.URL.Query().Get("sessionId")
	if sessionID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "sessionId required"})
		return
	}

	sm.mu.RLock()
	session, ok := sm.sessions[sessionID]
	sm.mu.RUnlock()
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "session not found"})
		return
	}

	session.mu.RLock()
	topics := make([]string, 0, len(session.Topics))
	for t := range session.Topics {
		topics = append(topics, t)
	}
	session.mu.RUnlock()

	writeJSON(w, http.StatusOK, map[string]interface{}{"topics": topics})
}

// handleClients queries connected client info for a session.
func (sm *SessionManager) handleClients(w http.ResponseWriter, r *http.Request) {
	sessionID := r.URL.Query().Get("sessionId")
	if sessionID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "sessionId required"})
		return
	}

	sm.mu.RLock()
	session, ok := sm.sessions[sessionID]
	sm.mu.RUnlock()
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "session not found"})
		return
	}

	session.clientsMu.RLock()
	brokerType := session.BrokerType
	session.clientsMu.RUnlock()

	if brokerType == "" || brokerType == BrokerUnknown {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"type":      BrokerUnknown,
			"clients":   nil,
			"updatedAt": nil,
		})
		return
	}

	bc := queryBrokerClients(session.Client, session.Broker, brokerType)

	session.clientsMu.Lock()
	session.BrokerClients = bc
	session.clientsMu.Unlock()

	if bc.Clients == nil {
		bc.Clients = []ClientInfo{}
	}
	writeJSON(w, http.StatusOK, bc)
}

// handleSSE establishes an SSE connection for a session.
func (sm *SessionManager) handleSSE(w http.ResponseWriter, r *http.Request) {
	sessionID := r.URL.Query().Get("sessionId")
	if sessionID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "sessionId required"})
		return
	}

	sm.mu.RLock()
	session, ok := sm.sessions[sessionID]
	sm.mu.RUnlock()
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "session not found"})
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	sub := &subscriber{
		events: make(chan SSEEvent, 256),
		done:   make(chan struct{}),
	}

	subID := uuid.New().String()
	session.mu.Lock()
	session.subscribers[subID] = sub
	session.mu.Unlock()

	defer func() {
		session.mu.Lock()
		delete(session.subscribers, subID)
		session.mu.Unlock()
		close(sub.done)
	}()

	// Send initial connected event
	fmt.Fprintf(w, "event: connected\ndata: {\"sessionId\":\"%s\"}\n\n", sessionID)
	flusher.Flush()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-session.done:
			fmt.Fprintf(w, "event: disconnected\ndata: {}\n\n")
			flusher.Flush()
			return
		case event := <-sub.events:
			data, _ := json.Marshal(event)
			fmt.Fprintf(w, "event: message\ndata: %s\n\n", data)
			flusher.Flush()
		}
	}
}

// broadcast sends an event to all SSE subscribers of a session.
func (s *Session) broadcast(event SSEEvent) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, sub := range s.subscribers {
		select {
		case sub.events <- event:
		default:
			// drop if channel is full
		}
	}
}

// removeSession cleans up a session and its MQTT connection.
func (sm *SessionManager) removeSession(sessionID string) {
	sm.mu.Lock()
	session, ok := sm.sessions[sessionID]
	if ok {
		delete(sm.sessions, sessionID)
	}
	sm.mu.Unlock()

	if !ok {
		return
	}

	restoreProxyEnv(session.origProxyEnv)
	session.origProxyEnv = nil
	close(session.done)
	session.Client.Disconnect(250)
	log.Printf("[mqtt-web] session %s disconnected", session.ID)
}

// Shutdown closes all sessions.
func (sm *SessionManager) Shutdown() {
	sm.mu.Lock()
	ids := make([]string, 0, len(sm.sessions))
	for id := range sm.sessions {
		ids = append(ids, id)
	}
	sm.mu.Unlock()

	for _, id := range ids {
		sm.removeSession(id)
	}
}

func (sm *SessionManager) handleProtoUpload(w http.ResponseWriter, r *http.Request) {
	sessionID := r.URL.Query().Get("sessionId")
	if sessionID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "sessionId is required"})
		return
	}

	sm.mu.RLock()
	session, ok := sm.sessions[sessionID]
	sm.mu.RUnlock()
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "session not found"})
		return
	}

	const maxUploadSize = 10 << 20 // 10 MB
	r.Body = http.MaxBytesReader(w, r.Body, maxUploadSize)
	if err := r.ParseMultipartForm(maxUploadSize); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to parse form: " + err.Error()})
		return
	}

	files := r.MultipartForm.File["files"]
	if len(files) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "no files provided"})
		return
	}

	var allTypes []string
	var allFields []ProtoFieldInfo
	errors := make(map[string]string)

	for _, fh := range files {
		f, err := fh.Open()
		if err != nil {
			errors[fh.Filename] = err.Error()
			continue
		}
		content, err := io.ReadAll(f)
		f.Close()
		if err != nil {
			errors[fh.Filename] = err.Error()
			continue
		}

		types, fields, err := session.ProtoRegistry.LoadProto(fh.Filename, string(content))
		if err != nil {
			errors[fh.Filename] = err.Error()
			continue
		}
		allTypes = append(allTypes, types...)
		allFields = append(allFields, fields...)
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"types":  allTypes,
		"fields": allFields,
		"errors": errors,
		"total":  session.ProtoRegistry.Count(),
	})
}

func (sm *SessionManager) handleProtoTypes(w http.ResponseWriter, r *http.Request) {
	sessionID := r.URL.Query().Get("sessionId")
	if sessionID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "sessionId is required"})
		return
	}

	sm.mu.RLock()
	session, ok := sm.sessions[sessionID]
	sm.mu.RUnlock()
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "session not found"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"types": session.ProtoRegistry.Types(),
		"total": session.ProtoRegistry.Count(),
	})
}

func (sm *SessionManager) handleProtoClear(w http.ResponseWriter, r *http.Request) {
	sessionID := r.URL.Query().Get("sessionId")
	if sessionID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "sessionId is required"})
		return
	}

	sm.mu.RLock()
	session, ok := sm.sessions[sessionID]
	sm.mu.RUnlock()
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "session not found"})
		return
	}

	session.ProtoRegistry.Clear()
	writeJSON(w, http.StatusOK, map[string]string{"status": "cleared"})
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

var proxyEnvVars = []string{
	"HTTP_PROXY", "http_proxy",
	"HTTPS_PROXY", "https_proxy",
	"ALL_PROXY", "all_proxy",
	"NO_PROXY", "no_proxy",
}

func saveProxyEnv() map[string]string {
	out := make(map[string]string, len(proxyEnvVars))
	for _, k := range proxyEnvVars {
		out[k] = os.Getenv(k)
	}
	return out
}

func clearProxyEnv() {
	for _, k := range proxyEnvVars {
		os.Unsetenv(k)
	}
}

func restoreProxyEnv(saved map[string]string) {
	if saved == nil {
		return
	}
	for _, k := range proxyEnvVars {
		os.Setenv(k, saved[k])
	}
}
