package cmd

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	ext "github.com/linuxsuren/api-testing/pkg/extension"
	"github.com/linuxsuren/atest-ext-store-mqtt/pkg"
	"github.com/linuxsuren/atest-ext-store-mqtt/pkg/web"
	"github.com/spf13/cobra"
)

func getLocalIPs() []string {
	var ips []string
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ips
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() && ipnet.IP.To4() != nil {
			ips = append(ips, ipnet.IP.String())
		}
	}
	return ips
}

// NewRootCommand returns the root Command
func NewRootCommand() (c *cobra.Command) {
	opt := &options{
		Extension: ext.NewExtension("mqtt", "store", 7074),
		webPort:   8080,
	}
	c = &cobra.Command{
		Use:   opt.GetFullName(),
		Short: "A store extension for MQTT",
		RunE:  opt.runE,
	}
	opt.AddFlags(c)
	return
}

type options struct {
	*ext.Extension
	webPort int
}

func (o *options) AddFlags(cmd *cobra.Command) {
	o.Extension.AddFlags(cmd.Flags())
	cmd.Flags().IntVar(&o.webPort, "web-port", 8080, "Port for the MQTT web viewer UI")
}

func (o *options) runE(cmd *cobra.Command, _ []string) (err error) {
	remoteServer := pkg.NewRemoteServer()

	sm := web.NewSessionManager()
	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", o.webPort),
		Handler: web.NewServer(sm),
	}

	go func() {
		ips := getLocalIPs()
		if len(ips) == 0 {
			log.Printf("[mqtt-web] starting web UI on http://localhost:%d", o.webPort)
		} else {
			log.Printf("[mqtt-web] starting web UI, available on:")
			for _, ip := range ips {
				log.Printf("[mqtt-web]   http://%s:%d", ip, o.webPort)
			}
		}
		if serveErr := httpServer.ListenAndServe(); serveErr != nil && serveErr != http.ErrServerClosed {
			log.Printf("[mqtt-web] server error: %v", serveErr)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	grpcDone := make(chan error, 1)
	go func() {
		grpcDone <- ext.CreateRunner(o.Extension, cmd, remoteServer)
	}()

	select {
	case err = <-grpcDone:
		if err != nil {
			log.Printf("[mqtt-web] gRPC server exited with error: %v", err)
		}
	case <-sigCh:
		log.Printf("[mqtt-web] received shutdown signal")
	}

	sm.Shutdown()
	httpServer.Shutdown(context.Background())
	return
}
