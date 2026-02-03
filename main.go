package main

import (
	"context"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/urfave/cli/v3"
	_ "github.com/urfave/cli/v3"
)

var configPath string

func main() {
	cmd := cli.Command{
		Name:  "waterway",
		Usage: "Sei EVM RPC Proxy with WebSocket to HTTP Fallback",
		Action: func(ctx context.Context, command *cli.Command) error {
			var err error
			var opts []Option
			if configPath != "" {
				opts, err = LoadConfig(filepath.Clean(configPath))
				if err != nil {
					return err
				}
			}

			waterway, err := NewWaterway(opts...)
			if err != nil {
				logger.Error("Failed to instantiate", "err", err)
				return err
			}
			if err := waterway.Start(ctx); err != nil {
				logger.Error("Failed to start", "err", err)
				return err
			}

			signals := make(chan os.Signal, 1)
			signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
			<-signals
			if err := waterway.Shutdown(ctx); err != nil {
				logger.Error("Failed to shutdown gracefully", "err", err)
				return err
			}
			return nil
		},
		Arguments: []cli.Argument{
			&cli.StringArg{
				Name:        "file",
				Destination: &configPath,
				Config: cli.StringConfig{
					TrimSpace: true,
				},
			},
		},
	}
	if err := cmd.Run(context.Background(), os.Args); err != nil {
		logger.Error("Failed to run Waterway", "err", err)
		os.Exit(1)
	}
}
