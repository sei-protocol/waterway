package main

import (
	"context"
	"os"
	"path/filepath"

	"github.com/urfave/cli/v3"
	_ "github.com/urfave/cli/v3"
)

var configPath string

func main() {
	cmd := cli.Command{
		Name:  "sluice",
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

			sluice, err := NewSluice(ctx, opts...)
			if err != nil {
				logger.Error("Failed to instantiate Sluice", "err", err)
				return err
			}
			if err := sluice.Start(ctx); err != nil {
				logger.Error("Failed to start Sluice", "err", err)
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
		logger.Error("Failed to run Sluice", "err", err)
		os.Exit(1)
	}
}
