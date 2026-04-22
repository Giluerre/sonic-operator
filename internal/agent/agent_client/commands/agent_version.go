// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package commands

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	client "github.com/ironcore-dev/sonic-operator/internal/agent/agent_client/client"
)

func GetAgentVersion() *cobra.Command {
	return &cobra.Command{
		Use:     "agent-version",
		Short:   "Get the version of the connected sonic-agent server",
		Example: "agent_cli get agent-version",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return RunGetAgentVersion(cmd.Context(), GetSharedSwitchAgentClient())
		},
	}
}

func RunGetAgentVersion(ctx context.Context, c client.SwitchAgentClient) error {
	version, buildTime, err := c.GetAgentVersion(ctx)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(os.Stdout, "Version:   %s\nBuildTime: %s\n", version, buildTime)
	return err
}
