// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"

	commands "github.com/ironcore-dev/sonic-operator/internal/agent/agent_client/commands"
)

var (
	version   = "dev"     // overridden by -ldflags "-X main.version=<git-hash>"
	buildTime = "unknown" // overridden by -ldflags "-X main.buildTime=<iso8601>"
)

func main() {
	cmd := commands.Command()
	cmd.Version = fmt.Sprintf("%s (built %s)", version, buildTime)

	if err := cmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
