// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"

	server "github.com/ironcore-dev/sonic-operator/internal/agent/agent_server"
)

var (
	version   = "dev"     // overridden by -ldflags "-X main.version=<git-hash>"
	buildTime = "unknown" // overridden by -ldflags "-X main.buildTime=<iso8601>"
)

func printBanner() {
	fmt.Printf(`
   _____  ____  _   _ _  _____                             _
  / ____|/ __ \| \ | (_)/ ____|      /\                   | |
 | (___ | |  | |  \| |_| |   ______ /  \   __ _  ___ _ __ | |_
  \___ \| |  | | . `+"`"+` | | |  |______/ /\ \ / _`+"`"+` |/ _ \ '_ \| __|
  ____) | |__| | |\  | | |____    / ____ \ (_| |  __/ | | | |_
 |_____/ \____/|_| \_|_|\_____|  /_/    \_\__, |\___|_| |_|\__|
                                           __/ |
                                          |___/     by IronCore
 Build: %s  Built: %s

`, version, buildTime)
}

func main() {
	server.AgentVersion = version
	server.AgentBuildTime = buildTime
	printBanner()
	server.StartServer()
}
