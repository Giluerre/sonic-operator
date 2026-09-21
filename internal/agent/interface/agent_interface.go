// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"context"
	"time"

	agent "github.com/ironcore-dev/sonic-operator/internal/agent/types"
)

type SwitchAgent interface {
	GetDeviceInfo(ctx context.Context) (*agent.SwitchDevice, *agent.Status)
	ListInterfaces(ctx context.Context) (*agent.InterfaceList, *agent.Status)
	ListInterfacesV2(ctx context.Context) ([]string, *agent.Status)

	SetInterfaceAdminStatus(ctx context.Context, iface *agent.Interface) (*agent.Interface, *agent.Status)
	SetInterfaceAliasName(ctx context.Context, iface *agent.Interface) (*agent.Interface, *agent.Status)

	GetInterface(ctx context.Context, iface *agent.Interface) (*agent.Interface, *agent.Status)
	GetInterfaceNeighbor(ctx context.Context, iface *agent.Interface) (*agent.InterfaceNeighbor, *agent.Status)

	ListPorts(ctx context.Context) (*agent.PortList, *agent.Status)
	ListPortsV2(ctx context.Context) (*agent.PortDetailsList, *agent.Status)

	SaveConfig(ctx context.Context) *agent.Status
	Reboot(ctx context.Context) *agent.Status
	OnieBootModeInstall(ctx context.Context) *agent.Status
	RestartSystemdService(ctx context.Context, serviceName string) *agent.Status
	RebootCause(ctx context.Context) (string, *agent.Status)
	FactoryReset(ctx context.Context) *agent.Status
	Reprovision(ctx context.Context) *agent.Status
	GetReadiness(ctx context.Context) (bool, *agent.Status)

	GetLastRebootTime(ctx context.Context) (time.Time, *agent.Status)
	EnsureInterface(ctx context.Context, req *agent.EnsureInterfaceRequest) *agent.Status
	DeleteInterface(ctx context.Context, interfaceName string) *agent.Status
	GetInterfaceStatus(ctx context.Context, interfaceName string) (*agent.InterfaceStatus, *agent.Status)

	EnsureDHCPRelay(ctx context.Context, req *agent.DHCPRelayRequest) *agent.Status
	DeleteDHCPRelay(ctx context.Context, interfaceNames []string) *agent.Status
	GetDHCPRelayStatus(ctx context.Context, interfaceNames []string) (*agent.DHCPRelayStatus, *agent.Status)

	EnsureVLAN(ctx context.Context, req *agent.VLANRequest) *agent.Status
	DeleteVLAN(ctx context.Context, vlanID int32) *agent.Status
	GetVLANStatus(ctx context.Context, vlanID int32) (*agent.VLANStatus, *agent.Status)

	EnsureLLDP(ctx context.Context, req *agent.LLDPRequest) *agent.Status
	DeleteLLDP(ctx context.Context) *agent.Status
	GetLLDPStatus(ctx context.Context) (*agent.LLDPStatus, *agent.Status)

	ApplySwitch(ctx context.Context, req *agent.ApplySwitchRequest) *agent.Status
	DeleteSwitch(ctx context.Context, device string) *agent.Status
	GetCellStatus(ctx context.Context, device string) (state, message string, status *agent.Status)
}
