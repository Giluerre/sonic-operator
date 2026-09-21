// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package agent_server

import (
	"context"
	"log"

	switchAgent "github.com/ironcore-dev/sonic-operator/internal/agent/interface"
	pb "github.com/ironcore-dev/sonic-operator/internal/agent/proto"
	agent "github.com/ironcore-dev/sonic-operator/internal/agent/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type wireSonicSwitchServer struct {
	pb.UnimplementedWireSonicSwitchServiceServer
	SwitchAgent switchAgent.SwitchAgent
}

func NewWireSonicSwitchServer(sa switchAgent.SwitchAgent) pb.WireSonicSwitchServiceServer {
	return &wireSonicSwitchServer{SwitchAgent: sa}
}

func (s *wireSonicSwitchServer) GetDevice(ctx context.Context, req *pb.WireGetDeviceRequest) (*pb.WireGetDeviceResponse, error) {
	log.Printf("Wire.GetDevice called: device=%s", req.GetDevice())

	device, agentStatus := s.SwitchAgent.GetDeviceInfo(ctx)
	if agentStatus != nil {
		return nil, status.Errorf(codes.Internal, "failed to get device info: %s", agentStatus.Message)
	}
	return &pb.WireGetDeviceResponse{DeviceId: device.LocalMacAddress}, nil
}

func (s *wireSonicSwitchServer) ApplySwitch(ctx context.Context, req *pb.ApplySwitchRequest) (*pb.ApplySwitchResponse, error) {
	log.Printf("Wire.ApplySwitch called: device=%s", req.GetDevice())

	cfg := req.GetConfig()
	agentReq := &agent.ApplySwitchRequest{
		Device: req.GetDevice(),
		Config: protoSwitchConfigToAgent(cfg),
	}

	agentStatus := s.SwitchAgent.ApplySwitch(ctx, agentReq)
	if agentStatus != nil {
		return nil, status.Errorf(codes.Internal, "failed to apply switch config: %s", agentStatus.Message)
	}
	return &pb.ApplySwitchResponse{}, nil
}

func (s *wireSonicSwitchServer) DeleteSwitch(ctx context.Context, req *pb.DeleteSwitchRequest) (*pb.DeleteSwitchResponse, error) {
	log.Printf("Wire.DeleteSwitch called: device=%s", req.GetDevice())
	agentStatus := s.SwitchAgent.DeleteSwitch(ctx, req.GetDevice())
	switch {
	case agentStatus == nil:
		// Reprovision completed — cell is gone.
		return &pb.DeleteSwitchResponse{State: ""}, nil
	case agentStatus.Code == 2:
		// Reprovision in progress — caller should retry.
		return &pb.DeleteSwitchResponse{State: agentStatus.Message}, nil
	default:
		return nil, status.Errorf(codes.Internal, "failed to delete switch config: %s", agentStatus.Message)
	}
}

func (s *wireSonicSwitchServer) GetCellStatus(ctx context.Context, req *pb.GetCellStatusRequest) (*pb.GetCellStatusResponse, error) {
	log.Printf("Wire.GetCellStatus called: device=%s", req.GetDevice())
	state, message, agentStatus := s.SwitchAgent.GetCellStatus(ctx, req.GetDevice())
	if agentStatus != nil {
		return nil, status.Errorf(codes.Internal, "failed to get cell status: %s", agentStatus.Message)
	}
	return &pb.GetCellStatusResponse{State: state, Message: message}, nil
}

func (s *wireSonicSwitchServer) GetInterface(ctx context.Context, req *pb.WireGetInterfaceRequest) (*pb.WireGetInterfaceResponse, error) {
	log.Printf("Wire.GetInterface called: iface=%s", req.GetIface())

	iface, agentStatus := s.SwitchAgent.GetInterface(ctx, &agent.Interface{
		TypeMeta: agent.TypeMeta{Kind: agent.InterfaceKind},
		Name:     req.GetIface(),
	})
	if agentStatus != nil {
		return nil, status.Errorf(codes.Internal, "failed to get interface: %s", agentStatus.Message)
	}
	return &pb.WireGetInterfaceResponse{InterfaceId: iface.NativeName}, nil
}

func (s *wireSonicSwitchServer) GetInterfaceState(ctx context.Context, req *pb.WireGetInterfaceStateRequest) (*pb.WireGetInterfaceStateResponse, error) {
	log.Printf("Wire.GetInterfaceState called: iface=%s", req.GetIface())

	ifaceStatus, agentStatus := s.SwitchAgent.GetInterfaceStatus(ctx, req.GetIface())
	if agentStatus != nil {
		return nil, status.Errorf(codes.Internal, "failed to get interface state: %s", agentStatus.Message)
	}
	return &pb.WireGetInterfaceStateResponse{AdminUp: ifaceStatus.AdminStatus, OperUp: ifaceStatus.OperStatus}, nil
}

func (s *wireSonicSwitchServer) ListInterfacesV2(ctx context.Context, _ *pb.ListInterfacesRequestV2) (*pb.ListInterfacesResponseV2, error) {
	log.Printf("Wire.ListInterfacesV2 called")
	names, agentStatus := s.SwitchAgent.ListInterfacesV2(ctx)
	if agentStatus != nil {
		return nil, status.Errorf(codes.Internal, "failed to list interfaces: %s", agentStatus.Message)
	}
	return &pb.ListInterfacesResponseV2{Interfaces: names}, nil
}

func (s *wireSonicSwitchServer) SetInterfaceAdminState(ctx context.Context, req *pb.WireSetInterfaceAdminStateRequest) (*pb.WireSetInterfaceAdminStateResponse, error) {
	log.Printf("Wire.SetInterfaceAdminState called: iface=%s, admin_state=%v", req.GetIface(), req.GetAdminState())

	adminStatus := agent.StatusDown
	if req.GetAdminState() {
		adminStatus = agent.StatusUp
	}

	_, agentStatus := s.SwitchAgent.SetInterfaceAdminStatus(ctx, &agent.Interface{
		TypeMeta:    agent.TypeMeta{Kind: agent.InterfaceKind},
		Name:        req.GetIface(),
		AdminStatus: adminStatus,
	})
	if agentStatus != nil {
		return nil, status.Errorf(codes.Internal, "failed to set interface admin state: %s", agentStatus.Message)
	}
	return &pb.WireSetInterfaceAdminStateResponse{}, nil
}

func protoSwitchConfigToAgent(cfg *pb.SwitchConfig) agent.WireSwitchConfig {
	if cfg == nil {
		return agent.WireSwitchConfig{}
	}

	vlans := make([]agent.WireVLAN, 0, len(cfg.GetVlans()))
	for _, v := range cfg.GetVlans() {
		members := make([]agent.WireVLANMember, 0, len(v.GetMembers()))
		for _, m := range v.GetMembers() {
			members = append(members, agent.WireVLANMember{InterfaceID: m.GetInterfaceId()})
		}
		vlans = append(vlans, agent.WireVLAN{
			ID:        v.GetId(),
			Prefix:    v.GetPrefix(),
			DHCPRelay: v.GetDhcpRelay(),
			Members:   members,
		})
	}

	var bgp *agent.WireBGPConfig
	if b := cfg.GetBgp(); b != nil {
		groups := make([]agent.WireBGPPeerGroup, 0, len(b.GetPeerGroups()))
		for _, pg := range b.GetPeerGroups() {
			neighbors := make([]agent.WireBGPNeighbor, 0, len(pg.GetNeighbors()))
			for _, n := range pg.GetNeighbors() {
				neighbors = append(neighbors, agent.WireBGPNeighbor{
					VlanID:      n.GetVlanId(),
					InterfaceID: n.GetInterfaceId(),
				})
			}
			groups = append(groups, agent.WireBGPPeerGroup{Name: pg.GetName(), Neighbors: neighbors})
		}
		bgp = &agent.WireBGPConfig{
			ASN:        b.GetAsn(),
			RouterID:   b.GetRouterId(),
			PeerGroups: groups,
		}
	}

	var meta agent.SwitchConfigMetadata
	if m := cfg.GetMetadata(); m != nil {
		meta = agent.SwitchConfigMetadata{
			Namespace: m.GetNamespace(),
			Name:      m.GetName(),
			UID:       m.GetUid(),
		}
	}

	return agent.WireSwitchConfig{
		Metadata:    meta,
		Hostname:    cfg.GetHostname(),
		LoopbackIPs: cfg.GetLoopbackIps(),
		Prefixes:    cfg.GetPrefixes(),
		VLANs:       vlans,
		BGP:         bgp,
	}
}
