// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package agent_server

import (
	"context"
	"log"

	switchAgent "github.com/ironcore-dev/sonic-operator/internal/agent/interface"
	pb "github.com/ironcore-dev/sonic-operator/internal/agent/proto"
	agent "github.com/ironcore-dev/sonic-operator/internal/agent/types"
)

type dhcpRelayProviderServer struct {
	pb.UnimplementedDHCPRelayProviderServiceServer
	SwitchAgent switchAgent.SwitchAgent
}

func NewDHCPRelayProviderServer(sa switchAgent.SwitchAgent) pb.DHCPRelayProviderServiceServer {
	return &dhcpRelayProviderServer{SwitchAgent: sa}
}

func (s *dhcpRelayProviderServer) EnsureDHCPRelay(ctx context.Context, req *pb.DHCPRelayProviderEnsureRequest) (*pb.DHCPRelayProviderEnsureResponse, error) {
	log.Printf("DHCPRelayProvider.EnsureDHCPRelay called for interfaces %v", req.GetInterfaceNames())

	status := s.SwitchAgent.EnsureDHCPRelay(ctx, &agent.DHCPRelayRequest{
		InterfaceNames:  req.GetInterfaceNames(),
		ServerAddresses: req.GetServerAddresses(),
		VRFName:         req.GetVrfName(),
	})
	if status != nil {
		return &pb.DHCPRelayProviderEnsureResponse{
			Status: &pb.Status{Code: status.Code, Message: status.Message},
		}, nil
	}
	return &pb.DHCPRelayProviderEnsureResponse{}, nil
}

func (s *dhcpRelayProviderServer) DeleteDHCPRelay(ctx context.Context, req *pb.DHCPRelayProviderDeleteRequest) (*pb.DHCPRelayProviderDeleteResponse, error) {
	log.Printf("DHCPRelayProvider.DeleteDHCPRelay called for interfaces %v", req.GetInterfaceNames())

	status := s.SwitchAgent.DeleteDHCPRelay(ctx, req.GetInterfaceNames())
	if status != nil {
		return &pb.DHCPRelayProviderDeleteResponse{
			Status: &pb.Status{Code: status.Code, Message: status.Message},
		}, nil
	}
	return &pb.DHCPRelayProviderDeleteResponse{}, nil
}

func (s *dhcpRelayProviderServer) GetDHCPRelayStatus(ctx context.Context, req *pb.DHCPRelayProviderGetStatusRequest) (*pb.DHCPRelayProviderGetStatusResponse, error) {
	log.Printf("DHCPRelayProvider.GetDHCPRelayStatus called for interfaces %v", req.GetInterfaceNames())

	relayStatus, status := s.SwitchAgent.GetDHCPRelayStatus(ctx, req.GetInterfaceNames())
	if status != nil {
		return &pb.DHCPRelayProviderGetStatusResponse{
			Status: &pb.Status{Code: status.Code, Message: status.Message},
		}, nil
	}
	return &pb.DHCPRelayProviderGetStatusResponse{
		ConfiguredInterfaces: relayStatus.ConfiguredInterfaces,
	}, nil
}
