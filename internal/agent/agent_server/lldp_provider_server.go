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

type lldpProviderServer struct {
	pb.UnimplementedLLDPProviderServiceServer
	SwitchAgent switchAgent.SwitchAgent
}

func NewLLDPProviderServer(sa switchAgent.SwitchAgent) pb.LLDPProviderServiceServer {
	return &lldpProviderServer{SwitchAgent: sa}
}

func (s *lldpProviderServer) EnsureLLDP(ctx context.Context, req *pb.LLDPProviderEnsureRequest) (*pb.LLDPProviderEnsureResponse, error) {
	log.Printf("LLDPProvider.EnsureLLDP called with admin_state %q, %d interfaces", req.GetAdminState(), len(req.GetInterfaces()))

	ifaces := make([]agent.LLDPInterfaceConfig, 0, len(req.GetInterfaces()))
	for _, i := range req.GetInterfaces() {
		ifaces = append(ifaces, agent.LLDPInterfaceConfig{
			InterfaceName: i.GetInterfaceName(),
			AdminState:    i.GetAdminState(),
		})
	}

	status := s.SwitchAgent.EnsureLLDP(ctx, &agent.LLDPRequest{
		AdminState: req.GetAdminState(),
		Interfaces: ifaces,
	})
	if status != nil {
		return &pb.LLDPProviderEnsureResponse{
			Status: &pb.Status{Code: status.Code, Message: status.Message},
		}, nil
	}
	return &pb.LLDPProviderEnsureResponse{}, nil
}

func (s *lldpProviderServer) DeleteLLDP(ctx context.Context, _ *pb.LLDPProviderDeleteRequest) (*pb.LLDPProviderDeleteResponse, error) {
	log.Printf("LLDPProvider.DeleteLLDP called")

	status := s.SwitchAgent.DeleteLLDP(ctx)
	if status != nil {
		return &pb.LLDPProviderDeleteResponse{
			Status: &pb.Status{Code: status.Code, Message: status.Message},
		}, nil
	}
	return &pb.LLDPProviderDeleteResponse{}, nil
}

func (s *lldpProviderServer) GetLLDPStatus(ctx context.Context, _ *pb.LLDPProviderGetStatusRequest) (*pb.LLDPProviderGetStatusResponse, error) {
	log.Printf("LLDPProvider.GetLLDPStatus called")

	lldpStatus, status := s.SwitchAgent.GetLLDPStatus(ctx)
	if status != nil {
		return &pb.LLDPProviderGetStatusResponse{
			Status: &pb.Status{Code: status.Code, Message: status.Message},
		}, nil
	}
	return &pb.LLDPProviderGetStatusResponse{
		OperStatus: lldpStatus.OperStatus,
	}, nil
}
