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

type interfaceProviderServer struct {
	pb.UnimplementedInterfaceProviderServiceServer
	SwitchAgent switchAgent.SwitchAgent
}

func NewInterfaceProviderServer(sa switchAgent.SwitchAgent) pb.InterfaceProviderServiceServer {
	return &interfaceProviderServer{SwitchAgent: sa}
}

func (s *interfaceProviderServer) EnsureInterface(ctx context.Context, req *pb.EnsureInterfaceRequest) (*pb.EnsureInterfaceResponse, error) {
	log.Printf("InterfaceProvider.EnsureInterface called for %s", req.GetInterfaceName())

	status := s.SwitchAgent.EnsureInterface(ctx, &agent.EnsureInterfaceRequest{
		InterfaceName:  req.GetInterfaceName(),
		AdminStatus:    agent.DeviceStatus(req.GetAdminStatus()),
		Description:    req.GetDescription(),
		Type:           req.GetInterfaceType(),
		MTU:            req.GetMtu(),
		IPv4Prefixes:   req.GetIpv4Prefixes(),
		VRFName:        req.GetVrfName(),
		SwitchportMode: req.GetSwitchportMode(),
		AccessVlan:     req.GetAccessVlan(),
		NativeVlan:     req.GetNativeVlan(),
		AllowedVlans:   req.GetAllowedVlans(),
	})
	if status != nil {
		return &pb.EnsureInterfaceResponse{
			Status: &pb.Status{Code: status.Code, Message: status.Message},
		}, nil
	}
	return &pb.EnsureInterfaceResponse{}, nil
}

func (s *interfaceProviderServer) DeleteInterface(ctx context.Context, req *pb.InterfaceProviderInterfaceRequest) (*pb.DeleteInterfaceResponse, error) {
	log.Printf("InterfaceProvider.DeleteInterface called for %s", req.GetInterfaceName())

	status := s.SwitchAgent.DeleteInterface(ctx, req.GetInterfaceName())
	if status != nil {
		return &pb.DeleteInterfaceResponse{
			Status: &pb.Status{Code: status.Code, Message: status.Message},
		}, nil
	}
	return &pb.DeleteInterfaceResponse{}, nil
}

func (s *interfaceProviderServer) GetInterfaceStatus(ctx context.Context, req *pb.InterfaceProviderInterfaceRequest) (*pb.GetInterfaceStatusResponse, error) {
	log.Printf("InterfaceProvider.GetInterfaceStatus called for %s", req.GetInterfaceName())

	ifaceStatus, status := s.SwitchAgent.GetInterfaceStatus(ctx, req.GetInterfaceName())
	if status != nil {
		return &pb.GetInterfaceStatusResponse{
			Status: &pb.Status{Code: status.Code, Message: status.Message},
		}, nil
	}
	return &pb.GetInterfaceStatusResponse{
		InterfaceStatus: &pb.InterfaceProviderInterfaceStatus{
			OperStatus:  ifaceStatus.OperStatus,
			OperMessage: ifaceStatus.OperMessage,
		},
	}, nil
}
