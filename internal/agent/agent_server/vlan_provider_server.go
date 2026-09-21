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

type vlanProviderServer struct {
	pb.UnimplementedVLANProviderServiceServer
	SwitchAgent switchAgent.SwitchAgent
}

func NewVLANProviderServer(sa switchAgent.SwitchAgent) pb.VLANProviderServiceServer {
	return &vlanProviderServer{SwitchAgent: sa}
}

func (s *vlanProviderServer) EnsureVLAN(ctx context.Context, req *pb.VLANProviderEnsureRequest) (*pb.VLANProviderEnsureResponse, error) {
	log.Printf("VLANProvider.EnsureVLAN called for vlan_id %d", req.GetVlanId())

	status := s.SwitchAgent.EnsureVLAN(ctx, &agent.VLANRequest{
		VlanID:     req.GetVlanId(),
		Name:       req.GetName(),
		AdminState: req.GetAdminState(),
	})
	if status != nil {
		return &pb.VLANProviderEnsureResponse{
			Status: &pb.Status{Code: status.Code, Message: status.Message},
		}, nil
	}
	return &pb.VLANProviderEnsureResponse{}, nil
}

func (s *vlanProviderServer) DeleteVLAN(ctx context.Context, req *pb.VLANProviderDeleteRequest) (*pb.VLANProviderDeleteResponse, error) {
	log.Printf("VLANProvider.DeleteVLAN called for vlan_id %d", req.GetVlanId())

	status := s.SwitchAgent.DeleteVLAN(ctx, req.GetVlanId())
	if status != nil {
		return &pb.VLANProviderDeleteResponse{
			Status: &pb.Status{Code: status.Code, Message: status.Message},
		}, nil
	}
	return &pb.VLANProviderDeleteResponse{}, nil
}

func (s *vlanProviderServer) GetVLANStatus(ctx context.Context, req *pb.VLANProviderGetStatusRequest) (*pb.VLANProviderGetStatusResponse, error) {
	log.Printf("VLANProvider.GetVLANStatus called for vlan_id %d", req.GetVlanId())

	vlanStatus, status := s.SwitchAgent.GetVLANStatus(ctx, req.GetVlanId())
	if status != nil {
		return &pb.VLANProviderGetStatusResponse{
			Status: &pb.Status{Code: status.Code, Message: status.Message},
		}, nil
	}
	return &pb.VLANProviderGetStatusResponse{
		OperStatus: vlanStatus.OperStatus,
	}, nil
}
