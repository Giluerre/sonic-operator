// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package agent_server

import (
	"context"
	"log"
	"time"

	switchAgent "github.com/ironcore-dev/sonic-operator/internal/agent/interface"
	pb "github.com/ironcore-dev/sonic-operator/internal/agent/proto"
)

type deviceProviderServer struct {
	pb.UnimplementedDeviceProviderServiceServer
	SwitchAgent switchAgent.SwitchAgent
}

func NewDeviceProviderServer(sa switchAgent.SwitchAgent) pb.DeviceProviderServiceServer {
	return &deviceProviderServer{SwitchAgent: sa}
}

func (s *deviceProviderServer) ListPorts(ctx context.Context, _ *pb.DeviceProviderListPortsRequest) (*pb.DeviceProviderListPortsResponse, error) {
	log.Printf("DeviceProvider.ListPorts called")

	portList, status := s.SwitchAgent.ListPortsV2(ctx)
	if status != nil {
		return &pb.DeviceProviderListPortsResponse{
			Status: &pb.Status{Code: status.Code, Message: status.Message},
		}, nil
	}

	ports := make([]*pb.DevicePort, 0, len(portList.Items))
	for _, p := range portList.Items {
		ports = append(ports, &pb.DevicePort{
			Id:                  p.ID,
			Type:                p.Type,
			SupportedSpeedsGbps: p.SupportedSpeedsGbps,
			Transceiver:         p.Transceiver,
		})
	}
	return &pb.DeviceProviderListPortsResponse{Ports: ports}, nil
}

func (s *deviceProviderServer) GetDeviceInfo(ctx context.Context, _ *pb.DeviceProviderGetDeviceInfoRequest) (*pb.DeviceProviderGetDeviceInfoResponse, error) {
	log.Printf("DeviceProvider.GetDeviceInfo called")

	device, status := s.SwitchAgent.GetDeviceInfo(ctx)
	if status != nil {
		return &pb.DeviceProviderGetDeviceInfoResponse{
			Status: &pb.Status{Code: status.Code, Message: status.Message},
		}, nil
	}

	return &pb.DeviceProviderGetDeviceInfoResponse{
		Info: &pb.DeviceInfo{
			// TODO: almost all the data which need to be provided here are stored in EEPROM, to read them would be better approach
			// create sonic-hostservice method
			Model:           device.Hwsku,
			FirmwareVersion: device.SonicOSVersion,
			Manufacturer:    device.Hwsku,
			SerialNumber:    device.Hwsku,
		},
	}, nil
}

func (s *deviceProviderServer) GetLastRebootTime(ctx context.Context, _ *pb.DeviceProviderGetLastRebootTimeRequest) (*pb.DeviceProviderGetLastRebootTimeResponse, error) {
	log.Printf("DeviceProvider.GetLastRebootTime called")

	t, status := s.SwitchAgent.GetLastRebootTime(ctx)
	if status != nil {
		return &pb.DeviceProviderGetLastRebootTimeResponse{
			Status: &pb.Status{Code: status.Code, Message: status.Message},
		}, nil
	}

	return &pb.DeviceProviderGetLastRebootTimeResponse{
		LastRebootTime: t.Format(time.RFC3339),
	}, nil
}

func (s *deviceProviderServer) Reboot(ctx context.Context, _ *pb.DeviceProviderRebootRequest) (*pb.DeviceProviderRebootResponse, error) {
	log.Printf("DeviceProvider.Reboot called")

	status := s.SwitchAgent.Reboot(ctx)
	if status != nil {
		return &pb.DeviceProviderRebootResponse{
			Status: &pb.Status{Code: status.Code, Message: status.Message},
		}, nil
	}
	return &pb.DeviceProviderRebootResponse{}, nil
}

func (s *deviceProviderServer) FactoryReset(ctx context.Context, _ *pb.DeviceProviderFactoryResetRequest) (*pb.DeviceProviderFactoryResetResponse, error) {
	log.Printf("DeviceProvider.FactoryReset called")

	status := s.SwitchAgent.FactoryReset(ctx)
	if status != nil {
		return &pb.DeviceProviderFactoryResetResponse{
			Status: &pb.Status{Code: status.Code, Message: status.Message},
		}, nil
	}
	return &pb.DeviceProviderFactoryResetResponse{}, nil
}

func (s *deviceProviderServer) Reprovision(ctx context.Context, _ *pb.DeviceProviderReprovisionRequest) (*pb.DeviceProviderReprovisionResponse, error) {
	log.Printf("DeviceProvider.Reprovision called")

	status := s.SwitchAgent.Reprovision(ctx)
	if status != nil {
		return &pb.DeviceProviderReprovisionResponse{
			Status: &pb.Status{Code: status.Code, Message: status.Message},
		}, nil
	}
	return &pb.DeviceProviderReprovisionResponse{}, nil
}
