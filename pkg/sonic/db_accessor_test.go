// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package sonic

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestAccessor(t *testing.T) (DBAccessor, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return NewDBAccessor(client, client, client), mr
}

func TestListPortNames(t *testing.T) {
	db, mr := newTestAccessor(t)
	mr.HSet("PORT|Ethernet0", "speed", "10000")
	mr.HSet("PORT|Ethernet4", "speed", "25000")

	names, err := db.ListPortNames(context.Background())
	if err != nil {
		t.Fatalf("ListPortNames: %v", err)
	}
	if len(names) != 2 {
		t.Fatalf("expected 2 ports, got %d: %v", len(names), names)
	}
	seen := map[string]bool{}
	for _, n := range names {
		seen[n] = true
	}
	if !seen["Ethernet0"] || !seen["Ethernet4"] {
		t.Errorf("unexpected port names: %v", names)
	}
}

func TestGetPortSupportedSpeeds_Present(t *testing.T) {
	db, mr := newTestAccessor(t)
	mr.HSet("PORT_TABLE|Ethernet0", "supported_speeds", "10000,25000,100000")

	speeds, err := db.GetPortSupportedSpeeds(context.Background(), "Ethernet0")
	if err != nil {
		t.Fatalf("GetPortSupportedSpeeds: %v", err)
	}
	if speeds != "10000,25000,100000" {
		t.Errorf("unexpected supported speeds: %q", speeds)
	}
}

func TestGetPortSupportedSpeeds_Absent(t *testing.T) {
	db, _ := newTestAccessor(t)

	speeds, err := db.GetPortSupportedSpeeds(context.Background(), "Ethernet0")
	if err != nil {
		t.Fatalf("expected nil error for absent key, got: %v", err)
	}
	if speeds != "" {
		t.Errorf("expected empty string, got: %q", speeds)
	}
}

func TestGetTransceiverType_Present(t *testing.T) {
	db, mr := newTestAccessor(t)
	mr.HSet("TRANSCEIVER_INFO|Ethernet0", "type", "QSFP28")

	typ, err := db.GetTransceiverType(context.Background(), "Ethernet0")
	if err != nil {
		t.Fatalf("GetTransceiverType: %v", err)
	}
	if typ != "QSFP28" {
		t.Errorf("unexpected type: %q", typ)
	}
}

func TestGetTransceiverType_Absent(t *testing.T) {
	db, _ := newTestAccessor(t)

	typ, err := db.GetTransceiverType(context.Background(), "Ethernet0")
	if err != nil {
		t.Fatalf("expected nil error for absent key, got: %v", err)
	}
	if typ != "" {
		t.Errorf("expected empty string, got: %q", typ)
	}
}

func TestSetFEC_Valid(t *testing.T) {
	db, _ := newTestAccessor(t)
	for _, fec := range []string{"rs", "fc", "none"} {
		if err := db.SetFEC(context.Background(), "Ethernet0", fec); err != nil {
			t.Errorf("SetFEC(%q): unexpected error: %v", fec, err)
		}
	}
}

func TestSetFEC_Invalid(t *testing.T) {
	db, _ := newTestAccessor(t)
	if err := db.SetFEC(context.Background(), "Ethernet0", "auto"); err == nil {
		t.Error("SetFEC with invalid value should return error")
	}
}

func TestSetAdminStatus_Valid(t *testing.T) {
	db, _ := newTestAccessor(t)
	for _, status := range []string{"up", "down"} {
		if err := db.SetAdminStatus(context.Background(), "Ethernet0", status); err != nil {
			t.Errorf("SetAdminStatus(%q): unexpected error: %v", status, err)
		}
	}
}

func TestSetAdminStatus_Invalid(t *testing.T) {
	db, _ := newTestAccessor(t)
	if err := db.SetAdminStatus(context.Background(), "Ethernet0", "enabled"); err == nil {
		t.Error("SetAdminStatus with invalid value should return error")
	}
	if err := db.SetAdminStatus(context.Background(), "Ethernet0", ""); err == nil {
		t.Error("SetAdminStatus with empty value should return error")
	}
}

func TestSyncIPAddresses(t *testing.T) {
	db, mr := newTestAccessor(t)
	ctx := context.Background()
	iface := "Loopback0"

	// seed two existing IPs
	mr.HSet("LOOPBACK_INTERFACE|"+iface+"|10.0.0.1/32", "NULL", "NULL")
	mr.HSet("LOOPBACK_INTERFACE|"+iface+"|10.0.0.2/32", "NULL", "NULL")

	// sync to keep only 10.0.0.2 and add 10.0.0.3
	desired := []string{"10.0.0.2/32", "10.0.0.3/32"}
	if err := db.SyncIPAddresses(ctx, iface, desired); err != nil {
		t.Fatalf("SyncIPAddresses: %v", err)
	}

	addrs, err := db.ListIPAddresses(ctx, iface)
	if err != nil {
		t.Fatalf("ListIPAddresses: %v", err)
	}
	addrSet := map[string]bool{}
	for _, a := range addrs {
		addrSet[a] = true
	}
	if addrSet["10.0.0.1/32"] {
		t.Error("10.0.0.1/32 should have been removed")
	}
	if !addrSet["10.0.0.2/32"] {
		t.Error("10.0.0.2/32 should be present")
	}
	if !addrSet["10.0.0.3/32"] {
		t.Error("10.0.0.3/32 should have been added")
	}
}

func TestSyncPortVLANs(t *testing.T) {
	db, mr := newTestAccessor(t)
	ctx := context.Background()
	port := "Ethernet0"

	// seed existing memberships
	mr.HSet("VLAN_MEMBER|Vlan10|"+port, "tagging_mode", "untagged")
	mr.HSet("VLAN_MEMBER|Vlan20|"+port, "tagging_mode", "tagged")
	mr.HSet("VLAN|Vlan10", "vlanid", "10")
	mr.HSet("VLAN|Vlan20", "vlanid", "20")
	mr.HSet("VLAN|Vlan30", "vlanid", "30")

	// sync: remove Vlan10, keep Vlan20, add Vlan30
	desired := map[string]string{
		"Vlan20": "tagged",
		"Vlan30": "untagged",
	}
	if err := db.SyncPortVLANs(ctx, port, desired); err != nil {
		t.Fatalf("SyncPortVLANs: %v", err)
	}

	members, err := db.ListPortVLANs(ctx, port)
	if err != nil {
		t.Fatalf("ListPortVLANs: %v", err)
	}
	memberSet := map[string]bool{}
	for _, m := range members {
		memberSet[m] = true
	}
	if memberSet["Vlan10"] {
		t.Error("Vlan10 membership should have been removed")
	}
	if !memberSet["Vlan20"] {
		t.Error("Vlan20 membership should be present")
	}
	if !memberSet["Vlan30"] {
		t.Error("Vlan30 membership should have been added")
	}
}

func TestParseSupportedSpeeds(t *testing.T) {
	cases := []struct {
		raw      string
		fallback int
		want     []int32
	}{
		{"10000,25000,100000", 0, []int32{10, 25, 100}},
		{"", 10000, []int32{10}},
		{"", 0, nil},
		{"  25000 , 100000 ", 0, []int32{25, 100}},
	}
	for _, c := range cases {
		got := ParseSupportedSpeeds(c.raw, c.fallback)
		if len(got) != len(c.want) {
			t.Errorf("ParseSupportedSpeeds(%q, %d): got %v, want %v", c.raw, c.fallback, got, c.want)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("ParseSupportedSpeeds(%q, %d)[%d]: got %d, want %d", c.raw, c.fallback, i, got[i], c.want[i])
			}
		}
	}
}
