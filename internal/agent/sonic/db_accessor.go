// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package sonic

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	agent "github.com/ironcore-dev/sonic-operator/internal/agent/types"
	"github.com/redis/go-redis/v9"
)

type portConfig struct {
	Speed int // Mbps; CONFIG_DB PORT|<name> "speed"
}

type portState struct {
	SupportedSpeeds string // comma-separated Mbps; STATE_DB PORT_TABLE|<name> "supported_speeds"
}

type transceiverInfo struct {
	Type string // STATE_DB TRANSCEIVER_INFO|<name> "type"
}

type dbAccessor struct {
	configDB *redis.Client
	stateDB  *redis.Client
	applDB   *redis.Client
}

func scanKeys(ctx context.Context, client *redis.Client, pattern string) ([]string, error) {
	var keys []string
	var cursor uint64
	for {
		batch, nextCursor, err := client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, err
		}
		keys = append(keys, batch...)
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
	return keys, nil
}

func (m *SonicAgent) newDBAccessor() (*dbAccessor, error) {
	configDB, err := m.Connect("CONFIG_DB")
	if err != nil {
		return nil, fmt.Errorf("failed to connect to CONFIG_DB: %w", err)
	}
	stateDB, err := m.Connect("STATE_DB")
	if err != nil {
		return nil, fmt.Errorf("failed to connect to STATE_DB: %w", err)
	}
	applDB, err := m.Connect("APPL_DB")
	if err != nil {
		return nil, fmt.Errorf("failed to connect to APPL_DB: %w", err)
	}
	return &dbAccessor{configDB: configDB, stateDB: stateDB, applDB: applDB}, nil
}

func (db *dbAccessor) listPortNames(ctx context.Context) ([]string, error) {
	keys, err := scanKeys(ctx, db.configDB, "PORT|*")
	if err != nil {
		return nil, fmt.Errorf("failed to list PORT keys: %w", err)
	}
	names := make([]string, len(keys))
	for i, k := range keys {
		names[i] = strings.TrimPrefix(k, "PORT|")
	}
	return names, nil
}

func (db *dbAccessor) getPortConfig(ctx context.Context, portName string) (*portConfig, error) {
	fields, err := db.configDB.HGetAll(ctx, "PORT|"+portName).Result()
	if err != nil {
		return nil, err
	}
	speed, _ := strconv.Atoi(fields["speed"])
	return &portConfig{Speed: speed}, nil
}

func (db *dbAccessor) getPortState(ctx context.Context, portName string) *portState {
	raw, err := db.stateDB.HGet(ctx, "PORT_TABLE|"+portName, "supported_speeds").Result()
	if err != nil {
		return &portState{} // field absent on virtual/copper ports
	}
	return &portState{SupportedSpeeds: raw}
}

func (db *dbAccessor) getTransceiverInfo(ctx context.Context, portName string) *transceiverInfo {
	t, err := db.stateDB.HGet(ctx, "TRANSCEIVER_INFO|"+portName, "type").Result()
	if err != nil {
		return &transceiverInfo{} // absent on virtual/copper ports
	}
	return &transceiverInfo{Type: t}
}

func (db *dbAccessor) getAdminStatus(ctx context.Context, interfaceName string) (bool, error) {
	if strings.HasPrefix(interfaceName, "Loopback") {
		val, err := db.configDB.HGet(ctx, "LOOPBACK_INTERFACE|"+interfaceName, "admin_status").Result()
		if err != nil {
			return false, nil
		}
		return val == "up", nil
	}
	if strings.HasPrefix(interfaceName, "Vlan") {
		val, err := db.applDB.HGet(ctx, "VLAN_TABLE:"+interfaceName, "admin_status").Result()
		if err != nil {
			return false, nil
		}
		return val == "up", nil
	}
	if strings.HasPrefix(interfaceName, "eth") {
		nativeName, err := agent.AbstractNameToNativeName(interfaceName)
		if err != nil {
			return false, fmt.Errorf("failed to convert interface name %s: %w", interfaceName, err)
		}
		val, err := db.applDB.HGet(ctx, "PORT_TABLE:"+nativeName, "admin_status").Result()
		if err != nil {
			return false, nil
		}
		return val == "up", nil
	}
	return false, fmt.Errorf("unknown interface: %s", interfaceName)
}

func (db *dbAccessor) getOperStatus(ctx context.Context, interfaceName string) (bool, error) {
	if strings.HasPrefix(interfaceName, "Loopback") {
		val, err := db.configDB.HGet(ctx, "LOOPBACK_INTERFACE|"+interfaceName, "admin_status").Result()
		if err != nil {
			return false, nil // absent means down
		}
		return val == "up", nil
	}
	if strings.HasPrefix(interfaceName, "Vlan") {
		val, err := db.applDB.HGet(ctx, "VLAN_TABLE:"+interfaceName, "admin_status").Result()
		if err != nil {
			return false, nil // absent means down
		}
		return val == "up", nil
	}
	if strings.HasPrefix(interfaceName, "eth") {
		nativeName, err := agent.AbstractNameToNativeName(interfaceName)
		if err != nil {
			return false, fmt.Errorf("failed to convert interface name %s: %w", interfaceName, err)
		}
		val, err := db.applDB.HGet(ctx, "PORT_TABLE:"+nativeName, "oper_status").Result()
		if err != nil {
			return false, nil // absent means down
		}
		return val == "up", nil
	}
	return false, fmt.Errorf("unknown interface: %s", interfaceName)

}

func (db *dbAccessor) ensureInterfaceLoopback(ctx context.Context, loopbackName string) error {
	if err := db.configDB.HSet(ctx, "LOOPBACK_INTERFACE|"+loopbackName, "NULL", "NULL").Err(); err != nil {
		return fmt.Errorf("failed to ensure loopback interface %s: %w", loopbackName, err)
	}
	return nil
}

func (db *dbAccessor) deleteInterfaceLoopback(ctx context.Context, loopbackName string) error {
	if err := db.configDB.Del(ctx, "LOOPBACK_INTERFACE|"+loopbackName).Err(); err != nil {
		return fmt.Errorf("failed to delete loopback interface %s: %w", loopbackName, err)
	}
	return nil
}

func (db *dbAccessor) setMTU(ctx context.Context, interfaceName string, mtu int) error {
	if strings.HasPrefix(interfaceName, "Loopback") {
		return fmt.Errorf("MTU cannot be set on loopback interface %s", interfaceName)
	}
	if err := db.configDB.HSet(ctx, "PORT|"+interfaceName, "mtu", strconv.Itoa(mtu)).Err(); err != nil {
		return fmt.Errorf("failed to set MTU for %s: %w", interfaceName, err)
	}
	return nil
}

func (db *dbAccessor) setFEC(ctx context.Context, interfaceName string, fec string) error {
	switch fec {
	case "rs", "fc", "none":
	default:
		return fmt.Errorf("invalid FEC value %q for %s: must be rs, fc, or none", fec, interfaceName)
	}
	if err := db.configDB.HSet(ctx, "PORT|"+interfaceName, "fec", fec).Err(); err != nil {
		return fmt.Errorf("failed to set FEC for %s: %w", interfaceName, err)
	}
	return nil
}

func (db *dbAccessor) setSpeed(ctx context.Context, interfaceName string, speedMbps int) error {
	if speedMbps <= 0 {
		return fmt.Errorf("invalid speed %d for %s: must be positive", speedMbps, interfaceName)
	}
	if err := db.configDB.HSet(ctx, "PORT|"+interfaceName, "speed", strconv.Itoa(speedMbps)).Err(); err != nil {
		return fmt.Errorf("failed to set speed for %s: %w", interfaceName, err)
	}
	return nil
}

func (db *dbAccessor) ensureVLAN(ctx context.Context, vlanName string) error {
	vlanID := strings.TrimPrefix(vlanName, "Vlan")
	if err := db.configDB.HSet(ctx, "VLAN|"+vlanName, "vlanid", vlanID).Err(); err != nil {
		return fmt.Errorf("failed to ensure VLAN %s: %w", vlanName, err)
	}
	return nil
}

func (db *dbAccessor) ensureVLANInterface(ctx context.Context, vlanName, vrfName string) error {
	key := "VLAN_INTERFACE|" + vlanName
	fields := []interface{}{"NULL", "NULL"}
	if vrfName != "" {
		fields = append(fields, "vrf_name", vrfName)
	}
	if err := db.configDB.HSet(ctx, key, fields...).Err(); err != nil {
		return fmt.Errorf("failed to ensure VLAN interface %s: %w", vlanName, err)
	}
	return nil
}

func (db *dbAccessor) deleteVLAN(ctx context.Context, vlanName string) error {
	if err := db.configDB.Del(ctx, "VLAN|"+vlanName).Err(); err != nil {
		return fmt.Errorf("failed to delete VLAN %s: %w", vlanName, err)
	}
	return nil
}

func (db *dbAccessor) deleteVLANInterface(ctx context.Context, vlanName string) error {
	if err := db.configDB.Del(ctx, "VLAN_INTERFACE|"+vlanName).Err(); err != nil {
		return fmt.Errorf("failed to delete VLAN interface %s: %w", vlanName, err)
	}
	return nil
}

func interfaceIPTable(interfaceName string) (string, error) {
	switch {
	case strings.HasPrefix(interfaceName, "Loopback"):
		return "LOOPBACK_INTERFACE", nil
	case strings.HasPrefix(interfaceName, "Vlan"):
		return "VLAN_INTERFACE", nil
	case strings.HasPrefix(interfaceName, "Ethernet"):
		return "INTERFACE", nil
	case strings.HasPrefix(interfaceName, "PortChannel"):
		return "PORTCHANNEL_INTERFACE", nil
	default:
		return "", fmt.Errorf("unknown interface type for IP assignment: %s", interfaceName)
	}
}

func (db *dbAccessor) addIPAddresses(ctx context.Context, interfaceName string, prefixes []string) error {
	table, err := interfaceIPTable(interfaceName)
	if err != nil {
		return err
	}
	for _, prefix := range prefixes {
		if err := db.configDB.HSet(ctx, table+"|"+interfaceName+"|"+prefix, "NULL", "NULL").Err(); err != nil {
			return fmt.Errorf("failed to add IP %s to %s: %w", prefix, interfaceName, err)
		}
	}
	return nil
}

func (db *dbAccessor) removeIPAddresses(ctx context.Context, interfaceName string, prefixes []string) error {
	table, err := interfaceIPTable(interfaceName)
	if err != nil {
		return err
	}
	for _, prefix := range prefixes {
		if err := db.configDB.Del(ctx, table+"|"+interfaceName+"|"+prefix).Err(); err != nil {
			return fmt.Errorf("failed to remove IP %s from %s: %w", prefix, interfaceName, err)
		}
	}
	return nil
}

func (db *dbAccessor) listIPAddresses(ctx context.Context, interfaceName string) ([]string, error) {
	table, err := interfaceIPTable(interfaceName)
	if err != nil {
		return nil, err
	}
	keys, err := scanKeys(ctx, db.configDB, table+"|"+interfaceName+"|*")
	if err != nil {
		return nil, fmt.Errorf("failed to list IP addresses for %s: %w", interfaceName, err)
	}
	pfx := table + "|" + interfaceName + "|"
	addrs := make([]string, 0, len(keys))
	for _, k := range keys {
		addrs = append(addrs, strings.TrimPrefix(k, pfx))
	}
	return addrs, nil
}

func (db *dbAccessor) syncIPAddresses(ctx context.Context, interfaceName string, desired []string) error {
	current, err := db.listIPAddresses(ctx, interfaceName)
	if err != nil {
		return err
	}

	desiredSet := make(map[string]struct{}, len(desired))
	for _, p := range desired {
		desiredSet[p] = struct{}{}
	}

	var toRemove []string
	for _, p := range current {
		if _, ok := desiredSet[p]; !ok {
			toRemove = append(toRemove, p)
		}
	}
	if len(toRemove) > 0 {
		if err := db.removeIPAddresses(ctx, interfaceName, toRemove); err != nil {
			return err
		}
	}

	currentSet := make(map[string]struct{}, len(current))
	for _, p := range current {
		currentSet[p] = struct{}{}
	}
	var toAdd []string
	for _, p := range desired {
		if _, ok := currentSet[p]; !ok {
			toAdd = append(toAdd, p)
		}
	}
	if len(toAdd) > 0 {
		if err := db.addIPAddresses(ctx, interfaceName, toAdd); err != nil {
			return err
		}
	}

	return nil
}

func (db *dbAccessor) setAdminState(ctx context.Context, interfaceName string, adminStatus string) error {
	var key string
	if strings.HasPrefix(interfaceName, "Loopback") {
		key = "LOOPBACK_INTERFACE|" + interfaceName
	} else if strings.HasPrefix(interfaceName, "Vlan") {
		key = "VLAN|" + interfaceName
	} else {
		key = "PORT|" + interfaceName
	}
	if err := db.configDB.HSet(ctx, key, "admin_status", adminStatus).Err(); err != nil {
		return fmt.Errorf("failed to set admin state for %s: %w", interfaceName, err)
	}
	return nil
}

func (db *dbAccessor) ensureVLANMember(ctx context.Context, vlanName, portName, taggingMode string) error {
	if err := db.configDB.HSet(ctx, "VLAN_MEMBER|"+vlanName+"|"+portName, "tagging_mode", taggingMode).Err(); err != nil {
		return fmt.Errorf("failed to ensure VLAN member %s on %s: %w", vlanName, portName, err)
	}
	return nil
}

func (db *dbAccessor) removeVLANMember(ctx context.Context, vlanName, portName string) error {
	if err := db.configDB.Del(ctx, "VLAN_MEMBER|"+vlanName+"|"+portName).Err(); err != nil {
		return fmt.Errorf("failed to remove VLAN member %s from %s: %w", vlanName, portName, err)
	}
	return nil
}

func (db *dbAccessor) listVLANMembers(ctx context.Context, portName string) ([]string, error) {
	keys, err := scanKeys(ctx, db.configDB, "VLAN_MEMBER|*|"+portName)
	if err != nil {
		return nil, fmt.Errorf("failed to list VLAN members for %s: %w", portName, err)
	}
	vlans := make([]string, 0, len(keys))
	for _, k := range keys {
		// key format: VLAN_MEMBER|Vlan<id>|<portName>
		parts := strings.SplitN(k, "|", 3)
		if len(parts) == 3 {
			vlans = append(vlans, parts[1])
		}
	}
	return vlans, nil
}

// syncVLANMembers reconciles VLAN_MEMBER entries for portName toward desired state.
// desired maps vlanName to taggingMode; a nil map removes all memberships.
func (db *dbAccessor) syncVLANMembers(ctx context.Context, portName string, desired map[string]string) error {
	current, err := db.listVLANMembers(ctx, portName)
	if err != nil {
		return err
	}
	for _, vlan := range current {
		if _, ok := desired[vlan]; !ok {
			if err := db.removeVLANMember(ctx, vlan, portName); err != nil {
				return err
			}
		}
	}
	for vlan, taggingMode := range desired {
		if err := db.ensureVLAN(ctx, vlan); err != nil {
			return err
		}
		if err := db.ensureVLANMember(ctx, vlan, portName, taggingMode); err != nil {
			return err
		}
	}
	return nil
}

func (db *dbAccessor) vlanExists(ctx context.Context, vlanName string) (bool, error) {
	n, err := db.configDB.Exists(ctx, "VLAN|"+vlanName).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check VLAN %s: %w", vlanName, err)
	}
	return n > 0, nil
}

func (db *dbAccessor) ensureDHCPRelay(ctx context.Context, vlanName string, servers []string) error {
	if err := db.configDB.HSet(ctx, "VLAN|"+vlanName, "dhcp_servers@", strings.Join(servers, ",")).Err(); err != nil {
		return fmt.Errorf("failed to set dhcp_servers for %s: %w", vlanName, err)
	}
	return nil
}

func (db *dbAccessor) deleteDHCPRelay(ctx context.Context, vlanName string) error {
	if err := db.configDB.HDel(ctx, "VLAN|"+vlanName, "dhcp_servers@").Err(); err != nil {
		return fmt.Errorf("failed to delete dhcp_servers for %s: %w", vlanName, err)
	}
	return nil
}

func (db *dbAccessor) hasDHCPRelay(ctx context.Context, vlanName string) (bool, error) {
	result, err := db.configDB.HExists(ctx, "VLAN|"+vlanName, "dhcp_servers@").Result()
	if err != nil {
		return false, fmt.Errorf("failed to check dhcp_servers for %s: %w", vlanName, err)
	}
	return result, nil
}

// parseSupportedSpeeds converts a comma-separated Mbps string to a Gbps slice,
// falling back to a single entry derived from fallbackMbps when raw is empty.
func parseSupportedSpeeds(raw string, fallbackMbps int) []int32 {
	if raw != "" {
		var out []int32
		for _, s := range strings.Split(raw, ",") {
			if mbps, err := strconv.Atoi(strings.TrimSpace(s)); err == nil && mbps > 0 {
				out = append(out, int32(mbps/1000))
			}
		}
		return out
	}
	if fallbackMbps > 0 {
		return []int32{int32(fallbackMbps / 1000)}
	}
	return nil
}

// ── Cell provisioning state (STATE_DB) ───────────────────────────────────────

const cellStateKeyPrefix = "WIRE_CELL_STATE|"

func (db *dbAccessor) setCellState(ctx context.Context, device, state, message string) error {
	if err := db.stateDB.HSet(ctx, cellStateKeyPrefix+device, "state", state, "message", message).Err(); err != nil {
		return fmt.Errorf("failed to set cell state for %s: %w", device, err)
	}
	return nil
}

func (db *dbAccessor) getCellState(ctx context.Context, device string) (state, message string, err error) {
	fields, err := db.stateDB.HGetAll(ctx, cellStateKeyPrefix+device).Result()
	if err != nil {
		return "", "", fmt.Errorf("failed to get cell state for %s: %w", device, err)
	}
	return fields["state"], fields["message"], nil
}

func (db *dbAccessor) deleteCellState(ctx context.Context, device string) error {
	if err := db.stateDB.Del(ctx, cellStateKeyPrefix+device).Err(); err != nil {
		return fmt.Errorf("failed to delete cell state for %s: %w", device, err)
	}
	return nil
}
