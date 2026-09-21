// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package sonic

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	errors "github.com/ironcore-dev/sonic-operator/internal/agent/errors"
	"github.com/ironcore-dev/sonic-operator/internal/agent/sonic/frr"
	"github.com/ironcore-dev/sonic-operator/internal/agent/sonic/hostservices"
	agent "github.com/ironcore-dev/sonic-operator/internal/agent/types"

	"github.com/redis/go-redis/v9"
	"github.com/vishvananda/netlink"
)

const (
	RedisDialTimeout     = 30 * time.Second
	RedisReadTimeout     = 5 * time.Second
	RedisWriteTimeout    = 5 * time.Second
	RedisPoolTimeout     = 10 * time.Second
	RedisMaxRetries      = 10
	RedisMinRetryBackoff = 500 * time.Millisecond
	RedisMaxRetryBackoff = 10 * time.Second
	RedisDefaultTimeout  = 5 * time.Second
)
const (
	ProfileEdgecore    = "edgecore"
	ProfileDefault     = "default"
	defaultFRRConfPath = "/etc/sonic/frr/frr.conf"
)

type SonicAgent struct {
	redisAddr       string
	clientPool      map[string]*redis.Client
	poolMutex       sync.RWMutex
	OSVersion       string
	OSProfile       string
	dbusClient      hostservices.DbusClient
	frrConfigPath   string
	writeFRRConfig  func(path, content string) error
	restartService  func(ctx context.Context, name string) error
	reprovisionDone atomic.Bool
	reprovisionErr  atomic.Pointer[agent.Status]
}

func getRedisDBIDByName(name string) int {
	switch name {
	case "APPL_DB":
		return 0
	case "ASIC_DB":
		return 1
	case "COUNTERS_DB":
		return 2
	case "LOGLEVEL_DB":
		return 3
	case "CONFIG_DB":
		return 4
	case "PFC_WD_DB":
		return 5
	case "FLEX_COUNTER_DB":
		return 5
	case "STATE_DB":
		return 6
	case "SNMP_OVERLAY_DB":
		return 7
	case "RESTagent_DB":
		return 8
	case "GB_ASIC_DB":
		return 9
	case "GB_COUNTERS_DB":
		return 10
	case "GB_FLEX_COUNTER_DB":
		return 11
	case "APPL_STATE_DB":
		return 14
	default:
		return -1
	}
}

func NewSonicRedisAgent(redisAddr string) (*SonicAgent, error) {
	// Test connection first
	testClient := redis.NewClient(&redis.Options{
		Addr:             redisAddr,
		DB:               4, // Test with CONFIG_DB
		DialTimeout:      RedisDialTimeout,
		ReadTimeout:      RedisReadTimeout,
		WriteTimeout:     RedisWriteTimeout,
		PoolTimeout:      RedisPoolTimeout,
		MaxRetries:       RedisMaxRetries,
		MinRetryBackoff:  RedisMinRetryBackoff,
		MaxRetryBackoff:  RedisMaxRetryBackoff,
		DisableIndentity: true, // Disable identity/protocol checks to avoid warnings
	})

	if err := testClient.Ping(context.Background()).Err(); err != nil {
		if err := testClient.Close(); err != nil {
			return nil, fmt.Errorf("failed to close Redis client: %w", err)
		}
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}
	if err := testClient.Close(); err != nil {
		return nil, fmt.Errorf("failed to close Redis client: %w", err)
	}

	var osVersion string
	var osProfile string
	if versionInfo, err := GetSonicVersionInfo(); err == nil {
		if buildVersion, exists := versionInfo["build_version"]; exists {
			osVersion = buildVersion

			if strings.Contains(osVersion, "Edgecore-SONiC") {
				osProfile = ProfileEdgecore
			} else {
				osProfile = ProfileDefault
			}
		}
	} else {
		log.Printf("Failed to get SONiC version info: %v", err)
	}

	if osProfile != "" {
		log.Printf("Detected SONiC OS version: %s, profile: %s", osVersion, osProfile)
	}

	sa := &SonicAgent{
		redisAddr:     redisAddr,
		OSVersion:     osVersion,
		OSProfile:     osProfile,
		clientPool:    make(map[string]*redis.Client),
		poolMutex:     sync.RWMutex{},
		frrConfigPath: defaultFRRConfPath,
		writeFRRConfig: func(path, content string) error {
			if info, err := os.Stat(path); err == nil && info.IsDir() {
				if err := os.Remove(path); err != nil {
					return fmt.Errorf("removing stale directory at %s: %w", path, err)
				}
			}
			if err := os.WriteFile(path, []byte(content), 0644); err != nil {
				return err
			}
			if _, err := os.Stat(path); err != nil {
				return fmt.Errorf("file not found after write at %s: %w", path, err)
			}
			return nil
		},
		restartService: func(ctx context.Context, name string) error {
			// placeholder; real implementation set after dbusClient is wired below
			return nil
		},
	}

	if osProfile != "" {
		dbusClient, err := hostservices.NewSystemDbusClient()
		if err != nil {
			log.Printf("Failed to create D-Bus client: %v", err)
		} else {
			sa.dbusClient = dbusClient
			sa.restartService = func(ctx context.Context, name string) error {
				if status := sa.RestartSystemdService(ctx, name); status != nil {
					return fmt.Errorf("%s", status.Message)
				}
				return nil
			}

			go func() {
				rebootAttempts := 0
				reprovisionAttempted := false
				for {
					// Wait for sonic-hostservice to be running before probing.
					// After a reboot the agent starts faster than sonic-hostservice,
					// so without this gate we'd incorrectly install modules and trigger
					// a reboot/restart while the service is still coming up.
					if !sa.dbusClient.ServiceAvailable(context.Background(), "org.SONiC.HostService") {
						log.Printf("Waiting for sonic-hostservice (%s) to become available on D-Bus...", "org.SONiC.HostService")
						time.Sleep(15 * time.Second)
						continue
					}

					compatible, errs := hostservices.HostServicesCompatibilityCheck(context.Background(), sa.dbusClient, osProfile)
					if len(errs) > 0 {
						errMsgs := make([]string, len(errs))
						for i, e := range errs {
							errMsgs[i] = e.Error()
						}
						log.Printf("Error checking HostServices compatibility for profile %s:\n%s", osProfile, strings.Join(errMsgs, "\n"))

						var modulesToInstall []hostservices.Handler
						for _, h := range hostservices.Modules[osProfile].Handlers {
							if h.Builtin {
								continue
							}
							modulesToInstall = append(modulesToInstall, h)
						}

						for _, v := range modulesToInstall {
							log.Println("Module to install: " + v.Name)
						}
						for _, h := range modulesToInstall {
							if err := hostservices.InstallHostServiceModule(sa.dbusClient, osProfile, h.Name); err != nil {
								log.Printf("Error installing HostService module %q: %v", h.Name, err)
							}
						}

						if osProfile == ProfileEdgecore {
							RestartSystemdService(context.Background(), sa.dbusClient, osProfile, "sonic-hostservice")
						} else if rebootAttempts == 0 {
							rebootAttempts++
							// Latest sonic-hostservices repository's systemd module has Allowlist which blocks
							// restarting of all services. To work around this, we reboot the system.
							// another possible solution could be ssh to host if we share the same network with host, and just restart the service.
							sa.Reboot(context.Background())
						} else {
							log.Printf("Compatibility fix already attempted via reboot; not rebooting again to avoid a reboot loop. Manual intervention may be required.")
						}
						time.Sleep(15 * time.Second)
					} else {
						log.Printf("HostServices compatibility for profile %s: %s", osProfile, compatible)
						if !reprovisionAttempted {
							reprovisionAttempted = true
							log.Printf("Triggering reprovision after hostServices became available")
							if st := sa.Reprovision(context.Background()); st != nil {
								log.Printf("Reprovision failed: %s", st.Message)
								sa.reprovisionErr.Store(st)
							} else {
								sa.reprovisionDone.Store(true)
							}
						}
						time.Sleep(1 * time.Minute)
					}
				}
			}()
		}
	}

	return sa, nil
}

func (m *SonicAgent) Connect(dbName string) (*redis.Client, error) {
	m.poolMutex.RLock()
	if client, exists := m.clientPool[dbName]; exists {
		m.poolMutex.RUnlock()

		// Test if connection is still alive
		if err := client.Ping(context.Background()).Err(); err == nil {
			return client, nil
		}
	} else {
		m.poolMutex.RUnlock()
	}

	// Need to create new client (write lock)
	m.poolMutex.Lock()
	defer m.poolMutex.Unlock()

	// Double-check in case another goroutine created it
	if client, exists := m.clientPool[dbName]; exists {
		if err := client.Ping(context.Background()).Err(); err == nil {
			return client, nil
		}
		// Close the dead connection
		if err := client.Close(); err != nil {
			return nil, fmt.Errorf("failed to close Redis client: %w", err)
		}
		delete(m.clientPool, dbName)
	}

	// Create new client
	dbID := getRedisDBIDByName(dbName)
	if dbID == -1 {
		return nil, fmt.Errorf("unknown database name: %s", dbName)
	}

	client := redis.NewClient(&redis.Options{
		Addr:         m.redisAddr,
		DB:           dbID,
		DialTimeout:  RedisDialTimeout,
		ReadTimeout:  RedisReadTimeout,
		WriteTimeout: RedisWriteTimeout,
		PoolTimeout:  RedisPoolTimeout,
		MaxRetries:   RedisMaxRetries,

		// Connection pool settings
		PoolSize:     10, // Maximum number of socket connections
		MinIdleConns: 2,  // Minimum idle connections
		MaxIdleConns: 5,  // Maximum idle connections

		// Connection lifecycle
		ConnMaxIdleTime: 30 * time.Minute,
		ConnMaxLifetime: 1 * time.Hour,

		DisableIndentity: true, // Disable identity/protocol checks to avoid warnings
	})

	// Test the new connection
	if err := client.Ping(context.Background()).Err(); err != nil {
		if err := client.Close(); err != nil {
			return nil, fmt.Errorf("failed to close Redis client: %w", err)
		}
		return nil, fmt.Errorf("failed to connect to Redis database %s: %w", dbName, err)
	}

	m.clientPool[dbName] = client

	return client, nil
}

func (m *SonicAgent) GetDeviceInfo(ctx context.Context) (*agent.SwitchDevice, *agent.Status) {
	rdb, err := m.Connect("CONFIG_DB")
	if err != nil {
		return nil, errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to connect to Redis: %v", err))
	}

	const deviceKey = "DEVICE_METADATA|localhost"
	fields, err := rdb.HGetAll(ctx, deviceKey).Result()
	if err != nil {
		return nil, errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to get device info: %v", err))
	}

	mac, ok := fields["mac"]
	if !ok {
		return nil, errors.NewErrorStatus(errors.NOT_FOUND, "missing or invalid MAC address")
	}

	hwsku := fields["hwsku"]
	sonicOSVersion := fields["sonic_os_version"]
	asicType := fields["asic_type"]

	// If values are missing from Redis, try to get from sonic_version.yml
	if hwsku == "" || sonicOSVersion == "" || asicType == "" {
		if versionInfo, err := GetSonicVersionInfo(); err == nil {
			if hwsku == "" {
				hwsku = versionInfo["hwsku"]
			}
			if sonicOSVersion == "" {
				sonicOSVersion = versionInfo["sonic_os_version"]
			}
			if asicType == "" {
				asicType = versionInfo["asic_type"]
			}
		}
	}

	return &agent.SwitchDevice{
		TypeMeta: agent.TypeMeta{
			Kind: agent.DeviceKind,
		},
		LocalMacAddress: mac,
		Hwsku:           hwsku,
		SonicOSVersion:  sonicOSVersion,
		AsicType:        asicType,
		Readiness:       uint32(agent.StatusReady),
	}, nil
}

func (m *SonicAgent) ListInterfaces(ctx context.Context) (*agent.InterfaceList, *agent.Status) {
	configDB, err := m.Connect("CONFIG_DB")
	if err != nil {
		return nil, errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to connect to CONFIG_DB: %v", err))
	}

	// Connect to STATE_DB for operational status
	stateDB, err := m.Connect("STATE_DB")
	if err != nil {
		return nil, errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to connect to STATE_DB: %v", err))
	}
	// defer stateDB.Close()

	applDB, err := m.Connect("APPL_DB")
	if err != nil {
		return nil, errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to connect to APPL_DB: %v", err))
	}

	pattern := "PORT|*"
	keys, err := configDB.Keys(ctx, pattern).Result()

	if err != nil {
		return nil, errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to obtain iface keys: %v", err))
	}

	interfaces := make([]agent.Interface, 0, len(keys))
	for _, key := range keys {
		var name string
		if _, err := fmt.Sscanf(key, "PORT|%s", &name); err != nil {
			return nil, errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to parse interface name from key %s: %v", key, err))
		}

		// Get operational status from STATE_DB
		stateKey := fmt.Sprintf("PORT_TABLE|%s", name)
		stateFields, err := stateDB.HGetAll(ctx, stateKey).Result()
		if err != nil {
			// If state info is not available, use default values
			stateFields = make(map[string]string)
		}
		applKey := fmt.Sprintf("PORT_TABLE:%s", name)
		applFields, err := applDB.HGetAll(ctx, applKey).Result()
		if err != nil {
			return nil, errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to get state info for interface %s: %v", name, err))
		}

		// Determine operational status
		operStatus := agent.StatusDown
		if applFields["oper_status"] == "up" {
			operStatus = agent.StatusUp
		}

		adminStatus := agent.StatusDown
		if stateFields["admin_status"] == "up" {
			adminStatus = agent.StatusUp
		}

		// Use device MAC as interface MAC (common in SONiC)
		link, err := netlink.LinkByName(name)
		if err != nil {
			return nil, agent.NewErrorStatus(errors.NOT_FOUND, fmt.Sprintf("failed to get interface %s: %v", name, err))
		}

		mac := link.Attrs().HardwareAddr
		if mac == nil {
			return nil, agent.NewErrorStatus(errors.NOT_FOUND, fmt.Sprintf("no MAC address found for interface %s", name))
		}

		abstractName, err := agent.NativeNameToAbstractName(name)
		if err != nil {
			return nil, agent.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to convert native name to abstract name: %v", err))
		}

		alias, err := configDB.HGet(ctx, fmt.Sprintf("PORT|%s", name), "alias").Result()
		if err != nil {
			return nil, errors.NewErrorStatus(errors.REDIS_KEY_CHECK_FAIL, fmt.Sprintf("failed to get alias: %v", err))
		}

		iface := agent.Interface{
			TypeMeta: agent.TypeMeta{
				Kind: agent.InterfaceKind,
			},
			Name:            abstractName,
			NativeName:      name,
			AliasName:       alias,
			MacAddress:      mac.String(),
			OperationStatus: operStatus,
			AdminStatus:     adminStatus,
		}
		interfaces = append(interfaces, iface)
	}

	return &agent.InterfaceList{
		TypeMeta: agent.TypeMeta{
			Kind: agent.InterfaceListKind,
		},
		Items:  interfaces,
		Status: agent.Status{Code: 0, Message: "ok"},
	}, nil
}

func (m *SonicAgent) ListInterfacesV2(ctx context.Context) ([]string, *agent.Status) {
	configDB, err := m.Connect("CONFIG_DB")
	if err != nil {
		return nil, errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to connect to CONFIG_DB: %v", err))
	}
	keys, err := scanKeys(ctx, configDB, "PORT|*")
	if err != nil {
		return nil, errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to obtain iface keys: %v", err))
	}
	names := make([]string, 0, len(keys))
	for _, key := range keys {
		var name string
		if _, err := fmt.Sscanf(key, "PORT|%s", &name); err != nil {
			continue
		}
		names = append(names, name)
	}
	return names, nil
}

func findHandler(profile, name string) (*hostservices.Handler, *agent.Status) {
	for _, h := range hostservices.Modules[profile].Handlers {
		if h.Name == name {
			return &h, nil
		}
	}
	return nil, errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("no handler found for %s in profile %s", name, profile))
}

func (m *SonicAgent) SaveConfig(ctx context.Context) *agent.Status {
	if m.dbusClient == nil {
		return errors.NewErrorStatus(errors.BAD_REQUEST, "D-Bus client not available")
	}
	handler, status := findHandler(m.OSProfile, "save_config")
	if status != nil {
		return status
	}

	var arg *hostservices.Arg
	for _, _arg := range handler.Args {
		if _arg.Name == "default_config" {
			arg = &_arg
		}
	}
	if arg == nil {
		return errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("argument 'default_config' not found for handler %s", handler.Name))
	}

	if err := m.dbusClient.CallHandler(ctx, *handler, arg.Value); err != nil {
		log.Printf("D-Bus call failed: %v", err)
		return errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to save config via D-Bus: %v", err))
	}

	log.Printf("Config saved successfully via D-Bus")
	return nil
}

func (m *SonicAgent) OnieBootModeInstall(ctx context.Context) *agent.Status {
	if m.dbusClient == nil {
		return errors.NewErrorStatus(errors.BAD_REQUEST, "D-Bus client not available")
	}
	handler, status := findHandler(m.OSProfile, "onie")
	if status != nil {
		return status
	}

	if err := m.dbusClient.CallHandler(ctx, *handler); err != nil {
		log.Printf("D-Bus call failed: %v", err)
		return errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to set ONIE boot mode to install: %v", err))
	}

	log.Printf("ONIE boot mode set to install successfully via D-Bus")
	return nil
}

func (m *SonicAgent) Reboot(ctx context.Context) *agent.Status {
	if m.dbusClient == nil {
		return errors.NewErrorStatus(errors.BAD_REQUEST, "D-Bus client not available")
	}
	handler, status := findHandler(m.OSProfile, "reboot")
	if status != nil {
		return status
	}

	if err := m.dbusClient.CallHandler(ctx, *handler, []string{`{"method":"COLD"}`}); err != nil {
		log.Printf("D-Bus call failed: %v", err)
		return errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to reboot via D-Bus: %v", err))
	}

	log.Printf("Reboot command sent successfully via D-Bus")
	return nil
}

func (m *SonicAgent) SetInterfaceAdminStatus(ctx context.Context, iface *agent.Interface) (*agent.Interface, *agent.Status) {
	// Validate input
	var ifaceName string
	var err error

	if iface == nil || iface.Name == "" {
		return nil, errors.NewErrorStatus(errors.BAD_REQUEST, "interface name cannot be empty")
	}
	if !strings.HasPrefix(iface.Name, "Ethernet") && !strings.HasPrefix(iface.Name, "eth") {
		return nil, errors.NewErrorStatus(errors.BAD_REQUEST, "invalid interface name. Must start with 'Ethernet' or 'eth'")
	}
	if strings.HasPrefix(iface.Name, "eth") {
		ifaceName, err = agent.AbstractNameToNativeName(iface.Name)
		if err != nil {
			return nil, errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to convert abstract name to native name: %v", err))
		}
	} else {
		ifaceName = iface.Name
	}

	configDB, err := m.Connect("CONFIG_DB")
	if err != nil {
		return nil, errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to connect to CONFIG_DB: %v", err))
	}

	portKey := fmt.Sprintf("PORT|%s", ifaceName)

	// store the current admin status for rollback
	fields, err := configDB.HGetAll(ctx, portKey).Result()
	if err != nil {
		return nil, errors.NewErrorStatus(errors.REDIS_KEY_CHECK_FAIL, fmt.Sprintf("failed to get current admin status: %v", err))
	}
	currentAdminStatus := fields["admin_status"]

	// Set admin status in CONFIG_DB
	adminStatusStr := string(iface.AdminStatus)
	err = configDB.HSet(ctx, portKey, "admin_status", adminStatusStr).Err()
	if err != nil {
		return nil, errors.NewErrorStatus(errors.REDIS_HSET_FAIL, fmt.Sprintf("failed to set admin status: %v", err))
	}
	// Persist changes to config_db.json
	if status := m.SaveConfig(ctx); status != nil {
		// Try to rollback if save fails
		_ = configDB.HSet(ctx, portKey, "admin_status", currentAdminStatus).Err()
		return nil, status
	}

	// Verify the interface exists by checking if we can get its current state
	exists, err := configDB.Exists(ctx, portKey).Result()
	if err != nil {
		return nil, errors.NewErrorStatus(errors.REDIS_KEY_CHECK_FAIL, fmt.Sprintf("failed to verify interface existence: %v", err))
	}
	if exists == 0 {
		return nil, errors.NewErrorStatus(errors.NOT_FOUND, fmt.Sprintf("interface %s not found", ifaceName))
	}

	time.Sleep(1000 * time.Millisecond)

	// Get updated interface status from STATE_DB
	stateDB, err := m.Connect("STATE_DB")
	if err != nil {
		return nil, errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to connect to STATE_DB: %v", err))
	}

	stateKey := fmt.Sprintf("PORT_TABLE|%s", ifaceName)
	stateFields, err := stateDB.HGetAll(ctx, stateKey).Result()
	_ = stateFields // currently we don't use any field from stateFields, but we get it anyway to check if the interface is still there after the update. If the key is gone, it means the interface is deleted during the update, we can return not found error in that case.
	if err != nil {
		// rollback admin status
		err = configDB.HSet(ctx, portKey, "admin_status", currentAdminStatus).Err()
		if err != nil {
			return nil, errors.NewErrorStatus(errors.REDIS_HSET_FAIL, fmt.Sprintf("failed to rollback admin status: %v", err))
		}
		return nil, errors.NewErrorStatus(errors.REDIS_KEY_CHECK_FAIL, fmt.Sprintf("failed to get state info: %v", err))
	}

	applDB, err := m.Connect("APPL_DB")
	if err != nil {
		return nil, errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to connect to APPL_DB: %v", err))
	}
	// get the newest operational status
	applKey := fmt.Sprintf("PORT_TABLE:%s", ifaceName)
	applFields, err := applDB.HGetAll(ctx, applKey).Result()
	if err != nil {
		// If state info is not available, use default values
		applFields = make(map[string]string)
	}

	// Determine operational status
	operStatus := agent.StatusDown
	if applFields["oper_status"] == "up" {
		operStatus = agent.StatusUp
	}

	alias, err := configDB.HGet(ctx, fmt.Sprintf("PORT|%s", ifaceName), "alias").Result()
	if err != nil {
		return nil, errors.NewErrorStatus(errors.REDIS_KEY_CHECK_FAIL, fmt.Sprintf("failed to get alias: %v", err))
	}

	abstractName, _ := agent.NativeNameToAbstractName(ifaceName)
	resultInterface := &agent.Interface{
		TypeMeta: agent.TypeMeta{
			Kind: agent.InterfaceKind,
		},
		Name:            abstractName,
		NativeName:      ifaceName,
		AliasName:       alias, // In SONiC, abstract name is the same as native name for physical interfaces
		MacAddress:      "",
		OperationStatus: operStatus,
		AdminStatus:     iface.AdminStatus,
		Status:          agent.Status{Code: 0, Message: "ok"},
	}
	return resultInterface, nil
}

func (m *SonicAgent) GetInterface(ctx context.Context, iface *agent.Interface) (*agent.Interface, *agent.Status) {
	// Validate input
	var ifaceName string
	var err error

	if iface == nil || iface.Name == "" {
		return nil, errors.NewErrorStatus(errors.BAD_REQUEST, "interface name cannot be empty")
	}
	if !strings.HasPrefix(iface.Name, "Ethernet") && !strings.HasPrefix(iface.Name, "eth") {
		return nil, errors.NewErrorStatus(errors.BAD_REQUEST, "invalid interface name. Must start with 'Ethernet' or 'eth'")
	}
	if strings.HasPrefix(iface.Name, "eth") {
		ifaceName, err = agent.AbstractNameToNativeName(iface.Name)
		if err != nil {
			return nil, errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to convert abstract name to native name: %v", err))
		}
	} else {
		ifaceName = iface.Name
	}

	configDB, err := m.Connect("CONFIG_DB")
	if err != nil {
		return nil, errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to connect to CONFIG_DB: %v", err))
	}

	// Connect to STATE_DB for operational status
	stateDB, err := m.Connect("STATE_DB")
	if err != nil {
		return nil, errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to connect to STATE_DB: %v", err))
	}

	// Check if interface exists in CONFIG_DB
	portKey := fmt.Sprintf("PORT|%s", ifaceName)
	exists, err := configDB.Exists(ctx, portKey).Result()
	if err != nil {
		return nil, errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to check interface existence: %v", err))
	}
	if exists == 0 {
		return nil, errors.NewErrorStatus(errors.NOT_FOUND, fmt.Sprintf("interface %s not found", ifaceName))
	}

	// Get operational status from STATE_DB
	stateKey := fmt.Sprintf("PORT_TABLE|%s", ifaceName)
	stateFields, err := stateDB.HGetAll(ctx, stateKey).Result()
	if err != nil {
		// If state info is not available, use default values
		stateFields = make(map[string]string)
	}
	applDB, err := m.Connect("APPL_DB")
	if err != nil {
		return nil, errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to connect to APPL_DB: %v", err))
	}
	applKey := fmt.Sprintf("PORT_TABLE:%s", ifaceName)
	applFields, err := applDB.HGetAll(ctx, applKey).Result()
	if err != nil {
		// If state info is not available, use default values
		applFields = make(map[string]string)
	}

	// Determine operational status
	operStatus := agent.StatusDown
	if applFields["oper_status"] == "up" {
		operStatus = agent.StatusUp
	}

	adminStatus := agent.StatusDown
	if stateFields["admin_status"] == "up" {
		adminStatus = agent.StatusUp
	}

	// Get interface MAC address using netlink
	link, err := netlink.LinkByName(ifaceName)
	if err != nil {
		return nil, errors.NewErrorStatus(errors.NOT_FOUND, fmt.Sprintf("failed to get interface %s: %v", ifaceName, err))
	}

	mac := link.Attrs().HardwareAddr
	if mac == nil {
		return nil, errors.NewErrorStatus(errors.NOT_FOUND, fmt.Sprintf("no MAC address found for interface %s", ifaceName))
	}

	alias, err := configDB.HGet(ctx, fmt.Sprintf("PORT|%s", ifaceName), "alias").Result()
	if err != nil {
		return nil, errors.NewErrorStatus(errors.REDIS_KEY_CHECK_FAIL, fmt.Sprintf("failed to get alias: %v", err))
	}

	abstractName, err := agent.NativeNameToAbstractName(ifaceName)
	if err != nil {
		return nil, errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to convert native name to abstract name: %v", err))
	}

	resultInterface := &agent.Interface{
		TypeMeta: agent.TypeMeta{
			Kind: agent.InterfaceKind,
		},
		Name:            abstractName,
		NativeName:      ifaceName,
		AliasName:       alias, // In SONiC, abstract name is the same as native name for physical interfaces
		MacAddress:      mac.String(),
		OperationStatus: operStatus,
		AdminStatus:     adminStatus,
		Status:          agent.Status{Code: 0, Message: "ok"},
	}

	return resultInterface, nil
}

func (m *SonicAgent) GetInterfaceNeighbor(ctx context.Context, iface *agent.Interface) (*agent.InterfaceNeighbor, *agent.Status) {
	var ifaceName string
	var err error

	if iface == nil || iface.Name == "" {
		return nil, errors.NewErrorStatus(errors.BAD_REQUEST, "interface name cannot be empty")
	}
	if !strings.HasPrefix(iface.Name, "Ethernet") && !strings.HasPrefix(iface.Name, "eth") {
		return nil, errors.NewErrorStatus(errors.BAD_REQUEST, "invalid interface name. Must start with 'Ethernet' or 'eth'")
	}
	if strings.HasPrefix(iface.Name, "eth") {
		ifaceName, err = agent.AbstractNameToNativeName(iface.Name)
		if err != nil {
			return nil, errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to convert abstract name to native name: %v", err))
		}
	} else {
		ifaceName = iface.Name
	}

	applDB, err := m.Connect("APPL_DB")
	if err != nil {
		return nil, errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to connect to APPL_DB: %v", err))
	}

	lldpKey := fmt.Sprintf("LLDP_ENTRY_TABLE:%s", ifaceName)

	// Check if LLDP entry exists for this interface
	exists, err := applDB.Exists(ctx, lldpKey).Result()
	if err != nil {
		return nil, errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to check LLDP entry existence: %v", err))
	}
	if exists == 0 {
		return nil, errors.NewErrorStatus(errors.NOT_FOUND, fmt.Sprintf("no LLDP neighbor found for interface %s", ifaceName))
	}

	// Get all LLDP fields
	lldpFields, err := applDB.HGetAll(ctx, lldpKey).Result()
	if err != nil {
		return nil, errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to get LLDP entry: %v", err))
	}

	// MacAddress from lldp_rem_chassis_id (when chassis_id_subtype is 4 - MAC address)
	macAddress := lldpFields["lldp_rem_chassis_id"]

	// SystemName from lldp_rem_sys_name
	systemName := lldpFields["lldp_rem_sys_name"]

	// Handle (remote interface name) from lldp_rem_port_desc
	// Note: lldp_rem_port_id contains "Eth5(Port5)" format, lldp_rem_port_desc contains "Ethernet16"
	handle := lldpFields["lldp_rem_port_desc"]
	if handle == "" {
		// Fallback to lldp_rem_port_id if port_desc is not available
		handle = lldpFields["lldp_rem_port_id"]
	} else {
		handle, err = agent.NativeNameToAbstractName(handle)
		if err != nil {
			return nil, errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to convert native name to abstract name: %v", err))
		}
	}

	// Validate that we have the essential information
	if macAddress == "" || systemName == "" {
		return nil, errors.NewErrorStatus(errors.NOT_FOUND, fmt.Sprintf("incomplete LLDP information for interface %s", ifaceName))
	}

	neighbor := &agent.InterfaceNeighbor{
		TypeMeta: agent.TypeMeta{
			Kind: agent.InterfaceNeighborKind,
		},
		Name:       ifaceName, // Interface name of yourself
		MacAddress: macAddress,
		SystemName: systemName,
		Handle:     handle, // Remote interface name
		Status:     agent.Status{Code: 0, Message: "ok"},
	}

	return neighbor, nil
}

func (m *SonicAgent) ListPorts(ctx context.Context) (*agent.PortList, *agent.Status) {
	// Connect to APPL_DB (table 0)
	applDB, err := m.Connect("APPL_DB")
	if err != nil {
		return nil, errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to connect to APPL_DB: %v", err))
	}

	// List keys starting with PORT_TABLE
	pattern := "PORT_TABLE:*"
	keys, err := applDB.Keys(ctx, pattern).Result()
	if err != nil {
		return nil, errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to obtain PORT_TABLE keys: %v", err))
	}

	ports := make([]agent.Port, 0)
	for _, key := range keys {
		var portName string
		if _, err := fmt.Sscanf(key, "PORT_TABLE:%s", &portName); err != nil {
			continue // Skip malformed keys
		}

		// Get the port configuration
		fields, err := applDB.HGetAll(ctx, key).Result()
		if err != nil {
			continue // Skip if we can't get the fields
		}

		// Check if this represents a physical port by examining the "parent_port" field
		// If parent_port equals the port name itself, it's a physical port
		parentPort, exists := fields["parent_port"]
		if !exists || parentPort != portName {
			continue // Skip non-physical ports (sub-interfaces, VLANs, etc.)
		}

		// Get alias if available
		alias := fields["alias"]
		if alias == "" {
			alias = portName // Use port name as alias if not specified
		}

		port := agent.Port{
			TypeMeta: agent.TypeMeta{
				Kind: agent.PortKind,
			},
			Name:   portName,
			Alias:  alias,
			Status: agent.Status{Code: 0, Message: "ok"},
		}
		ports = append(ports, port)
	}

	return &agent.PortList{
		TypeMeta: agent.TypeMeta{
			Kind: agent.PortListKind,
		},
		Items:  ports,
		Status: agent.Status{Code: 0, Message: "ok"},
	}, nil
}

// speedMbpsToType converts a port speed in Mbps to a short type string (e.g. 40000 → "40g").
func speedMbpsToType(mbps int) string {
	switch {
	case mbps >= 400000:
		return "400g"
	case mbps >= 200000:
		return "200g"
	case mbps >= 100000:
		return "100g"
	case mbps >= 40000:
		return "40g"
	case mbps >= 25000:
		return "25g"
	case mbps >= 10000:
		return "10g"
	case mbps >= 1000:
		return "1g"
	default:
		return ""
	}
}

func (m *SonicAgent) ListPortsV2(ctx context.Context) (*agent.PortDetailsList, *agent.Status) {
	db, err := m.newDBAccessor()
	if err != nil {
		return nil, errors.NewErrorStatus(errors.BAD_REQUEST, err.Error())
	}

	names, err := db.listPortNames(ctx)
	if err != nil {
		return nil, errors.NewErrorStatus(errors.BAD_REQUEST, err.Error())
	}

	details := make([]agent.PortDetails, 0, len(names))
	for _, portName := range names {
		cfg, _ := db.getPortConfig(ctx, portName)
		state := db.getPortState(ctx, portName)
		info := db.getTransceiverInfo(ctx, portName)

		abstractName, err := agent.NativeNameToAbstractName(portName)
		if err != nil {
			abstractName = portName
		}

		details = append(details, agent.PortDetails{
			ID:                  abstractName,
			Type:                speedMbpsToType(cfg.Speed),
			SupportedSpeedsGbps: parseSupportedSpeeds(state.SupportedSpeeds, cfg.Speed),
			Transceiver:         info.Type,
			Status:              agent.Status{Code: 0, Message: "ok"},
		})
	}

	return &agent.PortDetailsList{
		Items:  details,
		Status: agent.Status{Code: 0, Message: "ok"},
	}, nil
}

func (m *SonicAgent) SetInterfaceAliasName(ctx context.Context, iface *agent.Interface) (*agent.Interface, *agent.Status) {
	// Validate input
	var ifaceName string
	var err error

	if iface == nil || iface.Name == "" {
		return nil, errors.NewErrorStatus(errors.BAD_REQUEST, "interface name cannot be empty")
	}
	if !strings.HasPrefix(iface.Name, "Ethernet") && !strings.HasPrefix(iface.Name, "eth") {
		return nil, errors.NewErrorStatus(errors.BAD_REQUEST, "invalid interface name. Must start with 'Ethernet' or 'eth'")
	}
	if strings.HasPrefix(iface.Name, "eth") {
		ifaceName, err = agent.AbstractNameToNativeName(iface.Name)
		if err != nil {
			return nil, errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to convert abstract name to native name: %v", err))
		}
	} else {
		ifaceName = iface.Name
	}

	configDB, err := m.Connect("CONFIG_DB")
	if err != nil {
		return nil, errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to connect to CONFIG_DB: %v", err))
	}

	portKey := fmt.Sprintf("PORT|%s", ifaceName)
	log.Printf("Setting alias for port: %s", portKey)

	// store the current s Alias name for rollback
	fields, err := configDB.HGetAll(ctx, portKey).Result()
	if err != nil {
		return nil, errors.NewErrorStatus(errors.REDIS_KEY_CHECK_FAIL, fmt.Sprintf("failed to get current alias name: %v", err))
	}
	currentAlias := fields["alias"]
	futureAlias := iface.AliasName
	if futureAlias == "" {
		futureAlias = iface.Name // If alias is empty, use abstract name as alias
	}

	aliasStr := futureAlias
	err = configDB.HSet(ctx, portKey, "alias", aliasStr).Err()
	if err != nil {
		return nil, errors.NewErrorStatus(errors.REDIS_HSET_FAIL, fmt.Sprintf("failed to set alias name: %v", err))
	}
	// Persist changes to config_db.json
	if status := m.SaveConfig(ctx); status != nil {
		log.Printf("Failed to save config after setting alias name: %v", status)
		// Try to rollback if save fails
		err = configDB.HSet(ctx, portKey, "alias", currentAlias).Err()
		if err != nil {
			return nil, errors.NewErrorStatus(errors.REDIS_HSET_FAIL, fmt.Sprintf("failed to rollback alias name: %v", err))
		}
		return nil, status
	}

	// Verify the interface exists by checking if we can get its current state
	exists, err := configDB.Exists(ctx, portKey).Result()
	if err != nil {
		return nil, errors.NewErrorStatus(errors.REDIS_KEY_CHECK_FAIL, fmt.Sprintf("failed to verify interface existence: %v", err))
	}
	if exists == 0 {
		return nil, errors.NewErrorStatus(errors.NOT_FOUND, fmt.Sprintf("interface %s not found", iface.Name))
	}

	applDB, err := m.Connect("APPL_DB")
	if err != nil {
		return nil, errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to connect to APPL_DB: %v", err))
	}
	applKey := fmt.Sprintf("PORT_TABLE:%s", ifaceName)
	applFields, err := applDB.HGetAll(ctx, applKey).Result()
	if err != nil {
		// If state info is not available, use default values
		applFields = make(map[string]string)
	}

	// Determine operational status
	operStatus := agent.StatusDown
	if applFields["oper_status"] == "up" {
		operStatus = agent.StatusUp
	}

	// Return updated interface
	updatedIface := *iface
	updatedIface.OperationStatus = operStatus

	return &updatedIface, nil
}

func RestartSystemdService(ctx context.Context, client hostservices.DbusClient, profile string, systemdServiceName string) *agent.Status {
	handler, status := findHandler(profile, "systemd_restart")
	if status != nil {
		return status
	}

	if err := client.CallHandler(ctx, *handler, systemdServiceName); err != nil {
		log.Printf("D-Bus call failed: %v", err)
		return errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to restart systemd service %s via D-Bus: %v", systemdServiceName, err))
	}
	log.Printf("Systemd service %s restarted successfully via D-Bus", systemdServiceName)
	return nil
}

func (m *SonicAgent) RestartSystemdService(ctx context.Context, Service string) *agent.Status {
	if m.dbusClient == nil {
		return errors.NewErrorStatus(errors.BAD_REQUEST, "D-Bus client not available")
	}
	return RestartSystemdService(ctx, m.dbusClient, m.OSProfile, Service)
}

func (m *SonicAgent) RebootCause(ctx context.Context) (string, *agent.Status) {
	if m.dbusClient == nil {
		return "", errors.NewErrorStatus(errors.BAD_REQUEST, "D-Bus client not available")
	}
	handler, status := findHandler(m.OSProfile, "reboot_cause")
	if status != nil {
		return "", status
	}

	cause, err := m.dbusClient.CallHandlerWithOutput(ctx, *handler)
	if err != nil {
		log.Printf("D-Bus call failed: %v", err)
		return "", errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to get reboot cause via D-Bus: %v", err))
	}

	log.Printf("Reboot cause retrieved via D-Bus: %s", cause)
	return cause, nil
}

func (m *SonicAgent) FactoryReset(ctx context.Context) *agent.Status {
	if m.dbusClient == nil {
		return errors.NewErrorStatus(errors.BAD_REQUEST, "D-Bus client not available")
	}
	handler, status := findHandler(m.OSProfile, "factory_reset")
	if status != nil {
		return status
	}

	if err := m.dbusClient.CallHandler(ctx, *handler); err != nil {
		log.Printf("D-Bus call failed: %v", err)
		return errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to factory reset via D-Bus: %v", err))
	}

	log.Printf("Factory reset command sent successfully via D-Bus")
	return nil
}

func (m *SonicAgent) Reprovision(ctx context.Context) *agent.Status {
	if m.dbusClient == nil {
		return errors.NewErrorStatus(errors.BAD_REQUEST, "D-Bus client not available")
	}
	handler, status := findHandler(m.OSProfile, "reprovision")
	if status != nil {
		return status
	}

	if err := m.dbusClient.CallHandler(ctx, *handler); err != nil {
		log.Printf("D-Bus call failed: %v", err)
		return errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to reprovision via D-Bus: %v", err))
	}

	log.Printf("Reprovision command sent successfully via D-Bus")
	return nil
}

func (m *SonicAgent) GetReadiness(ctx context.Context) (bool, *agent.Status) {
	if m.dbusClient == nil || m.OSProfile == "" {
		return false, &agent.Status{Code: 1, Message: "no dbus client or OSProfile"}
	}
	if !m.dbusClient.ServiceAvailable(ctx, "org.SONiC.HostService") {
		return false, &agent.Status{Code: 1, Message: "hostService is not available"}
	}
	_, errs := hostservices.HostServicesCompatibilityCheck(ctx, m.dbusClient, m.OSProfile)
	if len(errs) > 0 {
		return false, &agent.Status{Code: 1, Message: "hostservice compatibility check is failing"}
	}
	if st := m.reprovisionErr.Load(); st != nil {
		return false, st
	}
	if !m.reprovisionDone.Load() {
		return false, &agent.Status{Code: 1, Message: "reprovision not yet complete"}
	}
	return true, nil
}

func (m *SonicAgent) GetLastRebootTime(ctx context.Context) (time.Time, *agent.Status) {
	cause, status := m.RebootCause(ctx)
	if status != nil {
		return time.Time{}, status
	}
	t, err := time.Parse(time.RFC3339, cause)
	if err != nil {
		return time.Time{}, errors.NewErrorStatus(errors.BAD_REQUEST,
			fmt.Sprintf("failed to parse reboot time %q: %v", cause, err))
	}
	return t, nil
}

func (m *SonicAgent) EnsureInterface(ctx context.Context, req *agent.EnsureInterfaceRequest) *agent.Status {

	switch req.Type {
	case "Loopback":
		db, err := m.newDBAccessor()
		if err != nil {
			return errors.NewErrorStatus(errors.BAD_REQUEST, err.Error())
		}
		if err := db.ensureInterfaceLoopback(ctx, req.InterfaceName); err != nil {
			return errors.NewErrorStatus(errors.BAD_REQUEST, err.Error())
		}
		if err := db.setAdminState(ctx, req.InterfaceName, string(req.AdminStatus)); err != nil {
			return errors.NewErrorStatus(errors.BAD_REQUEST, err.Error())
		}
		if err := db.syncIPAddresses(ctx, req.InterfaceName, req.IPv4Prefixes); err != nil {
			return errors.NewErrorStatus(errors.BAD_REQUEST, err.Error())
		}
		return &agent.Status{}

	case "Physical":
		nativeName, err := agent.AbstractNameToNativeName(req.InterfaceName)
		if err != nil {
			return &agent.Status{Code: errors.BAD_REQUEST, Message: "interface name: " + req.InterfaceName + "couldn't be parse to native name"}

		}
		db, err := m.newDBAccessor()
		if err != nil {
			return errors.NewErrorStatus(errors.BAD_REQUEST, err.Error())
		}

		if err := db.setAdminState(ctx, nativeName, string(req.AdminStatus)); err != nil {
			return errors.NewErrorStatus(errors.BAD_REQUEST, err.Error())
		}
		if err := db.setMTU(ctx, nativeName, int(req.MTU)); err != nil {
			return errors.NewErrorStatus(errors.BAD_REQUEST, err.Error())
		}

		if req.SwitchportMode == "" {
			// Routed: remove all VLAN memberships, then sync IPs
			if err := db.syncVLANMembers(ctx, nativeName, nil); err != nil {
				return errors.NewErrorStatus(errors.BAD_REQUEST, err.Error())
			}
			if err := db.syncIPAddresses(ctx, nativeName, req.IPv4Prefixes); err != nil {
				return errors.NewErrorStatus(errors.BAD_REQUEST, err.Error())
			}
		} else {
			// Switched: remove any IP addresses, then sync VLAN memberships
			if err := db.syncIPAddresses(ctx, nativeName, nil); err != nil {
				return errors.NewErrorStatus(errors.BAD_REQUEST, err.Error())
			}
			desired := buildVLANMemberMap(req)
			if err := db.syncVLANMembers(ctx, nativeName, desired); err != nil {
				return errors.NewErrorStatus(errors.BAD_REQUEST, err.Error())
			}
		}
		return &agent.Status{}

	case "RoutedVLAN":
		vlanIDStr := strings.TrimPrefix(req.InterfaceName, "Vlan")
		vlanID, err := strconv.Atoi(vlanIDStr)
		if err != nil {
			return errors.NewErrorStatus(errors.BAD_REQUEST,
				fmt.Sprintf("invalid RoutedVLAN interface name %q: %v", req.InterfaceName, err))
		}
		if status := m.EnsureVLAN(ctx, &agent.VLANRequest{
			VlanID:     int32(vlanID),
			AdminState: string(req.AdminStatus),
		}); status != nil {
			return status
		}
		db, err := m.newDBAccessor()
		if err != nil {
			return errors.NewErrorStatus(errors.BAD_REQUEST, err.Error())
		}
		if err := db.ensureVLANInterface(ctx, req.InterfaceName, req.VRFName); err != nil {
			return errors.NewErrorStatus(errors.BAD_REQUEST, err.Error())
		}
		if err := db.syncIPAddresses(ctx, req.InterfaceName, req.IPv4Prefixes); err != nil {
			return errors.NewErrorStatus(errors.BAD_REQUEST, err.Error())
		}
		return &agent.Status{}

	default:
		return &agent.Status{Code: errors.BAD_REQUEST, Message: "type " + req.Type + "not implemented"}

	}

}

// buildVLANMemberMap converts switchport request fields to a map of vlanName→taggingMode
// suitable for syncVLANMembers. Access mode produces one untagged entry; trunk mode
// produces tagged entries for AllowedVlans and an untagged entry for NativeVlan.
func buildVLANMemberMap(req *agent.EnsureInterfaceRequest) map[string]string {
	m := make(map[string]string)
	switch req.SwitchportMode {
	case "access":
		if req.AccessVlan > 0 {
			m[fmt.Sprintf("Vlan%d", req.AccessVlan)] = "untagged"
		}
	case "trunk":
		for _, vlan := range req.AllowedVlans {
			m[fmt.Sprintf("Vlan%d", vlan)] = "tagged"
		}
		if req.NativeVlan > 0 {
			m[fmt.Sprintf("Vlan%d", req.NativeVlan)] = "untagged"
		}
	}
	return m
}

func (m *SonicAgent) DeleteInterface(ctx context.Context, interfaceName string) *agent.Status {
	db, err := m.newDBAccessor()
	if err != nil {
		return errors.NewErrorStatus(errors.BAD_REQUEST, err.Error())
	}

	switch {
	case strings.HasPrefix(interfaceName, "Vlan"):
		vlanIDStr := strings.TrimPrefix(interfaceName, "Vlan")
		vlanID, err := strconv.Atoi(vlanIDStr)
		if err != nil {
			return errors.NewErrorStatus(errors.BAD_REQUEST,
				fmt.Sprintf("invalid VLAN interface name %q: %v", interfaceName, err))
		}
		return m.DeleteVLAN(ctx, int32(vlanID))

	case strings.HasPrefix(interfaceName, "Loopback"):
		if err := db.syncIPAddresses(ctx, interfaceName, nil); err != nil {
			return errors.NewErrorStatus(errors.BAD_REQUEST, err.Error())
		}
		if err := db.deleteInterfaceLoopback(ctx, interfaceName); err != nil {
			return errors.NewErrorStatus(errors.BAD_REQUEST, err.Error())
		}

	default:
		nativeName, err := agent.AbstractNameToNativeName(interfaceName)
		if err != nil {
			nativeName = interfaceName
		}
		if err := db.setAdminState(ctx, nativeName, "down"); err != nil {
			return errors.NewErrorStatus(errors.BAD_REQUEST, err.Error())
		}
		if err := db.syncIPAddresses(ctx, nativeName, nil); err != nil {
			return errors.NewErrorStatus(errors.BAD_REQUEST, err.Error())
		}
		if err := db.syncVLANMembers(ctx, nativeName, nil); err != nil {
			return errors.NewErrorStatus(errors.BAD_REQUEST, err.Error())
		}
	}

	return nil
}

func (m *SonicAgent) GetInterfaceStatus(ctx context.Context, interfaceName string) (*agent.InterfaceStatus, *agent.Status) {
	db, err := m.newDBAccessor()
	if err != nil {
		return nil, errors.NewErrorStatus(errors.BAD_REQUEST, fmt.Sprintf("failed to connect to DB: %v", err))
	}
	adminStatus, err := db.getAdminStatus(ctx, interfaceName)
	if err != nil {
		return nil, errors.NewErrorStatus(errors.REDIS_KEY_CHECK_FAIL, fmt.Sprintf("failed to get admin status: %v", err))
	}
	operStatus, err := db.getOperStatus(ctx, interfaceName)
	if err != nil {
		return nil, errors.NewErrorStatus(errors.REDIS_KEY_CHECK_FAIL, fmt.Sprintf("failed to get oper status: %v", err))
	}
	return &agent.InterfaceStatus{AdminStatus: adminStatus, OperStatus: operStatus}, nil
}

func (m *SonicAgent) EnsureDHCPRelay(ctx context.Context, req *agent.DHCPRelayRequest) *agent.Status {
	db, err := m.newDBAccessor()
	if err != nil {
		return errors.NewErrorStatus(errors.BAD_REQUEST, err.Error())
	}
	for _, ifaceName := range req.InterfaceNames {
		exists, err := db.vlanExists(ctx, ifaceName)
		if err != nil {
			return errors.NewErrorStatus(errors.REDIS_KEY_CHECK_FAIL, err.Error())
		}
		if !exists {
			return errors.NewErrorStatus(errors.NOT_FOUND, fmt.Sprintf("VLAN %s does not exist", ifaceName))
		}
		if err := db.ensureDHCPRelay(ctx, ifaceName, req.ServerAddresses); err != nil {
			return errors.NewErrorStatus(errors.BAD_REQUEST, err.Error())
		}
	}
	return nil
}

func (m *SonicAgent) DeleteDHCPRelay(ctx context.Context, interfaceNames []string) *agent.Status {
	db, err := m.newDBAccessor()
	if err != nil {
		return errors.NewErrorStatus(errors.BAD_REQUEST, err.Error())
	}
	for _, ifaceName := range interfaceNames {
		if err := db.deleteDHCPRelay(ctx, ifaceName); err != nil {
			return errors.NewErrorStatus(errors.BAD_REQUEST, err.Error())
		}
	}
	return nil
}

func (m *SonicAgent) GetDHCPRelayStatus(ctx context.Context, interfaceNames []string) (*agent.DHCPRelayStatus, *agent.Status) {
	db, err := m.newDBAccessor()
	if err != nil {
		return nil, errors.NewErrorStatus(errors.BAD_REQUEST, err.Error())
	}
	var configured []string
	for _, ifaceName := range interfaceNames {
		has, err := db.hasDHCPRelay(ctx, ifaceName)
		if err != nil {
			return nil, errors.NewErrorStatus(errors.REDIS_KEY_CHECK_FAIL, err.Error())
		}
		if has {
			configured = append(configured, ifaceName)
		}
	}
	return &agent.DHCPRelayStatus{ConfiguredInterfaces: configured}, nil
}

func (m *SonicAgent) EnsureVLAN(ctx context.Context, req *agent.VLANRequest) *agent.Status {
	vlanName := fmt.Sprintf("Vlan%d", req.VlanID)
	db, err := m.newDBAccessor()
	if err != nil {
		return errors.NewErrorStatus(errors.BAD_REQUEST, err.Error())
	}
	if err := db.ensureVLAN(ctx, vlanName); err != nil {
		return errors.NewErrorStatus(errors.BAD_REQUEST, err.Error())
	}
	if req.Name != "" {
		if err := db.configDB.HSet(ctx, "VLAN|"+vlanName, "description", req.Name).Err(); err != nil {
			return errors.NewErrorStatus(errors.BAD_REQUEST, err.Error())
		}
	}
	if req.AdminState != "" {
		if err := db.setAdminState(ctx, vlanName, req.AdminState); err != nil {
			return errors.NewErrorStatus(errors.BAD_REQUEST, err.Error())
		}
	}
	return nil
}

func (m *SonicAgent) DeleteVLAN(ctx context.Context, vlanID int32) *agent.Status {
	vlanName := fmt.Sprintf("Vlan%d", vlanID)
	db, err := m.newDBAccessor()
	if err != nil {
		return errors.NewErrorStatus(errors.BAD_REQUEST, err.Error())
	}
	if err := db.syncIPAddresses(ctx, vlanName, nil); err != nil {
		return errors.NewErrorStatus(errors.BAD_REQUEST, err.Error())
	}
	if err := db.deleteVLANInterface(ctx, vlanName); err != nil {
		return errors.NewErrorStatus(errors.BAD_REQUEST, err.Error())
	}
	if err := db.deleteVLAN(ctx, vlanName); err != nil {
		return errors.NewErrorStatus(errors.BAD_REQUEST, err.Error())
	}
	return nil
}

func (m *SonicAgent) GetVLANStatus(ctx context.Context, vlanID int32) (*agent.VLANStatus, *agent.Status) {
	vlanName := fmt.Sprintf("Vlan%d", vlanID)
	db, err := m.newDBAccessor()
	if err != nil {
		return nil, errors.NewErrorStatus(errors.BAD_REQUEST, err.Error())
	}
	operStatus, err := db.getOperStatus(ctx, vlanName)
	if err != nil {
		return nil, errors.NewErrorStatus(errors.REDIS_KEY_CHECK_FAIL, err.Error())
	}
	return &agent.VLANStatus{OperStatus: operStatus}, nil
}

func (m *SonicAgent) EnsureLLDP(_ context.Context, _ *agent.LLDPRequest) *agent.Status {
	return &agent.Status{Code: 0, Message: "LLDP cannot be configured per interface on Sonic"}
	// return errors.NewErrorStatus(errors.BAD_REQUEST, "EnsureLLDP not implemented")

}

func (m *SonicAgent) DeleteLLDP(_ context.Context) *agent.Status {
	return &agent.Status{Code: 0, Message: "LLDP cannot be configured per interface on Sonic"}

	// return errors.NewErrorStatus(errors.BAD_REQUEST, "DeleteLLDP not implemented")
}

func (m *SonicAgent) GetLLDPStatus(_ context.Context) (*agent.LLDPStatus, *agent.Status) {
	return &agent.LLDPStatus{OperStatus: true}, &agent.Status{Code: 0, Message: "feature cannot be disabled"}

	// return nil, errors.NewErrorStatus(errors.BAD_REQUEST, "GetLLDPStatus not implemented")
}

// vlanPrefixFromLoopback derives a /80 IPv6 prefix for a VLAN interface from the
// switch's loopback CIDR. It strips the loopback address to its /48 base and then
// encodes the VLAN ID as a big-endian uint16 into bytes 6–7 of the address.
//
// Example: loopbackCIDR="2001:db8:0:1::1/128", vlanID=1001 → "2001:db8:0:3e9::/80"
func vlanPrefixFromLoopback(loopbackCIDR string, vlanID int32) (string, error) {
	if vlanID < 0 || vlanID > 0xFFFF {
		return "", fmt.Errorf("vlanID %d out of uint16 range", vlanID)
	}
	p, err := netip.ParsePrefix(loopbackCIDR)
	if err != nil {
		return "", fmt.Errorf("invalid loopback CIDR %q: %w", loopbackCIDR, err)
	}
	addr := p.Addr()
	if !addr.Is6() {
		return "", fmt.Errorf("loopback address %q is not IPv6", loopbackCIDR)
	}
	b := addr.As16()
	// Zero bytes 6–15 to obtain the /48 network base.
	for i := 6; i < 16; i++ {
		b[i] = 0
	}
	// Encode VLAN ID into bytes 6–7.
	binary.BigEndian.PutUint16(b[6:8], uint16(vlanID))
	derived, ok := netip.AddrFromSlice(b[:])
	if !ok {
		return "", fmt.Errorf("failed to reconstruct IPv6 address")
	}
	return netip.PrefixFrom(derived.Unmap(), 80).String(), nil
}

func (m *SonicAgent) ApplySwitch(ctx context.Context, req *agent.ApplySwitchRequest) *agent.Status {
	ready, readyStatus := m.GetReadiness(ctx)
	if !ready {
		if readyStatus != nil {
			return readyStatus
		}
		return &agent.Status{Code: 1, Message: "system not ready"}
	}

	db, err := m.newDBAccessor()
	if err != nil {
		return &agent.Status{Code: 1, Message: fmt.Sprintf("failed to connect to DB: %v", err)}
	}

	return m.applySwitch(ctx, db, req.Config, req.Device)
}

func (m *SonicAgent) applySwitch(ctx context.Context, db *dbAccessor, cfg agent.WireSwitchConfig, device string) *agent.Status {
	// Record that provisioning has started. Any subsequent early-return on error
	// will overwrite this with state=Error so CellStatus can report the failure.
	_ = db.setCellState(ctx, device, "Creating", "")

	fail := func(msg string) *agent.Status {
		_ = db.setCellState(ctx, device, "Error", msg)
		return &agent.Status{Code: 1, Message: msg}
	}

	// Hostname
	if cfg.Hostname != "" {
		if err := db.configDB.HSet(ctx, "DEVICE_METADATA|localhost", "hostname", cfg.Hostname).Err(); err != nil {
			return fail(fmt.Sprintf("failed to set hostname: %v", err))
		}
	}

	// Loopback0
	if err := db.ensureInterfaceLoopback(ctx, "Loopback0"); err != nil {
		return fail(fmt.Sprintf("failed to ensure Loopback0: %v", err))
	}
	if len(cfg.LoopbackIPs) > 0 {
		if err := db.syncIPAddresses(ctx, "Loopback0", cfg.LoopbackIPs); err != nil {
			return fail(fmt.Sprintf("failed to sync loopback IPs: %v", err))
		}
	}

	// VLANs (leaf-specific; spines send empty VLANs list)
	// id=0 entries carry NORTH uplink port names for BGP only — no VLAN is created,
	// but we still evict those ports from any stale VLAN membership and configure them.
	for _, vlan := range cfg.VLANs {
		if vlan.ID == 0 {
			if err := applyUplinkPorts(ctx, db, vlan.Members, fail); err != nil {
				return err
			}
			continue
		}
		if err := applyVLAN(ctx, db, cfg, vlan, fail); err != nil {
			return err
		}
	}

	// BGP / FRR config generation
	if cfg.BGP != nil {
		bgpCfg := &agent.WireBGPConfig{
			ASN:        cfg.BGP.ASN,
			RouterID:   cfg.BGP.RouterID,
			PeerGroups: buildBGPPeerGroups(cfg),
		}

		frrConf, err := frr.GenerateFRRConfig(bgpCfg, cfg.Hostname, cfg.Prefixes)
		if err != nil {
			return fail(fmt.Sprintf("failed to generate frr.conf: %v", err))
		}
		log.Print(frrConf)
		if err := m.writeFRRConfig(m.frrConfigPath, frrConf); err != nil {
			return fail(fmt.Sprintf("failed to write frr.conf: %v", err))
		}
		if err := m.restartService(ctx, "bgp"); err != nil {
			return fail(fmt.Sprintf("failed to restart bgp service: %s", err))
		}
		if _, err := os.Stat(m.frrConfigPath); os.IsNotExist(err) {
			return fail("bgp service deleted frr.conf")
		}

	}

	_ = db.setCellState(ctx, device, "Active", "")
	return nil
}

// applyUplinkPorts configures NORTH uplink ports (VLAN id=0): evicts them from
// all VLANs and sets MTU + FEC.
func applyUplinkPorts(ctx context.Context, db *dbAccessor, members []agent.WireVLANMember, fail func(string) *agent.Status) *agent.Status {
	for _, member := range members {
		portName, err := agent.AbstractNameToNativeName(member.InterfaceID)
		if err != nil {
			portName = member.InterfaceID
		}
		if err := db.syncVLANMembers(ctx, portName, nil); err != nil {
			return fail(fmt.Sprintf("failed to evict %s from VLANs: %v", portName, err))
		}
		if err := db.setMTU(ctx, portName, int(9100)); err != nil {
			return fail(fmt.Sprintf("failed to set MTU on %s: %v", portName, err))
		}
		if err := db.setFEC(ctx, portName, "rs"); err != nil {
			return fail(fmt.Sprintf("failed to set FEC on %s: %v", portName, err))
		}
		// if err := db.setSpeed(ctx, portName, 25_000); err != nil {
		// 	return fail(fmt.Sprintf("failed to set speed on %s: %v", portName, err))
		// }
	}
	return nil
}

// applyVLAN creates/updates a single VLAN entry including its interface, IP
// addresses, DHCP relay and member port configuration.
func applyVLAN(ctx context.Context, db *dbAccessor, cfg agent.WireSwitchConfig, vlan agent.WireVLAN, fail func(string) *agent.Status) *agent.Status {
	vlanName := fmt.Sprintf("Vlan%d", vlan.ID)
	if err := db.ensureVLAN(ctx, vlanName); err != nil {
		return fail(fmt.Sprintf("failed to ensure %s: %v", vlanName, err))
	}
	if err := db.ensureVLANInterface(ctx, vlanName, ""); err != nil {
		return fail(fmt.Sprintf("failed to ensure VLAN interface %s: %v", vlanName, err))
	}
	prefix := vlan.Prefix
	if prefix == "" && len(cfg.LoopbackIPs) > 0 {
		if derived, err := vlanPrefixFromLoopback(cfg.LoopbackIPs[0], vlan.ID); err == nil {
			prefix = derived
		}
	}
	if prefix != "" {
		if err := db.syncIPAddresses(ctx, vlanName, []string{prefix}); err != nil {
			return fail(fmt.Sprintf("failed to sync IPs for %s: %v", vlanName, err))
		}
	}
	if vlan.DHCPRelay != "" {
		if err := db.ensureDHCPRelay(ctx, vlanName, []string{vlan.DHCPRelay}); err != nil {
			return fail(fmt.Sprintf("failed to set DHCP relay for %s: %v", vlanName, err))
		}
	}
	for _, member := range vlan.Members {
		portName, err := agent.AbstractNameToNativeName(member.InterfaceID)
		if err != nil {
			// Not an abstract Ethernet name (e.g. PortChannel1) — use as-is.
			portName = member.InterfaceID
		}
		if err := db.syncVLANMembers(ctx, portName, map[string]string{vlanName: "untagged"}); err != nil {
			return fail(fmt.Sprintf("failed to sync member %s to %s: %v", portName, vlanName, err))
		}
		if err := db.setMTU(ctx, portName, int(9100)); err != nil {
			return fail(fmt.Sprintf("failed to set MTU on %s: %v", portName, err))
		}
		if err := db.setFEC(ctx, portName, "rs"); err != nil {
			return fail(fmt.Sprintf("failed to set FEC on %s: %v", portName, err))
		}
		if err := db.setSpeed(ctx, portName, 25_000); err != nil {
			return fail(fmt.Sprintf("failed to set speed on %s: %v", portName, err))
		}
	}
	return nil
}

// buildBGPPeerGroups derives BGP peer groups from the switch config.
// Topology detection: if no VLAN has a DHCPRelay the switch is a spine.
// Spine: single LEAFS peer group, all ports as neighbors.
// Leaf: NORTH (id=0, no relay) and SOUTH (relay present) peer groups.
func buildBGPPeerGroups(cfg agent.WireSwitchConfig) []agent.WireBGPPeerGroup {
	hasSouth := false
	for _, vlan := range cfg.VLANs {
		if vlan.DHCPRelay != "" {
			hasSouth = true
			break
		}
	}

	if !hasSouth {
		var leafsNeighbors []agent.WireBGPNeighbor
		for _, vlan := range cfg.VLANs {
			for _, member := range vlan.Members {
				portName, err := agent.AbstractNameToNativeName(member.InterfaceID)
				if err != nil {
					portName = member.InterfaceID
				}
				leafsNeighbors = append(leafsNeighbors, agent.WireBGPNeighbor{InterfaceID: portName})
			}
		}
		return []agent.WireBGPPeerGroup{{Name: "LEAFS", Neighbors: leafsNeighbors}}
	}

	var northNeighbors, southNeighbors []agent.WireBGPNeighbor
	for _, vlan := range cfg.VLANs {
		if vlan.DHCPRelay != "" {
			southNeighbors = append(southNeighbors, agent.WireBGPNeighbor{
				VlanID:      vlan.ID,
				InterfaceID: fmt.Sprintf("Vlan%d", vlan.ID),
			})
		} else {
			for _, member := range vlan.Members {
				portName, err := agent.AbstractNameToNativeName(member.InterfaceID)
				if err != nil {
					portName = member.InterfaceID
				}
				northNeighbors = append(northNeighbors, agent.WireBGPNeighbor{InterfaceID: portName})
			}
		}
	}
	return []agent.WireBGPPeerGroup{
		{Name: "NORTH", Neighbors: northNeighbors},
		{Name: "SOUTH", Neighbors: southNeighbors},
	}
}

const cellStateReprovisioning = "Reprovisioning"

func (m *SonicAgent) DeleteSwitch(ctx context.Context, device string) *agent.Status {
	db, err := m.newDBAccessor()
	if err != nil {
		return &agent.Status{Code: 1, Message: err.Error()}
	}

	state, _, err := db.getCellState(ctx, device)
	if err != nil {
		return &agent.Status{Code: 1, Message: fmt.Sprintf("failed to read reprovision state: %v", err)}
	}

	switch state {
	case cellStateReprovisioning:
		// Still running — return in-progress sentinel.
		return &agent.Status{Code: 2, Message: cellStateReprovisioning}
	case "":
		// Key absent or state cleared — reprovision is not running. Check if we
		// need to start it (first call) or it already completed (subsequent call).
		// We distinguish these by whether the key exists: absent = never started
		// or already finished. Either way, return success immediately.
		return nil
	}

	// State is anything other than "" or "Reprovisioning" (e.g. "Active", "Error")
	// — reprovision has not been requested yet. Kick it off.
	_ = db.setCellState(ctx, device, cellStateReprovisioning, "")

	go func() {
		bgCtx := context.Background()
		st := m.Reprovision(bgCtx)
		bgDB, err := m.newDBAccessor()
		if err != nil {
			return
		}
		if st != nil {
			_ = bgDB.setCellState(bgCtx, device, "Error", st.Message)
		} else {
			_ = bgDB.deleteCellState(bgCtx, device)
		}
	}()

	return &agent.Status{Code: 2, Message: cellStateReprovisioning}
}

func (m *SonicAgent) GetCellStatus(ctx context.Context, device string) (state, message string, agentStatus *agent.Status) {
	db, err := m.newDBAccessor()
	if err != nil {
		return "", "", &agent.Status{Code: 1, Message: fmt.Sprintf("failed to connect to DB: %v", err)}
	}
	state, message, err = db.getCellState(ctx, device)
	if err != nil {
		return "", "", &agent.Status{Code: 1, Message: fmt.Sprintf("failed to get cell state: %v", err)}
	}
	return state, message, nil
}
