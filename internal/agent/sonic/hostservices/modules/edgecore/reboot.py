"""Reboot command handler"""

from host_modules import host_service
import json
import subprocess

MOD_NAME = 'reboot'

REBOOT_METHOD_COLD_BOOT_VALUES = {1, "COLD"}
REBOOT_METHOD_HALT_BOOT_VALUES = {3, "HALT"}
REBOOT_METHOD_WARM_BOOT_VALUES = {4, "WARM"}

class Reboot(host_service.HostModule):
    """
    DBus endpoint that executes the reboot command
    """

    @host_service.method(host_service.bus_name(MOD_NAME), in_signature='as', out_signature='is')
    def issue_reboot(self, options):
        try:
            params = json.loads(options[0]) if options else {}
        except (json.JSONDecodeError, IndexError):
            return 1, "Invalid JSON input"
        reboot_method = params.get('method', '')

        if reboot_method in REBOOT_METHOD_COLD_BOOT_VALUES or reboot_method == "COLD":
            cmd = ['sudo', 'reboot']
        elif reboot_method in REBOOT_METHOD_HALT_BOOT_VALUES or reboot_method == "HALT":
            cmd = ['sudo', 'reboot', '-p']
        elif reboot_method in REBOOT_METHOD_WARM_BOOT_VALUES or reboot_method == "WARM":
            cmd = ['sudo', 'warm-reboot']
        else:
            return 1, "Unsupported reboot method: " + str(reboot_method)

        result = subprocess.run(cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        msg = ''
        if result.returncode:
            lines = result.stderr.decode().split('\n')
            for line in lines:
                if 'Error' in line:
                    msg = line
                    break
        return result.returncode, msg

def register():
    """Return class and module name"""
    return Reboot, MOD_NAME
