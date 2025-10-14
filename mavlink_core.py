#!/usr/bin/env python3
"""
MAVLink connection and control layer with thread safety
"""

from pymavlink import mavutil
import threading
import time
import logging
from typing import Optional
from config import Config, Validators

logger = logging.getLogger('jetson_bridge.mavlink')

# ============================================================================
# CONNECTION MANAGER
# ============================================================================

class ConnectionManager:
    """Thread-safe MAVLink connection manager with reconnection"""
    
    def __init__(self, config: Config):
        self.config = config
        self.master: Optional[mavutil.mavfile] = None
        self.udp_output: Optional[mavutil.mavfile] = None
        self.lock = threading.RLock()
        self.connected = False
        self.retry_count = 0
    
    def connect(self) -> bool:
        """Connect to flight controller and setup UDP output"""
        with self.lock:
            if self._connect_serial() and self._setup_udp():
                self.connected = True
                self.retry_count = 0
                return True
            return False
    
    def _connect_serial(self) -> bool:
        """Connect to flight controller via UART"""
        logger.info(f"Connecting to {self.config.serial_port} at {self.config.baud_rate} baud")
        
        try:
            self.master = mavutil.mavlink_connection(
                self.config.serial_port,
                baud=self.config.baud_rate,
                source_system=255
            )
            
            logger.info("Waiting for heartbeat...")
            heartbeat_msg = self.master.recv_match(
                type='HEARTBEAT',
                blocking=True,
                timeout=self.config.heartbeat_timeout
            )
            
            if heartbeat_msg is None:
                logger.error("Heartbeat timeout")
                return False
            
            logger.info(f"Connected to system {self.master.target_system}, "
                       f"component {self.master.target_component}")
            return True
            
        except Exception as e:
            logger.error(f"Serial connection failed: {e}")
            return False
    
    def _setup_udp(self) -> bool:
        """Setup UDP broadcast output"""
        logger.info(f"Creating UDP output to {self.config.udp_ip}:{self.config.udp_port}")
        
        try:
            self.udp_output = mavutil.mavlink_connection(
                f'udpout:{self.config.udp_ip}:{self.config.udp_port}',
                source_system=self.master.target_system
            )
            logger.info("UDP output created")
            return True
            
        except Exception as e:
            logger.error(f"UDP setup failed: {e}")
            return False
    
    def reconnect(self) -> bool:
        """Attempt to reconnect with retry logic"""
        if self.retry_count >= self.config.max_connection_retries:
            logger.error("Max reconnection attempts reached")
            return False
        
        self.retry_count += 1
        logger.warning(f"Reconnection attempt {self.retry_count}/{self.config.max_connection_retries}")
        
        self.close()
        time.sleep(self.config.connection_retry_delay)
        
        return self.connect()
    
    def is_connected(self) -> bool:
        """Check if connection is alive"""
        with self.lock:
            return self.connected and self.master is not None
    
    def get_master(self) -> Optional[mavutil.mavfile]:
        """Thread-safe access to master connection"""
        with self.lock:
            return self.master if self.connected else None
    
    def get_udp(self) -> Optional[mavutil.mavfile]:
        """Thread-safe access to UDP output"""
        with self.lock:
            return self.udp_output
    
    def close(self):
        """Close all connections"""
        with self.lock:
            self.connected = False
            if self.master:
                try:
                    self.master.close()
                except:
                    pass
                self.master = None
            if self.udp_output:
                try:
                    self.udp_output.close()
                except:
                    pass
                self.udp_output = None

# ============================================================================
# VEHICLE STATE TRACKER
# ============================================================================

class VehicleState:
    """Track vehicle state thread-safely"""
    
    def __init__(self):
        self.lock = threading.Lock()
        self.armed = False
        self.mode = "UNKNOWN"
        self.battery_voltage = 0.0
        self.gps_fix = 0
        self.last_heartbeat = 0.0
    
    def update_from_heartbeat(self, msg):
        """Update state from HEARTBEAT message"""
        with self.lock:
            self.armed = bool(msg.base_mode & mavutil.mavlink.MAV_MODE_FLAG_SAFETY_ARMED)
            self.mode = mavutil.mode_string_v10(msg)
            self.last_heartbeat = time.time()
    
    def update_battery(self, voltage: float):
        """Update battery voltage"""
        with self.lock:
            self.battery_voltage = voltage
    
    def get_state(self) -> dict:
        """Get current state snapshot"""
        with self.lock:
            return {
                'armed': self.armed,
                'mode': self.mode,
                'battery_v': self.battery_voltage,
                'gps_fix': self.gps_fix,
                'heartbeat_age': time.time() - self.last_heartbeat
            }

# ============================================================================
# MOTOR CONTROLLER
# ============================================================================

class MotorController:
    """Thread-safe motor control with rate limiting"""
    
    def __init__(self, conn_manager: ConnectionManager, config: Config):
        self.conn = conn_manager
        self.config = config
        self.lock = threading.Lock()
        self.last_command_time = 0.0
        self.emergency_stopped = False
    
    def _check_rate_limit(self) -> bool:
        """Check if command rate limit allows operation"""
        now = time.time()
        if now - self.last_command_time < self.config.motor_command_rate_limit:
            return False
        self.last_command_time = now
        return True
    
    def _check_safety(self) -> bool:
        """Check safety conditions"""
        if self.emergency_stopped:
            logger.warning("Emergency stop active")
            return False
        if not self.conn.is_connected():
            logger.warning("Not connected to flight controller")
            return False
        return True
    
    def set_motor_speed(self, motor_num: int, pwm_value: int) -> tuple[bool, str]:
        """Set individual motor speed with safety checks"""
        with self.lock:
            if not self._check_safety():
                return False, "Safety check failed"
            
            if not self._check_rate_limit():
                return False, "Rate limited"
            
            if not Validators.motor_num(motor_num):
                return False, f"Invalid motor number: {motor_num}"
            
            if not Validators.pwm_value(pwm_value):
                return False, f"Invalid PWM value: {pwm_value}"
            
            master = self.conn.get_master()
            if not master:
                return False, "No connection"
            
            try:
                rc_channels = [65535] * 8
                rc_channels[motor_num - 1] = pwm_value
                
                master.mav.rc_channels_override_send(
                    master.target_system,
                    master.target_component,
                    *rc_channels
                )
                
                logger.debug(f"Motor {motor_num} -> PWM {pwm_value}")
                return True, "OK"
                
            except Exception as e:
                logger.error(f"Motor command failed: {e}")
                return False, str(e)
    
    def set_all_motors(self, pwm_value: int) -> tuple[bool, str]:
        """Set all motors to same speed"""
        with self.lock:
            if not self._check_safety():
                return False, "Safety check failed"
            
            if not self._check_rate_limit():
                return False, "Rate limited"
            
            if not Validators.pwm_value(pwm_value):
                return False, f"Invalid PWM: {pwm_value}"
            
            master = self.conn.get_master()
            if not master:
                return False, "No connection"
            
            try:
                rc_channels = [pwm_value] * 8
                
                master.mav.rc_channels_override_send(
                    master.target_system,
                    master.target_component,
                    *rc_channels
                )
                
                logger.debug(f"All motors -> PWM {pwm_value}")
                return True, "OK"
                
            except Exception as e:
                logger.error(f"All motors command failed: {e}")
                return False, str(e)
    
    def clear_override(self) -> tuple[bool, str]:
        """Clear all RC overrides"""
        with self.lock:
            master = self.conn.get_master()
            if not master:
                return False, "No connection"
            
            try:
                rc_channels = [65535] * 8
                
                master.mav.rc_channels_override_send(
                    master.target_system,
                    master.target_component,
                    *rc_channels
                )
                
                logger.info("RC override cleared")
                return True, "OK"
                
            except Exception as e:
                logger.error(f"Clear override failed: {e}")
                return False, str(e)
    
    def emergency_stop(self) -> tuple[bool, str]:
        """Emergency stop all motors"""
        logger.warning("EMERGENCY STOP ACTIVATED")
        self.emergency_stopped = True
        return self.set_all_motors(1000)
    
    def reset_emergency_stop(self):
        """Reset emergency stop flag"""
        logger.info("Emergency stop reset")
        self.emergency_stopped = False
    
    def motor_test(self, motor_num: int, throttle_pct: int, duration: int) -> tuple[bool, str]:
        """Test individual motor"""
        with self.lock:
            if not self._check_safety():
                return False, "Safety check failed"
            
            if not Validators.motor_num(motor_num):
                return False, f"Invalid motor: {motor_num}"
            
            if not Validators.throttle_percent(throttle_pct):
                return False, f"Invalid throttle: {throttle_pct}"
            
            if not Validators.duration(duration):
                return False, f"Invalid duration: {duration}"
            
            master = self.conn.get_master()
            if not master:
                return False, "No connection"
            
            try:
                master.mav.command_long_send(
                    master.target_system,
                    master.target_component,
                    mavutil.mavlink.MAV_CMD_DO_MOTOR_TEST,
                    0, motor_num, 0, throttle_pct, duration, 0, 0, 0
                )
                
                logger.info(f"Motor test: {motor_num} @ {throttle_pct}% for {duration}s")
                return True, "OK"
                
            except Exception as e:
                logger.error(f"Motor test failed: {e}")
                return False, str(e)

# ============================================================================
# VEHICLE CONTROLLER
# ============================================================================

class VehicleController:
    """Thread-safe vehicle arm/disarm and mode control"""
    
    def __init__(self, conn_manager: ConnectionManager, config: Config, state: VehicleState):
        self.conn = conn_manager
        self.config = config
        self.state = state
        self.lock = threading.Lock()
    
    def arm(self) -> tuple[bool, str]:
        """Arm vehicle with timeout"""
        with self.lock:
            master = self.conn.get_master()
            if not master:
                return False, "No connection"
            
            try:
                logger.info("Arming vehicle...")
                master.arducopter_arm()
                
                # Wait for armed state with timeout
                start_time = time.time()
                while time.time() - start_time < self.config.arm_timeout:
                    msg = master.recv_match(type='HEARTBEAT', blocking=True, timeout=1.0)
                    if msg and (msg.base_mode & mavutil.mavlink.MAV_MODE_FLAG_SAFETY_ARMED):
                        logger.info("Vehicle armed")
                        return True, "OK"
                
                logger.warning("Arm timeout")
                return False, "Arm timeout"
                
            except Exception as e:
                logger.error(f"Arm failed: {e}")
                return False, str(e)
    
    def disarm(self) -> tuple[bool, str]:
        """Disarm vehicle with timeout"""
        with self.lock:
            master = self.conn.get_master()
            if not master:
                return False, "No connection"
            
            try:
                logger.info("Disarming vehicle...")
                master.arducopter_disarm()
                
                start_time = time.time()
                while time.time() - start_time < self.config.arm_timeout:
                    msg = master.recv_match(type='HEARTBEAT', blocking=True, timeout=1.0)
                    if msg and not (msg.base_mode & mavutil.mavlink.MAV_MODE_FLAG_SAFETY_ARMED):
                        logger.info("Vehicle disarmed")
                        return True, "OK"
                
                logger.warning("Disarm timeout")
                return False, "Disarm timeout"
                
            except Exception as e:
                logger.error(f"Disarm failed: {e}")
                return False, str(e)
    
    def set_mode(self, mode_name: str) -> tuple[bool, str]:
        """Set flight mode"""
        with self.lock:
            master = self.conn.get_master()
            if not master:
                return False, "No connection"
            
            try:
                mode_mapping = master.mode_mapping()
                if mode_name not in mode_mapping:
                    return False, f"Unknown mode: {mode_name}"
                
                mode_id = mode_mapping[mode_name]
                master.set_mode(mode_id)
                
                logger.info(f"Mode set to {mode_name}")
                return True, "OK"
                
            except Exception as e:
                logger.error(f"Mode change failed: {e}")
                return False, str(e)
    
    def disable_prearm_checks(self, token: str) -> tuple[bool, str]:
        """Disable prearm checks with token verification"""
        if self.config.require_prearm_token and token != self.config.prearm_disable_token:
            logger.warning("Prearm disable attempted with invalid token")
            return False, "Invalid token"
        
        with self.lock:
            master = self.conn.get_master()
            if not master:
                return False, "No connection"
            
            try:
                master.mav.param_set_send(
                    master.target_system,
                    master.target_component,
                    b'ARMING_CHECK',
                    0,
                    mavutil.mavlink.MAV_PARAM_TYPE_INT32
                )
                
                logger.warning("Pre-arm checks DISABLED")
                return True, "OK"
                
            except Exception as e:
                logger.error(f"Disable prearm failed: {e}")
                return False, str(e)