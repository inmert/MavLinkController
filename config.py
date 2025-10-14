#!/usr/bin/env python3
"""
Configuration and validation for ArduPilot bridge
"""

import os
import logging
from dataclasses import dataclass
from typing import Optional

# ============================================================================
# CONFIGURATION
# ============================================================================

@dataclass
class Config:
    """Main configuration"""
    # Serial
    serial_port: str = os.getenv('SERIAL_PORT', '/dev/ttyTHS1')
    baud_rate: int = int(os.getenv('BAUD_RATE', '57600'))
    
    # Network
    udp_ip: str = os.getenv('UDP_IP', '192.168.50.17')
    udp_port: int = int(os.getenv('UDP_PORT', '14550'))
    tcp_port: int = int(os.getenv('TCP_PORT', '5760'))
    
    # Timeouts
    heartbeat_timeout: float = 10.0
    arm_timeout: float = 5.0
    connection_retry_delay: float = 5.0
    max_connection_retries: int = 5
    tcp_recv_timeout: float = 1.0
    
    # Safety
    motor_command_rate_limit: float = 0.05  # 50ms minimum between commands
    emergency_stop_enabled: bool = True
    require_prearm_token: bool = True
    prearm_disable_token: str = "DISABLE_SAFETY_CHECKS"
    
    # Logging
    log_level: str = os.getenv('LOG_LEVEL', 'INFO')
    log_file: Optional[str] = os.getenv('LOG_FILE', 'bridge.log')
    
    # Message filtering
    forward_all_messages: bool = False
    critical_message_types: list = None
    
    def __post_init__(self):
        if self.critical_message_types is None:
            self.critical_message_types = [
                'HEARTBEAT', 'GLOBAL_POSITION_INT', 'ATTITUDE',
                'GPS_RAW_INT', 'BATTERY_STATUS', 'STATUSTEXT',
                'SERVO_OUTPUT_RAW', 'RC_CHANNELS', 'VFR_HUD'
            ]

# ============================================================================
# VALIDATORS
# ============================================================================

class Validators:
    """Input validation functions"""
    
    @staticmethod
    def motor_num(value: int) -> bool:
        """Validate motor number"""
        return 1 <= value <= 8
    
    @staticmethod
    def pwm_value(value: int) -> bool:
        """Validate PWM value"""
        return 1000 <= value <= 2000
    
    @staticmethod
    def throttle_percent(value: int) -> bool:
        """Validate throttle percentage"""
        return 0 <= value <= 100
    
    @staticmethod
    def duration(value: int) -> bool:
        """Validate duration in seconds"""
        return 1 <= value <= 30
    
    @staticmethod
    def mode_name(value: str, valid_modes: list) -> bool:
        """Validate mode name"""
        return value.upper() in valid_modes

# ============================================================================
# CONSTANTS
# ============================================================================

class ResponseCode:
    """Response codes for TCP commands"""
    SUCCESS = "OK"
    ERROR = "ERROR"
    INVALID_COMMAND = "INVALID_COMMAND"
    INVALID_PARAMS = "INVALID_PARAMS"
    RATE_LIMITED = "RATE_LIMITED"
    NOT_CONNECTED = "NOT_CONNECTED"
    EMERGENCY_STOP = "EMERGENCY_STOP"
    REQUIRES_TOKEN = "REQUIRES_TOKEN"

# ============================================================================
# LOGGING SETUP
# ============================================================================

def setup_logging(config: Config) -> logging.Logger:
    """Setup logging configuration"""
    logger = logging.getLogger('jetson_bridge')
    logger.setLevel(getattr(logging, config.log_level.upper()))
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S'
    )
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)
    
    # File handler
    if config.log_file:
        file_handler = logging.FileHandler(config.log_file)
        file_handler.setLevel(logging.DEBUG)
        file_format = logging.Formatter(
            '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_format)
        logger.addHandler(file_handler)
    
    return logger