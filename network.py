#!/usr/bin/env python3
"""
Network layer: TCP command server and message forwarding
"""

import socket
import json
import threading
import time
import logging
from typing import Optional, Callable
from config import Config, ResponseCode
from mavlink_core import ConnectionManager, MotorController, VehicleController, VehicleState

logger = logging.getLogger('jetson_bridge.network')

# ============================================================================
# COMMAND PARSER
# ============================================================================

class CommandParser:
    """Parse and validate JSON commands from Unity"""
    
    @staticmethod
    def parse(data: str) -> tuple[Optional[dict], Optional[str]]:
        """Parse JSON command string"""
        try:
            cmd = json.loads(data)
            
            if not isinstance(cmd, dict):
                return None, "Command must be JSON object"
            
            if 'cmd' not in cmd:
                return None, "Missing 'cmd' field"
            
            return cmd, None
            
        except json.JSONDecodeError as e:
            return None, f"JSON parse error: {e}"
    
    @staticmethod
    def create_response(code: str, message: str = "", data: dict = None) -> str:
        """Create JSON response"""
        response = {
            'status': code,
            'message': message
        }
        if data:
            response['data'] = data
        
        return json.dumps(response) + '\n'

# ============================================================================
# MESSAGE HANDLER
# ============================================================================

class MessageHandler:
    """Forward messages from FC to UDP with filtering"""
    
    def __init__(self, conn_manager: ConnectionManager, vehicle_state: VehicleState, config: Config):
        self.conn = conn_manager
        self.state = vehicle_state
        self.config = config
        self.running = False
        self.message_count = 0
        self.last_stats_time = time.time()
        self.stats_interval = 10.0
    
    def start(self):
        """Start message forwarding loop"""
        self.running = True
        logger.info("Message forwarding started")
        
        try:
            while self.running:
                if not self.conn.is_connected():
                    logger.warning("Connection lost, attempting reconnect...")
                    if not self.conn.reconnect():
                        time.sleep(5.0)
                        continue
                
                master = self.conn.get_master()
                udp = self.conn.get_udp()
                
                if not master:
                    time.sleep(0.1)
                    continue
                
                # Blocking receive with timeout
                msg = master.recv_match(blocking=True, timeout=1.0)
                
                if msg is None:
                    continue
                
                self.message_count += 1
                
                # Update vehicle state
                self._update_state(msg)
                
                # Forward to UDP if filtering allows
                if udp and self._should_forward(msg):
                    try:
                        udp.write(msg.get_msgbuf())
                    except Exception as e:
                        logger.error(f"UDP write failed: {e}")
                
                # Periodic stats
                self._print_stats()
        
        except Exception as e:
            logger.error(f"Message loop error: {e}")
        
        finally:
            logger.info(f"Message forwarding stopped. Total messages: {self.message_count}")
    
    def stop(self):
        """Stop message forwarding"""
        self.running = False
    
    def _update_state(self, msg):
        """Update vehicle state from message"""
        msg_type = msg.get_type()
        
        if msg_type == 'HEARTBEAT':
            self.state.update_from_heartbeat(msg)
        
        elif msg_type == 'BATTERY_STATUS':
            self.state.update_battery(msg.voltages[0] / 1000.0 if msg.voltages else 0.0)
        
        elif msg_type == 'STATUSTEXT':
            logger.info(f"FC: {msg.text}")
    
    def _should_forward(self, msg) -> bool:
        """Determine if message should be forwarded to UDP"""
        if self.config.forward_all_messages:
            return True
        
        return msg.get_type() in self.config.critical_message_types
    
    def _print_stats(self):
        """Print periodic statistics"""
        now = time.time()
        if now - self.last_stats_time >= self.stats_interval:
            msg_rate = self.message_count / (now - self.last_stats_time)
            state = self.state.get_state()
            
            logger.info(f"Messages: {self.message_count} ({msg_rate:.1f}/s) | "
                       f"State: {state['mode']} {'ARMED' if state['armed'] else 'DISARMED'} | "
                       f"Battery: {state['battery_v']:.2f}V")
            
            self.message_count = 0
            self.last_stats_time = now

# ============================================================================
# TCP COMMAND SERVER
# ============================================================================

class TCPCommandServer:
    """TCP server for receiving JSON commands from Unity"""
    
    def __init__(self, vehicle: VehicleController, motors: MotorController, 
                 vehicle_state: VehicleState, config: Config):
        self.vehicle = vehicle
        self.motors = motors
        self.state = vehicle_state
        self.config = config
        self.running = False
        self.server_socket: Optional[socket.socket] = None
        self.client_socket: Optional[socket.socket] = None
        self.client_address = None
        self.heartbeat_thread: Optional[threading.Thread] = None
    
    def start(self):
        """Start TCP server"""
        self.running = True
        
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind(('0.0.0.0', self.config.tcp_port))
            self.server_socket.listen(1)
            self.server_socket.settimeout(1.0)
            
            logger.info(f"TCP server listening on port {self.config.tcp_port}")
            
            while self.running:
                try:
                    logger.info("Waiting for Unity connection...")
                    self.client_socket, self.client_address = self.server_socket.accept()
                    self.client_socket.settimeout(self.config.tcp_recv_timeout)
                    
                    logger.info(f"Unity connected from {self.client_address}")
                    
                    # Start heartbeat thread
                    self.heartbeat_thread = threading.Thread(
                        target=self._send_heartbeats,
                        daemon=True
                    )
                    self.heartbeat_thread.start()
                    
                    # Handle client
                    self._handle_client()
                    
                except socket.timeout:
                    continue
                except Exception as e:
                    if self.running:
                        logger.error(f"Accept error: {e}")
        
        except Exception as e:
            logger.error(f"TCP server error: {e}")
        
        finally:
            self._cleanup()
    
    def stop(self):
        """Stop TCP server"""
        self.running = False
        if self.server_socket:
            self.server_socket.close()
    
    def _send_heartbeats(self):
        """Send periodic heartbeats to Unity"""
        while self.running and self.client_socket:
            try:
                state = self.state.get_state()
                heartbeat = CommandParser.create_response(
                    ResponseCode.SUCCESS,
                    "heartbeat",
                    state
                )
                self.client_socket.sendall(heartbeat.encode('utf-8'))
                time.sleep(1.0)
            except:
                break
    
    def _handle_client(self):
        """Handle commands from connected Unity client"""
        buffer = ""
        
        try:
            while self.running:
                try:
                    data = self.client_socket.recv(4096).decode('utf-8')
                    
                    if not data:
                        logger.info("Unity disconnected")
                        break
                    
                    buffer += data
                    
                    # Process complete commands (newline-delimited JSON)
                    while '\n' in buffer:
                        line, buffer = buffer.split('\n', 1)
                        line = line.strip()
                        
                        if line:
                            response = self._process_command(line)
                            self._send_response(response)
                
                except socket.timeout:
                    continue
        
        except Exception as e:
            logger.error(f"Client handling error: {e}")
        
        finally:
            if self.client_socket:
                self.client_socket.close()
                self.client_socket = None
                self.client_address = None
    
    def _process_command(self, command_str: str) -> str:
        """Process JSON command and return response"""
        logger.debug(f"Received: {command_str}")
        
        # Parse command
        cmd, error = CommandParser.parse(command_str)
        if error:
            logger.warning(f"Parse error: {error}")
            return CommandParser.create_response(ResponseCode.INVALID_COMMAND, error)
        
        # Dispatch to handler
        try:
            return self._dispatch_command(cmd)
        except Exception as e:
            logger.error(f"Command execution error: {e}")
            return CommandParser.create_response(ResponseCode.ERROR, str(e))
    
    def _dispatch_command(self, cmd: dict) -> str:
        """Dispatch command to appropriate handler"""
        cmd_name = cmd['cmd'].lower()
        
        # Vehicle commands
        if cmd_name == 'arm':
            success, msg = self.vehicle.arm()
            return CommandParser.create_response(
                ResponseCode.SUCCESS if success else ResponseCode.ERROR, msg
            )
        
        elif cmd_name == 'disarm':
            success, msg = self.vehicle.disarm()
            return CommandParser.create_response(
                ResponseCode.SUCCESS if success else ResponseCode.ERROR, msg
            )
        
        elif cmd_name == 'mode':
            mode = cmd.get('mode', '').upper()
            success, msg = self.vehicle.set_mode(mode)
            return CommandParser.create_response(
                ResponseCode.SUCCESS if success else ResponseCode.ERROR, msg
            )
        
        # Motor commands
        elif cmd_name == 'motor':
            motor_num = cmd.get('motor', 0)
            pwm = cmd.get('pwm', 0)
            success, msg = self.motors.set_motor_speed(motor_num, pwm)
            return CommandParser.create_response(
                ResponseCode.SUCCESS if success else ResponseCode.ERROR, msg
            )
        
        elif cmd_name == 'all_motors':
            pwm = cmd.get('pwm', 0)
            success, msg = self.motors.set_all_motors(pwm)
            return CommandParser.create_response(
                ResponseCode.SUCCESS if success else ResponseCode.ERROR, msg
            )
        
        elif cmd_name == 'clear_override':
            success, msg = self.motors.clear_override()
            return CommandParser.create_response(
                ResponseCode.SUCCESS if success else ResponseCode.ERROR, msg
            )
        
        elif cmd_name == 'motor_test':
            motor = cmd.get('motor', 0)
            throttle = cmd.get('throttle', 0)
            duration = cmd.get('duration', 2)
            success, msg = self.motors.motor_test(motor, throttle, duration)
            return CommandParser.create_response(
                ResponseCode.SUCCESS if success else ResponseCode.ERROR, msg
            )
        
        # Emergency stop
        elif cmd_name == 'emergency_stop':
            success, msg = self.motors.emergency_stop()
            return CommandParser.create_response(
                ResponseCode.EMERGENCY_STOP if success else ResponseCode.ERROR, msg
            )
        
        elif cmd_name == 'reset_estop':
            self.motors.reset_emergency_stop()
            return CommandParser.create_response(ResponseCode.SUCCESS, "E-stop reset")
        
        # Status query
        elif cmd_name == 'status':
            state = self.state.get_state()
            return CommandParser.create_response(ResponseCode.SUCCESS, "status", state)
        
        # Safety commands
        elif cmd_name == 'disable_prearm':
            token = cmd.get('token', '')
            success, msg = self.vehicle.disable_prearm_checks(token)
            code = ResponseCode.SUCCESS if success else ResponseCode.REQUIRES_TOKEN
            return CommandParser.create_response(code, msg)
        
        else:
            return CommandParser.create_response(
                ResponseCode.INVALID_COMMAND,
                f"Unknown command: {cmd_name}"
            )
    
    def _send_response(self, response: str):
        """Send response to Unity client"""
        try:
            if self.client_socket:
                self.client_socket.sendall(response.encode('utf-8'))
        except Exception as e:
            logger.error(f"Send response failed: {e}")
    
    def _cleanup(self):
        """Cleanup sockets"""
        if self.client_socket:
            try:
                self.client_socket.close()
            except:
                pass
        if self.server_socket:
            try:
                self.server_socket.close()
            except:
                pass
        logger.info("TCP server stopped")