#!/usr/bin/env python3
"""
ArduPilot Bridge Main Coordinator
Manages lifecycle and thread coordination
"""

import sys
import time
import threading
import signal
import logging
from typing import Optional

from config import Config, setup_logging
from mavlink_core import ConnectionManager, MotorController, VehicleController, VehicleState
from network import MessageHandler, TCPCommandServer

logger = logging.getLogger('jetson_bridge.main')

# ============================================================================
# MAIN BRIDGE COORDINATOR
# ============================================================================

class ArduPilotBridge:
    """Main bridge coordinator with lifecycle management"""
    
    def __init__(self, config: Config):
        self.config = config
        
        # Core components
        self.conn_manager: Optional[ConnectionManager] = None
        self.vehicle_state: Optional[VehicleState] = None
        self.vehicle_ctrl: Optional[VehicleController] = None
        self.motor_ctrl: Optional[MotorController] = None
        self.msg_handler: Optional[MessageHandler] = None
        self.tcp_server: Optional[TCPCommandServer] = None
        
        # Threads
        self.message_thread: Optional[threading.Thread] = None
        self.tcp_thread: Optional[threading.Thread] = None
        
        # Shutdown flag
        self.shutdown_requested = False
    
    def run(self) -> int:
        """Main run method - returns exit code"""
        logger.info("=" * 60)
        logger.info("ArduPilot UART-UDP Bridge with Unity TCP Interface")
        logger.info("=" * 60)
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        # Initialize components
        if not self._initialize():
            logger.error("Initialization failed")
            return 1
        
        # Connect to flight controller
        if not self._connect():
            logger.error("Connection failed")
            return 1
        
        # Start services
        if not self._start_services():
            logger.error("Service startup failed")
            return 1
        
        # Monitor and maintain
        self._monitor_loop()
        
        # Shutdown
        self._shutdown()
        
        return 0
    
    def _initialize(self) -> bool:
        """Initialize all components"""
        logger.info("Initializing components...")
        
        try:
            self.conn_manager = ConnectionManager(self.config)
            self.vehicle_state = VehicleState()
            
            return True
            
        except Exception as e:
            logger.error(f"Initialization error: {e}")
            return False
    
    def _connect(self) -> bool:
        """Connect to flight controller with retries"""
        logger.info("Connecting to flight controller...")
        
        for attempt in range(self.config.max_connection_retries):
            if self.conn_manager.connect():
                logger.info("Connected successfully")
                
                # Initialize controllers after connection
                self.vehicle_ctrl = VehicleController(
                    self.conn_manager,
                    self.config,
                    self.vehicle_state
                )
                self.motor_ctrl = MotorController(
                    self.conn_manager,
                    self.config
                )
                
                return True
            
            if attempt < self.config.max_connection_retries - 1:
                logger.warning(f"Connection attempt {attempt + 1} failed, retrying...")
                time.sleep(self.config.connection_retry_delay)
        
        logger.error("Connection failed after all retries")
        return False
    
    def _start_services(self) -> bool:
        """Start message handler and TCP server"""
        logger.info("Starting services...")
        
        try:
            # Initialize handlers
            self.msg_handler = MessageHandler(
                self.conn_manager,
                self.vehicle_state,
                self.config
            )
            
            self.tcp_server = TCPCommandServer(
                self.vehicle_ctrl,
                self.motor_ctrl,
                self.vehicle_state,
                self.config
            )
            
            # Start message forwarding thread
            self.message_thread = threading.Thread(
                target=self.msg_handler.start,
                name="MessageHandler",
                daemon=True
            )
            self.message_thread.start()
            logger.info("Message handler started")
            
            # Start TCP server thread
            self.tcp_thread = threading.Thread(
                target=self.tcp_server.start,
                name="TCPServer",
                daemon=False
            )
            self.tcp_thread.start()
            logger.info("TCP server started")
            
            return True
            
        except Exception as e:
            logger.error(f"Service startup error: {e}")
            return False
    
    def _monitor_loop(self):
        """Monitor system health and handle reconnections"""
        logger.info("Entering monitor loop")
        
        last_health_check = time.time()
        health_check_interval = 5.0
        
        try:
            while not self.shutdown_requested:
                # Check if TCP thread is still alive
                if not self.tcp_thread.is_alive():
                    logger.warning("TCP thread died")
                    break
                
                # Periodic health check
                now = time.time()
                if now - last_health_check >= health_check_interval:
                    self._health_check()
                    last_health_check = now
                
                time.sleep(1.0)
        
        except Exception as e:
            logger.error(f"Monitor loop error: {e}")
    
    def _health_check(self):
        """Perform health check"""
        if not self.conn_manager.is_connected():
            logger.warning("Connection lost during health check")
            # Message handler will attempt reconnection
        
        state = self.vehicle_state.get_state()
        heartbeat_age = state['heartbeat_age']
        
        if heartbeat_age > 5.0:
            logger.warning(f"No heartbeat for {heartbeat_age:.1f}s")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.shutdown_requested = True
        
        # Stop TCP server to unblock main thread
        if self.tcp_server:
            self.tcp_server.stop()
    
    def _shutdown(self):
        """Clean shutdown of all components"""
        logger.info("Starting shutdown sequence...")
        
        # Stop TCP server
        if self.tcp_server:
            logger.info("Stopping TCP server...")
            self.tcp_server.stop()
        
        # Stop message handler
        if self.msg_handler:
            logger.info("Stopping message handler...")
            self.msg_handler.stop()
        
        # Wait for threads with timeout
        if self.message_thread and self.message_thread.is_alive():
            logger.info("Waiting for message thread...")
            self.message_thread.join(timeout=2.0)
        
        if self.tcp_thread and self.tcp_thread.is_alive():
            logger.info("Waiting for TCP thread...")
            self.tcp_thread.join(timeout=2.0)
        
        # Clear motor overrides before disconnecting
        if self.motor_ctrl:
            logger.info("Clearing motor overrides...")
            self.motor_ctrl.clear_override()
            time.sleep(0.5)
        
        # Close connections
        if self.conn_manager:
            logger.info("Closing connections...")
            self.conn_manager.close()
        
        logger.info("Shutdown complete")

# ============================================================================
# ENTRY POINT
# ============================================================================

def main():
    """Main entry point"""
    # Load configuration
    config = Config()
    
    # Setup logging
    global logger
    logger = setup_logging(config)
    
    # Create and run bridge
    bridge = ArduPilotBridge(config)
    exit_code = bridge.run()
    
    sys.exit(exit_code)

if __name__ == "__main__":
    main()