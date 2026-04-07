"""
Mock Gateway for testing scheduler-runtime

Simulates the scheduler gateway WebSocket server for local testing.
"""
import asyncio
import json
import logging
import uuid
from datetime import datetime
from typing import Dict, Set

import websockets
from websockets.server import WebSocketServerProtocol

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mock_gateway")


class MockGateway:
    """Mock scheduler gateway for testing"""
    
    def __init__(self, host: str = "localhost", port: int = 8889):
        self.host = host
        self.port = port
        self.clients: Dict[str, WebSocketServerProtocol] = {}
        self.machine_status: Dict[str, dict] = {}
        self.running = False
        
    async def start(self):
        """Start the mock gateway"""
        self.running = True
        logger.info(f"Starting mock gateway on ws://{self.host}:{self.port}")
        
        async with websockets.serve(
            self._handle_client,
            self.host,
            self.port,
        ):
            logger.info(f"Mock gateway listening on ws://{self.host}:{self.port}")
            
            # Keep running
            while self.running:
                await asyncio.sleep(1)
    
    def stop(self):
        """Stop the gateway"""
        self.running = False
        
    async def _handle_client(self, websocket: WebSocketServerProtocol, path: str):
        """Handle client connection"""
        # Get headers
        headers = dict(websocket.request_headers)
        machine_id = headers.get("X-Machine-ID", "unknown")
        api_key = headers.get("X-API-Key", "")
        
        logger.info(f"Client connected: {machine_id} from {websocket.remote_address}")
        
        if api_key != "test-api-key":
            logger.warning(f"Invalid API key from {machine_id}")
            await websocket.close(1008, "Invalid API key")
            return
        
        self.clients[machine_id] = websocket
        
        try:
            async for message in websocket:
                try:
                    data = json.loads(message)
                    await self._handle_message(machine_id, websocket, data)
                except json.JSONDecodeError:
                    logger.error(f"Invalid JSON from {machine_id}: {message}")
                except Exception as e:
                    logger.error(f"Error handling message: {e}")
        except websockets.exceptions.ConnectionClosed:
            logger.info(f"Client disconnected: {machine_id}")
        finally:
            if machine_id in self.clients:
                del self.clients[machine_id]
            if machine_id in self.machine_status:
                del self.machine_status[machine_id]
    
    async def _handle_message(self, machine_id: str, websocket: WebSocketServerProtocol, data: dict):
        """Handle incoming message"""
        msg_type = data.get("type")
        
        if msg_type == "heartbeat":
            self.machine_status[machine_id] = {
                "timestamp": data.get("timestamp"),
                "status": data.get("status", {}),
            }
            logger.info(f"Heartbeat from {machine_id}: {data.get('status', {}).get('tasks_running', 0)} tasks")
        
        elif msg_type == "task_ack":
            logger.info(f"Task acknowledged by {machine_id}: {data.get('task_id')}")
        
        elif msg_type == "task_complete":
            logger.info(f"Task completed by {machine_id}: {data.get('task_id')} - {data.get('result', {}).get('status')}")
        
        elif msg_type == "task_failed":
            logger.error(f"Task failed by {machine_id}: {data.get('task_id')} - {data.get('error')}")
        
        elif msg_type == "pong":
            pass  # Ignore pongs
        
        else:
            logger.info(f"Unknown message type from {machine_id}: {msg_type}")
    
    async def send_task(self, machine_id: str, task_config: dict) -> bool:
        """Send a task to a machine"""
        if machine_id not in self.clients:
            logger.error(f"Machine not connected: {machine_id}")
            return False
        
        task_id = str(uuid.uuid4())[:8]
        message = {
            "type": "task_assign",
            "task_id": task_id,
            "job_config": task_config,
            "timestamp": datetime.utcnow().isoformat(),
        }
        
        try:
            await self.clients[machine_id].send(json.dumps(message))
            logger.info(f"Task sent to {machine_id}: {task_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to send task: {e}")
            return False
    
    async def send_session(self, machine_id: str, session_path: str, env_vars: dict = None) -> bool:
        """Send a session to resume on a machine"""
        if machine_id not in self.clients:
            logger.error(f"Machine not connected: {machine_id}")
            return False
        
        task_id = str(uuid.uuid4())[:8]
        message = {
            "type": "session_resume",
            "task_id": task_id,
            "session_path": session_path,
            "env_vars": env_vars or {},
            "timestamp": datetime.utcnow().isoformat(),
        }
        
        try:
            await self.clients[machine_id].send(json.dumps(message))
            logger.info(f"Session sent to {machine_id}: {session_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to send session: {e}")
            return False
    
    async def request_screenshot(self, machine_id: str) -> bool:
        """Request screenshot from a machine"""
        if machine_id not in self.clients:
            logger.error(f"Machine not connected: {machine_id}")
            return False
        
        message = {
            "type": "screenshot_request",
            "request_id": str(uuid.uuid4()),
        }
        
        try:
            await self.clients[machine_id].send(json.dumps(message))
            logger.info(f"Screenshot requested from {machine_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to request screenshot: {e}")
            return False
    
    def list_machines(self) -> list:
        """List connected machines"""
        return [
            {
                "machine_id": mid,
                "status": self.machine_status.get(mid, {}),
            }
            for mid in self.clients.keys()
        ]


async def interactive_control(gateway: MockGateway):
    """Interactive control for the mock gateway"""
    print("\n" + "="*50)
    print("Mock Gateway Control")
    print("="*50)
    print("Commands:")
    print("  list                    - List connected machines")
    print("  task <machine_id> <cmd> - Send task to machine")
    print("  session <machine_id> <path> - Send session to machine")
    print("  screenshot <machine_id> - Request screenshot")
    print("  quit                    - Stop gateway")
    print("="*50 + "\n")
    
    while gateway.running:
        try:
            command = await asyncio.get_event_loop().run_in_executor(
                None, lambda: input("gateway> ")
            )
            command = command.strip()
            
            if not command:
                continue
            
            parts = command.split(maxsplit=2)
            cmd = parts[0].lower()
            
            if cmd == "list":
                machines = gateway.list_machines()
                if not machines:
                    print("No machines connected")
                else:
                    for m in machines:
                        status = m["status"].get("status", {})
                        print(f"  {m['machine_id']}: {status.get('tasks_running', 0)} tasks, CPU {status.get('cpu_percent', 0)}%")
            
            elif cmd == "task" and len(parts) >= 3:
                machine_id = parts[1]
                command_str = parts[2]
                await gateway.send_task(machine_id, {
                    "command": command_str,
                    "project_path": "~/scheduler_work",
                })
            
            elif cmd == "session" and len(parts) >= 3:
                machine_id = parts[1]
                session_path = parts[2]
                await gateway.send_session(machine_id, session_path, {
                    "TEST_VAR": "hello_from_gateway",
                })
            
            elif cmd == "screenshot" and len(parts) >= 2:
                machine_id = parts[1]
                await gateway.request_screenshot(machine_id)
            
            elif cmd == "quit":
                gateway.stop()
                break
            
            else:
                print(f"Unknown command: {cmd}")
        
        except EOFError:
            break
        except Exception as e:
            print(f"Error: {e}")


async def main():
    """Main entry point"""
    gateway = MockGateway()
    
    # Start gateway and interactive control
    await asyncio.gather(
        gateway.start(),
        interactive_control(gateway),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nGateway stopped")
