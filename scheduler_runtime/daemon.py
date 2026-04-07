"""
Main daemon logic for scheduler-runtime

Handles WebSocket connection, heartbeat, and task execution.
"""
import asyncio
import json
import os
import signal
import sys
import time
import traceback
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Set

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

from scheduler_runtime.config import Config
from scheduler_runtime.task import TaskManager
from scheduler_runtime.health import HealthServer


class Daemon:
    """Scheduler runtime daemon"""
    
    def __init__(self, config: Config):
        self.config = config
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.running = False
        self.connected = False
        self.last_heartbeat = 0
        self.tasks: TaskManager = TaskManager(config)
        self.health = HealthServer(config)
        self._shutdown_event = asyncio.Event()
        
    async def run(self) -> None:
        """Main daemon loop"""
        self.running = True
        
        # Start health server
        await self.health.start()
        
        # Start task manager
        await self.tasks.start()
        
        # Main connection loop
        while self.running:
            try:
                await self._connect()
                await self._handle_connection()
            except ConnectionClosed:
                self.connected = False
                self._log("Connection closed, reconnecting...")
            except WebSocketException as e:
                self.connected = False
                self._log(f"WebSocket error: {e}")
            except Exception as e:
                self.connected = False
                self._log(f"Unexpected error: {e}")
                traceback.print_exc()
            
            if self.running:
                # Wait before reconnecting
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(),
                        timeout=self.config.reconnect_interval
                    )
                except asyncio.TimeoutError:
                    pass
        
        # Cleanup
        await self.tasks.stop()
        await self.health.stop()
    
    def stop(self) -> None:
        """Stop the daemon"""
        self.running = False
        self._shutdown_event.set()
        if self.ws:
            asyncio.create_task(self.ws.close())
    
    async def _connect(self) -> None:
        """Establish WebSocket connection"""
        headers = {
            "X-Machine-ID": self.config.machine_id,
            "X-API-Key": self.config.api_key,
        }
        
        self._log(f"Connecting to {self.config.gateway_url}...")
        
        self.ws = await websockets.connect(
            self.config.gateway_url,
            extra_headers=headers,
            ping_interval=20,
            ping_timeout=10,
        )
        
        self.connected = True
        self._log("Connected to gateway")
    
    async def _handle_connection(self) -> None:
        """Handle connected WebSocket"""
        # Start heartbeat task
        heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        
        try:
            async for message in self.ws:
                try:
                    data = json.loads(message)
                    await self._handle_message(data)
                except json.JSONDecodeError:
                    self._log(f"Invalid JSON: {message}")
                except Exception as e:
                    self._log(f"Error handling message: {e}")
                    traceback.print_exc()
        finally:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass
    
    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeats"""
        while self.running and self.connected:
            try:
                await self._send_heartbeat()
                await asyncio.sleep(self.config.heartbeat_interval)
            except Exception as e:
                self._log(f"Heartbeat error: {e}")
                break
    
    async def _send_heartbeat(self) -> None:
        """Send heartbeat message"""
        if not self.ws:
            return
        
        status = await self._get_machine_status()
        
        message = {
            "type": "heartbeat",
            "timestamp": datetime.utcnow().isoformat(),
            "machine_id": self.config.machine_id,
            "status": status,
        }
        
        await self.ws.send(json.dumps(message))
        self.last_heartbeat = time.time()
    
    async def _get_machine_status(self) -> Dict:
        """Get current machine status"""
        import psutil
        
        return {
            "cpu_percent": psutil.cpu_percent(interval=0.1),
            "memory": {
                "used": psutil.virtual_memory().used,
                "total": psutil.virtual_memory().total,
            },
            "disk": {
                "used": psutil.disk_usage('/').used,
                "total": psutil.disk_usage('/').total,
            },
            "tasks_running": len(self.tasks.running_tasks),
        }
    
    async def _handle_message(self, data: Dict) -> None:
        """Handle incoming message from gateway"""
        msg_type = data.get("type")
        
        if msg_type == "ping":
            await self.ws.send(json.dumps({"type": "pong"}))
        
        elif msg_type == "task_assign":
            await self._handle_task_assign(data)
        
        elif msg_type == "session_resume":
            await self._handle_session_resume(data)
        
        elif msg_type == "task_cancel":
            await self._handle_task_cancel(data)
        
        elif msg_type == "screenshot_request":
            await self._handle_screenshot_request(data)
        
        elif msg_type == "exec_command":
            await self._handle_exec_command(data)
        
        else:
            self._log(f"Unknown message type: {msg_type}")
    
    async def _handle_task_assign(self, data: Dict) -> None:
        """Handle task assignment"""
        task_id = data.get("task_id")
        job_config = data.get("job_config", {})
        
        self._log(f"Task assigned: {task_id}")
        
        # Acknowledge
        await self.ws.send(json.dumps({
            "type": "task_ack",
            "task_id": task_id,
            "machine_id": self.config.machine_id,
        }))
        
        # Start task
        asyncio.create_task(self._run_task(task_id, job_config))
    
    async def _handle_session_resume(self, data: Dict) -> None:
        """Handle session resume (fork agent session on this machine)"""
        task_id = data.get("task_id")
        session_path = data.get("session_path")
        env_vars = data.get("env_vars", {})
        
        self._log(f"Session resume requested: {task_id} -> {session_path}")
        
        # Acknowledge
        await self.ws.send(json.dumps({
            "type": "task_ack",
            "task_id": task_id,
            "machine_id": self.config.machine_id,
        }))
        
        # Start session resume task
        asyncio.create_task(self._run_session_resume(task_id, session_path, env_vars))
    
    async def _run_session_resume(self, task_id: str, session_path: str, env_vars: Dict) -> None:
        """Resume an agent session on this machine"""
        try:
            # Detect agent type from session path
            agent_cli = self._detect_agent_from_session(session_path)
            
            job_config = {
                "command": agent_cli,
                "project_path": "~",
                "env": env_vars,
                "session_path": session_path,
                "is_session_resume": True,
            }
            
            result = await self.tasks.execute(task_id, job_config)
            
            # Report completion
            await self.ws.send(json.dumps({
                "type": "task_complete",
                "task_id": task_id,
                "machine_id": self.config.machine_id,
                "result": result,
            }))
        except Exception as e:
            self._log(f"Session resume {task_id} failed: {e}")
            
            # Report failure
            await self.ws.send(json.dumps({
                "type": "task_failed",
                "task_id": task_id,
                "machine_id": self.config.machine_id,
                "error": str(e),
            }))
    
    def _detect_agent_from_session(self, session_path: str) -> str:
        """Detect agent CLI from session path"""
        if ".codex" in session_path.lower():
            return "codex"
        return "pi"
    
    async def _run_task(self, task_id: str, job_config: Dict) -> None:
        """Run a task"""
        try:
            result = await self.tasks.execute(task_id, job_config)
            
            # Report completion
            await self.ws.send(json.dumps({
                "type": "task_complete",
                "task_id": task_id,
                "machine_id": self.config.machine_id,
                "result": result,
            }))
        except Exception as e:
            self._log(f"Task {task_id} failed: {e}")
            
            # Report failure
            await self.ws.send(json.dumps({
                "type": "task_failed",
                "task_id": task_id,
                "machine_id": self.config.machine_id,
                "error": str(e),
            }))
    
    async def _handle_task_cancel(self, data: Dict) -> None:
        """Handle task cancellation"""
        task_id = data.get("task_id")
        self._log(f"Canceling task: {task_id}")
        await self.tasks.cancel(task_id)
    
    async def _handle_screenshot_request(self, data: Dict) -> None:
        """Handle screenshot request"""
        request_id = data.get("request_id")
        
        try:
            screenshot = await self._capture_screenshot()
            
            await self.ws.send(json.dumps({
                "type": "screenshot_response",
                "request_id": request_id,
                "machine_id": self.config.machine_id,
                "screenshot": screenshot,
            }))
        except Exception as e:
            self._log(f"Screenshot failed: {e}")
            
            await self.ws.send(json.dumps({
                "type": "screenshot_response",
                "request_id": request_id,
                "machine_id": self.config.machine_id,
                "error": str(e),
            }))
    
    async def _capture_screenshot(self) -> str:
        """Capture screenshot and return as base64"""
        import base64
        import subprocess
        
        # Try different screenshot methods based on platform
        screenshot_path = f"/tmp/scheduler_screenshot_{uuid.uuid4().hex}.png"
        
        if sys.platform == "darwin":
            # macOS
            subprocess.run(
                ["screencapture", "-x", screenshot_path],
                check=True,
                capture_output=True,
            )
        elif sys.platform == "linux":
            # Linux with X11
            try:
                subprocess.run(
                    ["gnome-screenshot", "-f", screenshot_path],
                    check=True,
                    capture_output=True,
                )
            except:
                # Try import-py or other methods
                import pyautogui
                screenshot = pyautogui.screenshot()
                screenshot.save(screenshot_path)
        else:
            raise RuntimeError(f"Screenshot not supported on {sys.platform}")
        
        # Read and encode
        with open(screenshot_path, "rb") as f:
            data = f.read()
        
        # Cleanup
        os.unlink(screenshot_path)
        
        return base64.b64encode(data).decode()
    
    async def _handle_exec_command(self, data: Dict) -> None:
        """Handle direct command execution"""
        request_id = data.get("request_id")
        command = data.get("command")
        
        self._log(f"Executing command: {command}")
        
        try:
            result = await self.tasks.exec_direct(command)
            
            await self.ws.send(json.dumps({
                "type": "exec_response",
                "request_id": request_id,
                "machine_id": self.config.machine_id,
                "result": result,
            }))
        except Exception as e:
            await self.ws.send(json.dumps({
                "type": "exec_response",
                "request_id": request_id,
                "machine_id": self.config.machine_id,
                "error": str(e),
            }))
    
    def _log(self, message: str) -> None:
        """Log message"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] {message}", flush=True)
