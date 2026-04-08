"""
Main daemon logic for scheduler-runtime

Handles WebSocket connection, heartbeat, and task execution.
"""
import asyncio
import base64
import json
import os
import subprocess
import signal
import ssl
import sys
import time
import traceback
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Set
from urllib import request as urllib_request
from urllib.error import URLError, HTTPError

import websockets
try:
    import certifi
except Exception:
    certifi = None
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
        self._pending_reports_path = self.config.pending_reports_path()
        self._pending_control_reports_path = self.config.pending_control_reports_path()
        self._control_state_path = self.config.control_state_path()
        self._pending_reports_path.parent.mkdir(parents=True, exist_ok=True)
        self._pending_control_reports_path.parent.mkdir(parents=True, exist_ok=True)
        self._control_state_path.parent.mkdir(parents=True, exist_ok=True)
        self._restart_requested = False
        
    async def run(self) -> None:
        """Main daemon loop"""
        self.running = True
        
        # Start health server
        await self.health.start()
        
        # Start task manager
        await self.tasks.start()
        poll_task = asyncio.create_task(self._poll_loop())
        
        # Main connection loop
        try:
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
                    try:
                        await asyncio.wait_for(
                            self._shutdown_event.wait(),
                            timeout=self.config.reconnect_interval
                        )
                    except asyncio.TimeoutError:
                        pass
        finally:
            poll_task.cancel()
            try:
                await poll_task
            except asyncio.CancelledError:
                pass
        
        # Cleanup
        await self.tasks.stop()
        await self.health.stop()
        if self._restart_requested:
            self._exec_self_restart()
    
    def stop(self) -> None:
        """Stop the daemon"""
        self.running = False
        self._shutdown_event.set()
        if self.ws:
            asyncio.create_task(self.ws.close())
    
    async def _connect(self) -> None:
        """Establish WebSocket connection with token in URL"""
        # Build connection URL with token
        gateway_url = self._runtime_ws_url()
        
        # Parse existing query params
        from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
        parsed = urlparse(gateway_url)
        params = parse_qs(parsed.query)
        
        # Add token and machine_id
        params['token'] = [self.config.api_key]
        params['machine_id'] = [self.config.machine_id]
        
        # Rebuild URL
        new_query = urlencode(params, doseq=True)
        connection_url = urlunparse((
            parsed.scheme, parsed.netloc, parsed.path,
            parsed.params, new_query, parsed.fragment
        ))
        
        self._log(f"Connecting to gateway...")

        connect_kwargs = {
            "ping_interval": 20,
            "ping_timeout": 10,
        }
        if parsed.scheme == "wss":
            connect_kwargs["ssl"] = self._build_ssl_context()

        self.ws = await websockets.connect(connection_url, **connect_kwargs)
        
        self.connected = True
        self._log("Connected to gateway")

    def _runtime_ws_url(self) -> str:
        from urllib.parse import urlparse, urlunparse

        parsed = urlparse(self.config.gateway_url)
        scheme = parsed.scheme.lower()
        if scheme in {"ws", "wss"}:
            ws_scheme = scheme
            ws_path = parsed.path.rstrip("/") or "/ws/runtime"
        else:
            ws_scheme = "wss" if scheme == "https" else "ws"
            ws_path = "/ws/runtime"
        return urlunparse((ws_scheme, parsed.netloc, ws_path, "", "", ""))

    def _build_ssl_context(self) -> ssl.SSLContext:
        """Build an SSL context that doesn't depend on host Python cert setup."""
        cafile = self.config.ssl_ca_file or os.environ.get("SSL_CERT_FILE")
        capath = os.environ.get("SSL_CERT_DIR")
        if cafile or capath:
            return ssl.create_default_context(cafile=cafile, capath=capath)

        try:
            if certifi is None:
                raise RuntimeError("certifi unavailable")
            return ssl.create_default_context(cafile=certifi.where())
        except Exception:
            return ssl.create_default_context()
    
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

    async def _poll_loop(self) -> None:
        while self.running:
            try:
                await self._flush_pending_reports()
                await self._flush_pending_control_reports()
                await self._poll_control_actions()
                await self._claim_next_task()
            except Exception as e:
                self._log(f"Polling error: {e}")
            await asyncio.sleep(self.config.task_poll_interval)
    
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
            "runtime_version": self._current_runtime_version(),
        }
    
    async def _handle_message(self, data: Dict) -> None:
        """Handle incoming message from gateway"""
        msg_type = data.get("type")
        
        if msg_type == "ping":
            await self.ws.send(json.dumps({"type": "pong"}))

        elif msg_type == "pong":
            return
        
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

        elif msg_type == "write_file":
            await self._handle_write_file(data)

        elif msg_type == "read_file":
            await self._handle_read_file(data)
        
        else:
            self._log(f"Unknown message type: {msg_type}")
    
    async def _handle_task_assign(self, data: Dict) -> None:
        """Handle task assignment"""
        task_id = data.get("task_id")
        job_config = data.get("job_config", {})
        
        self._log(f"Task assigned: {task_id}")
        await self._submit_task_report("task_ack", task_id)
        
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
            
            await self.tasks.execute(task_id, job_config)
            await self._submit_task_report("task_ack", task_id)
        except Exception as e:
            self._log(f"Session resume {task_id} failed: {e}")
            await self._submit_task_report("task_failed", task_id, error=str(e))
    
    def _detect_agent_from_session(self, session_path: str) -> str:
        """Detect agent CLI from session path"""
        if ".codex" in session_path.lower():
            return "codex"
        return "pi"
    
    async def _run_task(self, task_id: str, job_config: Dict) -> None:
        """Run a task"""
        try:
            await self.tasks.execute(task_id, job_config)
            await self._submit_task_report("task_ack", task_id)
        except Exception as e:
            self._log(f"Task {task_id} failed: {e}")
            await self._submit_task_report("task_failed", task_id, error=str(e))
    
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

    async def _handle_write_file(self, data: Dict) -> None:
        """Write a file on the runtime machine"""
        request_id = data.get("request_id")
        path = str(data.get("path") or "").strip()
        content_b64 = str(data.get("content_b64") or "")

        try:
            if not path:
                raise RuntimeError("Missing path")
            content = base64.b64decode(content_b64.encode())
            target = Path(os.path.expandvars(path)).expanduser()
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
            await self.ws.send(json.dumps({
                "type": "write_file_response",
                "request_id": request_id,
                "machine_id": self.config.machine_id,
                "path": str(target),
                "size": len(content),
            }))
        except Exception as e:
            await self.ws.send(json.dumps({
                "type": "write_file_response",
                "request_id": request_id,
                "machine_id": self.config.machine_id,
                "error": str(e),
            }))

    async def _handle_read_file(self, data: Dict) -> None:
        """Read a file from the runtime machine"""
        request_id = data.get("request_id")
        path = str(data.get("path") or "").strip()

        try:
            if not path:
                raise RuntimeError("Missing path")
            target = Path(os.path.expandvars(path)).expanduser()
            content = target.read_bytes()
            await self.ws.send(json.dumps({
                "type": "read_file_response",
                "request_id": request_id,
                "machine_id": self.config.machine_id,
                "path": str(target),
                "content_b64": base64.b64encode(content).decode(),
                "size": len(content),
            }))
        except Exception as e:
            await self.ws.send(json.dumps({
                "type": "read_file_response",
                "request_id": request_id,
                "machine_id": self.config.machine_id,
                "error": str(e),
            }))
    
    def _log(self, message: str) -> None:
        """Log message"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] {message}", flush=True)

    async def _post_json(self, path: str, payload: Dict) -> Dict:
        def _request() -> Dict:
            handlers = [urllib_request.ProxyHandler({})]
            if self.config.gateway_http_base().startswith("https://"):
                handlers.append(urllib_request.HTTPSHandler(context=self._build_ssl_context()))
            opener = urllib_request.build_opener(*handlers)
            req = urllib_request.Request(
                self.config.gateway_http_base() + path,
                data=json.dumps(payload).encode(),
                headers={"Content-Type": "application/json"},
            )
            with opener.open(req, timeout=10) as resp:
                return json.loads(resp.read().decode())
        return await asyncio.to_thread(_request)

    def _load_pending_reports(self) -> list[Dict]:
        if not self._pending_reports_path.exists():
            return []
        try:
            return json.loads(self._pending_reports_path.read_text(encoding="utf-8"))
        except Exception:
            return []

    def _save_pending_reports(self, reports: list[Dict]) -> None:
        self._pending_reports_path.write_text(json.dumps(reports, ensure_ascii=False, indent=2), encoding="utf-8")

    def _queue_pending_report(self, report: Dict) -> None:
        reports = self._load_pending_reports()
        reports.append(report)
        self._save_pending_reports(reports)

    async def _submit_task_report(self, event: str, task_id: str, *, result: Optional[Dict] = None, error: str = "") -> None:
        payload = {
            "token": self.config.api_key,
            "machine_id": self.config.machine_id,
            "task_id": task_id,
            "event": event,
            "result": result or {},
            "error": error,
        }
        try:
            await self._post_json("/api/runtime-tasks/report", payload)
            return
        except Exception as exc:
            self._log(f"HTTP task report {event} for {task_id} failed: {exc}")
        if self.ws and self.connected:
            try:
                ws_payload = {
                    "type": event,
                    "task_id": task_id,
                    "machine_id": self.config.machine_id,
                }
                if result:
                    ws_payload["result"] = result
                if error:
                    ws_payload["error"] = error
                await self.ws.send(json.dumps(ws_payload))
                return
            except Exception as exc:
                self._log(f"WebSocket task report {event} for {task_id} failed: {exc}")
        self._log(f"Deferred task report {event} for {task_id}")
        self._queue_pending_report(payload)

    async def _submit_control_report(self, action_id: str, event: str, *, result: Optional[Dict] = None, error: str = "") -> None:
        payload = {
            "token": self.config.api_key,
            "machine_id": self.config.machine_id,
            "action_id": action_id,
            "event": event,
            "result": result or {},
            "error": error,
        }
        try:
            await self._post_json("/api/runtime-control/report", payload)
            return
        except Exception as exc:
            self._log(f"HTTP control report {event} for {action_id} failed: {exc}")
        self._log(f"Deferred control report {event} for {action_id}")
        self._queue_pending_control_report(payload)

    async def _flush_pending_reports(self) -> None:
        reports = self._load_pending_reports()
        if not reports:
            return
        remaining: list[Dict] = []
        for report in reports:
            try:
                await self._post_json("/api/runtime-tasks/report", report)
            except Exception:
                remaining.append(report)
        if len(remaining) != len(reports):
            self._save_pending_reports(remaining)

    async def _flush_pending_control_reports(self) -> None:
        reports = self._load_pending_control_reports()
        if not reports:
            return
        remaining: list[Dict] = []
        for report in reports:
            try:
                await self._post_json("/api/runtime-control/report", report)
            except Exception:
                remaining.append(report)
        if len(remaining) != len(reports):
            self._save_pending_control_reports(remaining)

    async def _poll_control_actions(self) -> None:
        payload = {
            "token": self.config.api_key,
            "machine_id": self.config.machine_id,
            "status": await self._get_machine_status(),
        }
        try:
            response = await self._post_json("/api/runtime-control/poll", payload)
        except Exception:
            return
        action = response.get("action") or {}
        action_id = str(action.get("id") or "").strip()
        if not action_id:
            return
        await self._handle_control_action(action)

    async def _handle_control_action(self, action: Dict) -> None:
        action_id = str(action.get("id") or "").strip()
        action_type = str(action.get("type") or "").strip()
        if not action_id or not action_type:
            return
        if action_type != "self_update":
            self._log(f"Ignoring unsupported control action: {action_type}")
            return
        state = self._load_control_state()
        if state.get("last_applied_action_id") == action_id:
            await self._submit_control_report(
                action_id,
                "completed",
                result={"version": self._current_runtime_version()},
            )
            return
        try:
            version = self._apply_self_update(dict(action.get("payload") or {}))
            state["last_applied_action_id"] = action_id
            state["last_applied_version"] = version
            self._save_control_state(state)
            await self._submit_control_report(
                action_id,
                "completed",
                result={"version": version},
            )
            self._log(f"Applied self update {action_id}, restarting runtime")
            self._restart_requested = True
            self.stop()
        except Exception as exc:
            self._log(f"Self update {action_id} failed: {exc}")
            await self._submit_control_report(action_id, "failed", error=str(exc))

    async def _claim_next_task(self) -> None:
        if self._restart_requested:
            return
        if self.tasks.running_tasks:
            return
        if len(self.tasks.running_tasks) >= self.config.max_concurrent_tasks:
            return
        payload = {
            "token": self.config.api_key,
            "machine_id": self.config.machine_id,
            "status": await self._get_machine_status(),
        }
        try:
            response = await self._post_json("/api/runtime-tasks/claim", payload)
        except Exception:
            return
        task = response.get("task") or {}
        task_id = str(task.get("task_id") or "").strip()
        if not task_id:
            return
        job_config = dict(task.get("job_config") or {})
        self._log(f"Claimed task: {task_id}")
        asyncio.create_task(self._run_task(task_id, job_config))

    def _load_pending_control_reports(self) -> list[Dict]:
        try:
            with open(self._pending_control_reports_path, "r", encoding="utf-8") as f:
                payload = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return []
        return payload if isinstance(payload, list) else []

    def _save_pending_control_reports(self, reports: list[Dict]) -> None:
        if not reports:
            try:
                self._pending_control_reports_path.unlink()
            except FileNotFoundError:
                pass
            return
        with open(self._pending_control_reports_path, "w", encoding="utf-8") as f:
            json.dump(reports, f, ensure_ascii=False, indent=2)

    def _queue_pending_control_report(self, payload: Dict) -> None:
        reports = self._load_pending_control_reports()
        reports.append(payload)
        self._save_pending_control_reports(reports)

    def _load_control_state(self) -> Dict:
        try:
            with open(self._control_state_path, "r", encoding="utf-8") as f:
                payload = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return {}
        return payload if isinstance(payload, dict) else {}

    def _save_control_state(self, payload: Dict) -> None:
        with open(self._control_state_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    def _apply_self_update(self, payload: Dict) -> str:
        repo_url = str(payload.get("repo_url") or "").strip()
        ref = str(payload.get("ref") or "").strip() or "main"
        repo_root = self.config.repo_root()
        if not (repo_root / ".git").exists():
            raise RuntimeError(f"Runtime repo root is not a git checkout: {repo_root}")

        self._ensure_git_remote(repo_root, repo_url)
        self._run_update_command(["git", "fetch", "origin", "--tags"], cwd=repo_root)
        remote_branch = subprocess.run(
            ["git", "rev-parse", "--verify", f"refs/remotes/origin/{ref}"],
            cwd=repo_root,
            check=False,
            capture_output=True,
            text=True,
        )
        if remote_branch.returncode == 0:
            local_branch = subprocess.run(
                ["git", "rev-parse", "--verify", f"refs/heads/{ref}"],
                cwd=repo_root,
                check=False,
                capture_output=True,
                text=True,
            )
            if local_branch.returncode == 0:
                self._run_update_command(["git", "checkout", ref], cwd=repo_root)
            else:
                self._run_update_command(["git", "checkout", "-B", ref, f"origin/{ref}"], cwd=repo_root)
            self._run_update_command(["git", "pull", "--ff-only", "origin", ref], cwd=repo_root)
        else:
            self._run_update_command(["git", "checkout", ref], cwd=repo_root)
        self._run_update_command([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"], cwd=repo_root)
        version = self._current_runtime_version()
        if not version:
            raise RuntimeError("Failed to determine runtime version after update")
        return version

    def _ensure_git_remote(self, repo_root: Path, repo_url: str) -> None:
        if not repo_url:
            raise RuntimeError("Missing runtime update repo_url")
        remote = subprocess.run(
            ["git", "remote", "get-url", "origin"],
            cwd=repo_root,
            check=False,
            capture_output=True,
            text=True,
        )
        if remote.returncode == 0:
            current = remote.stdout.strip()
            if current != repo_url:
                self._run_update_command(["git", "remote", "set-url", "origin", repo_url], cwd=repo_root)
            return
        self._run_update_command(["git", "remote", "add", "origin", repo_url], cwd=repo_root)

    def _run_update_command(self, command: list[str], *, cwd: Path) -> None:
        result = subprocess.run(command, cwd=cwd, check=False, capture_output=True, text=True)
        if result.returncode != 0:
            stderr = (result.stderr or result.stdout or "").strip()
            raise RuntimeError(f"{' '.join(command)} failed: {stderr}")

    def _current_runtime_version(self) -> str:
        repo_root = self.config.repo_root()
        if not (repo_root / ".git").exists():
            return ""
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            return ""
        return result.stdout.strip()

    def _exec_self_restart(self) -> None:
        argv = [sys.executable, "-m", "scheduler_runtime", "start"]
        if self.config.config_path:
            argv.extend(["--config", self.config.config_path])
        if self.config.foreground:
            argv.append("--foreground")
        os.execv(sys.executable, argv)
