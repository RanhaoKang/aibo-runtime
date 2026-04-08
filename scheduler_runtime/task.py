"""
Task execution management for scheduler-runtime

Handles running jobs via tmux and managing task lifecycle.
"""
import asyncio
import os
import shlex
import subprocess
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from scheduler_runtime.config import Config


class Task:
    """Represents a running task"""
    
    def __init__(self, task_id: str, job_config: Dict):
        self.task_id = task_id
        self.job_config = job_config
        requested_session_name = str(job_config.get("session_name") or "").strip()
        self.session_name = requested_session_name or f"scheduler_{task_id}_{datetime.now().strftime('%m%d_%H%M%S')}"
        self.started_at: Optional[datetime] = None
        self.completed_at: Optional[datetime] = None
        self.exit_code: Optional[int] = None
        self.cancelled = False
        self.process: Optional[asyncio.subprocess.Process] = None


class TaskManager:
    """Manages task execution"""
    
    def __init__(self, config: Config):
        self.config = config
        self.running_tasks: Dict[str, Task] = {}
        self.completed_tasks: Dict[str, Task] = {}
        self._work_dir = Path(config.work_dir).expanduser()
        self._work_dir.mkdir(parents=True, exist_ok=True)
    
    async def start(self) -> None:
        """Start task manager"""
        # Clean up any stale sessions
        await self._cleanup_stale_sessions()
    
    async def stop(self) -> None:
        """Stop task manager and cancel all tasks"""
        for task in list(self.running_tasks.values()):
            await self.cancel(task.task_id)
    
    async def execute(self, task_id: str, job_config: Dict) -> Dict:
        """Execute a task"""
        task = Task(task_id, job_config)
        
        if len(self.running_tasks) >= self.config.max_concurrent_tasks:
            raise RuntimeError("Max concurrent tasks reached")
        
        self.running_tasks[task_id] = task
        
        try:
            result = await self._run_task(task)
            return result
        finally:
            del self.running_tasks[task_id]
            self.completed_tasks[task_id] = task
    
    def _build_bash_setup(self) -> str:
        """Build bash environment setup from config"""
        lines = []
        
        # Add default env vars from config
        for key, value in self.config.env_vars.items():
            lines.append(f'export {key}="{value}"')
        
        # Add extra bashrc lines
        lines.extend(self.config.bashrc_extra)
        
        return '\n'.join(lines)
    
    async def cancel(self, task_id: str) -> bool:
        """Cancel a running task"""
        task = self.running_tasks.get(task_id)
        if not task:
            return False
        
        task.cancelled = True
        
        # Kill tmux session
        try:
            subprocess.run(
                ["tmux", "kill-session", "-t", task.session_name],
                check=False,
                capture_output=True,
            )
        except Exception:
            pass
        
        # Kill process if running directly
        if task.process and task.process.returncode is None:
            task.process.kill()
            try:
                await asyncio.wait_for(task.process.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                task.process.terminate()
        
        return True
    
    async def _run_task(self, task: Task) -> Dict:
        """Run a single task"""
        task.started_at = datetime.now()
        
        # Get job parameters
        command = task.job_config.get("command", "")
        project_path = task.job_config.get("project_path", "~")
        env = task.job_config.get("env", {})
        session_path = task.job_config.get("session_path", "")
        is_session_resume = task.job_config.get("is_session_resume", False)
        
        # Expand project path
        project_path = Path(project_path).expanduser()
        project_path.mkdir(parents=True, exist_ok=True)
        
        # Prepare environment
        task_env = os.environ.copy()
        task_env.update(env)
        
        # Get bash setup from config
        bash_setup = self._build_bash_setup()
        
        # Build command based on whether this is a session resume
        if is_session_resume and session_path:
            # Resume agent session with fork
            agent_cli = self._detect_agent_from_session(session_path)
            if agent_cli == "pi":
                # pi --fork <session_path>
                agent_cmd = f"{agent_cli} --fork {shlex.quote(session_path)}"
            elif agent_cli == "codex":
                # codex fork (if supported) or resume
                agent_cmd = f"{agent_cli} --resume {shlex.quote(session_path)}"
            else:
                agent_cmd = f"{agent_cli} {shlex.quote(session_path)}"
            
            full_command = f"{bash_setup}\n{agent_cmd}" if bash_setup else agent_cmd
        else:
            # Regular task
            full_command = f"{bash_setup}\n{command}" if bash_setup else command
        
        # Build tmux command
        tmux_cmd = self._build_tmux_command(
            session_name=task.session_name,
            command=full_command,
            cwd=project_path,
            env=task_env,
        )
        
        # Execute
        task.process = await asyncio.create_subprocess_shell(
            tmux_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        
        stdout, stderr = await task.process.communicate()
        
        task.completed_at = datetime.now()
        task.exit_code = task.process.returncode
        
        if task.cancelled:
            return {
                "status": "cancelled",
                "task_id": task.task_id,
            }
        
        return {
            "status": "completed" if task.exit_code == 0 else "failed",
            "task_id": task.task_id,
            "session_name": task.session_name,
            "exit_code": task.exit_code,
            "stdout": stdout.decode() if stdout else "",
            "stderr": stderr.decode() if stderr else "",
            "duration": (task.completed_at - task.started_at).total_seconds(),
        }
    
    def _detect_agent_from_session(self, session_path: str) -> str:
        """Detect agent CLI from session path"""
        if ".codex" in session_path.lower():
            return "codex"
        return "pi"
    
    def _build_tmux_command(
        self,
        session_name: str,
        command: str,
        cwd: Path,
        env: Dict[str, str],
    ) -> str:
        """Build tmux command for task execution"""
        # Sanitize session name
        session_name = session_name.replace(".", "_").replace(":", "_")
        
        # Build environment exports
        env_exports = " ".join(
            f"export {shlex.quote(k)}={shlex.quote(v)};"
            for k, v in env.items()
        )
        
        # Full command
        full_command = f"cd {shlex.quote(str(cwd))}; {env_exports} {command}"
        
        # tmux new-session
        tmux_cmd = f"tmux new-session -d -s {shlex.quote(session_name)} {shlex.quote(full_command)}"
        
        return tmux_cmd
    
    async def _cleanup_stale_sessions(self) -> None:
        """Clean up stale tmux sessions from previous runs"""
        try:
            result = subprocess.run(
                ["tmux", "ls"],
                capture_output=True,
                text=True,
            )
            
            if result.returncode != 0:
                return
            
            for line in result.stdout.splitlines():
                if line.startswith("scheduler_"):
                    session_name = line.split(":")[0]
                    try:
                        subprocess.run(
                            ["tmux", "kill-session", "-t", session_name],
                            check=False,
                            capture_output=True,
                        )
                    except Exception:
                        pass
        except Exception:
            pass
    
    async def exec_direct(self, command: str) -> Dict:
        """Execute a direct command (not in tmux)"""
        process = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        
        stdout, stderr = await process.communicate()
        
        return {
            "exit_code": process.returncode,
            "stdout": stdout.decode() if stdout else "",
            "stderr": stderr.decode() if stderr else "",
        }
    
    def get_session_output(self, session_name: str, lines: int = 50) -> str:
        """Get output from a tmux session"""
        try:
            result = subprocess.run(
                ["tmux", "capture-pane", "-t", session_name, "-p", "-S", f"-{lines}"],
                capture_output=True,
                text=True,
            )
            return result.stdout if result.returncode == 0 else ""
        except Exception as e:
            return f"Error: {e}"
