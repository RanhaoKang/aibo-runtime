"""
Task execution management for scheduler-runtime

Handles running jobs via tmux and managing task lifecycle.
"""
import asyncio
import base64
import os
import shlex
import subprocess
import tempfile
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

        full_command, project_path, task_env = self._prepare_task_execution(task)
        
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

    def _build_command_with_prompt(self, command: str, prompt: str) -> str:
        if not prompt:
            return command
        try:
            parts = shlex.split(command)
        except ValueError:
            parts = command.split()

        executable = ""
        if parts[:2] == ["uv", "run"] and len(parts) >= 3:
            executable = Path(parts[2]).name.lower()
        elif parts:
            executable = Path(parts[0]).name.lower()

        prompt_text = prompt
        if prompt_text.startswith("-"):
            prompt_text = f" {prompt_text}"
        quoted_prompt = shlex.quote(prompt_text)
        if executable in {"pi", "kimi"}:
            return f"{command} -- {quoted_prompt}"
        return f"{command} {quoted_prompt}"

    def _build_shell_wrapped_command(self, command: str) -> str:
        shell_path = (os.environ.get("SHELL") or "").strip()
        shell_name = Path(shell_path).name if shell_path else ""
        if shell_name == "zsh":
            shell_bin = "zsh"
            rc_file = "~/.zshrc"
        else:
            shell_bin = "bash"
            rc_file = "~/.bashrc"
        inner_cmd = f"source {rc_file} 2>/dev/null || true; source ~/.bash_profile || true ; {command}"
        return f"{shell_bin} -c {shlex.quote(inner_cmd)}"

    def _materialize_attachments(self, task_id: str, attachments: List[Dict]) -> List[Dict[str, str]]:
        if not attachments:
            return []
        root = Path(tempfile.gettempdir()) / "aibo-attachments" / task_id
        root.mkdir(parents=True, exist_ok=True)
        materialized: List[Dict[str, str]] = []
        for index, attachment in enumerate(attachments, start=1):
            filename = Path(str(attachment.get("filename") or f"attachment-{index}")).name or f"attachment-{index}"
            payload = str(attachment.get("content_b64") or "").strip()
            if not payload:
                continue
            target = root / filename
            target.write_bytes(base64.b64decode(payload.encode()))
            materialized.append(
                {
                    "filename": filename,
                    "path": str(target),
                    "source_url": str(attachment.get("source_url") or ""),
                    "content_type": str(attachment.get("content_type") or ""),
                }
            )
        return materialized

    def _build_attachment_prompt(self, attachments: List[Dict[str, str]]) -> str:
        if not attachments:
            return ""
        lines = ["Attached files on this runtime:"]
        for attachment in attachments:
            line = f"- {attachment['filename']}: {attachment['path']}"
            if attachment.get("source_url"):
                line += f" (from {attachment['source_url']})"
            lines.append(line)
        return "\n".join(lines)

    def _prepare_task_execution(self, task: Task) -> tuple[str, Path, Dict[str, str]]:
        command = task.job_config.get("command", "")
        prompt = str(task.job_config.get("prompt") or "")
        project_path = task.job_config.get("project_path", "~")
        env = task.job_config.get("env", {})
        session_path = task.job_config.get("session_path", "")
        is_session_resume = task.job_config.get("is_session_resume", False)
        attachments = self._materialize_attachments(task.task_id, list(task.job_config.get("attachments") or []))

        project_path = Path(project_path).expanduser()
        project_path.mkdir(parents=True, exist_ok=True)

        task_env = os.environ.copy()
        task_env.update(env)

        bash_setup = self._build_bash_setup()

        if is_session_resume and session_path:
            agent_cli = self._detect_agent_from_session(session_path)
            if agent_cli == "pi":
                base_command = f"{agent_cli} --fork {shlex.quote(session_path)}"
            elif agent_cli == "codex":
                base_command = f"{agent_cli} --resume {shlex.quote(session_path)}"
            else:
                base_command = f"{agent_cli} {shlex.quote(session_path)}"
            full_command = f"{bash_setup}\n{base_command}" if bash_setup else base_command
            return full_command, project_path, task_env

        attachment_note = self._build_attachment_prompt(attachments)
        if attachment_note:
            prompt = f"{prompt}\n\n{attachment_note}".strip() if prompt else attachment_note

        if "prompt" in task.job_config or attachments:
            command_with_prompt = self._build_command_with_prompt(command, prompt)
            command_body = f"{bash_setup}\n{command_with_prompt}" if bash_setup else command_with_prompt
            full_command = self._build_shell_wrapped_command(command_body)
        else:
            full_command = f"{bash_setup}\n{command}" if bash_setup else command

        return full_command, project_path, task_env
    
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
