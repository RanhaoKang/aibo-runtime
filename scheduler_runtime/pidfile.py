"""
PID file management for daemon process
"""
import os
from pathlib import Path


class PidFileError(Exception):
    """PID file error"""
    pass


class PidFile:
    """Manages PID file for daemon"""
    
    def __init__(self, path: Path = None):
        if path is None:
            path = Path.home() / ".scheduler" / "runtime.pid"
        self.path = path
        self._pid: int = None
    
    def _get_pid_path(self) -> Path:
        """Get PID file path"""
        return self.path
    
    def read(self) -> int:
        """Read PID from file"""
        try:
            with open(self._get_pid_path()) as f:
                return int(f.read().strip())
        except (FileNotFoundError, ValueError):
            return None
    
    def acquire(self) -> None:
        """Acquire PID file (write current PID)"""
        pid = os.getpid()
        
        # Check if already running
        existing_pid = self.read()
        if existing_pid:
            try:
                os.kill(existing_pid, 0)
                raise PidFileError(f"Process already running (PID: {existing_pid})")
            except ProcessLookupError:
                # Stale PID file
                pass
        
        # Write PID file
        self._get_pid_path().parent.mkdir(parents=True, exist_ok=True)
        with open(self._get_pid_path(), 'w') as f:
            f.write(str(pid))
        
        self._pid = pid
    
    def release(self) -> None:
        """Release PID file (delete it)"""
        try:
            self._get_pid_path().unlink()
        except FileNotFoundError:
            pass
        self._pid = None
