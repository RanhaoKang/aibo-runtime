"""
Configuration management for scheduler-runtime
"""
import json
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse


def runtime_state_dir() -> Path:
    return Path.home() / ".aibo"


@dataclass
class Config:
    """Runtime configuration"""
    
    # Connection settings
    gateway_url: str = "ws://localhost:8888/ws/runtime"
    api_key: str = ""
    ssl_ca_file: Optional[str] = None
    
    # Machine identification
    machine_id: str = ""
    machine_name: str = ""
    
    # Timing settings
    heartbeat_interval: int = 15  # seconds
    reconnect_interval: int = 5   # seconds
    task_poll_interval: int = 3   # seconds
    
    # Concurrency settings
    max_concurrent_tasks: int = 1
    
    # Paths
    work_dir: str = "~/scheduler_work"
    
    # Features
    enable_screenshot: bool = True
    enable_gui_automation: bool = False
    
    # Bash environment
    bashrc_extra: list = field(default_factory=list)  # Extra lines to add to bashrc
    env_vars: dict = field(default_factory=dict)      # Default environment variables
    
    # Runtime state (not persisted)
    foreground: bool = field(default=False, repr=False)
    config_path: str = field(default="", repr=False)
    
    def __post_init__(self):
        if not self.machine_id:
            import os
            self.machine_id = os.uname().nodename
        if not self.machine_name:
            self.machine_name = self.machine_id
    
    def save(self, path: Path) -> None:
        """Save config to file"""
        # Don't save runtime state
        data = asdict(self)
        del data['foreground']
        del data['config_path']
        
        with open(path, 'w') as f:
            json.dump(data, f, indent=2)
    
    def get_bash_env_setup(self) -> str:
        """Generate bash environment setup script"""
        lines = []
        
        # Add default env vars
        for key, value in self.env_vars.items():
            lines.append(f'export {key}="{value}"')
        
        # Add extra bashrc lines
        lines.extend(self.bashrc_extra)
        
        return '\n'.join(lines)

    def gateway_http_base(self) -> str:
        parsed = urlparse(self.gateway_url)
        scheme = "https" if parsed.scheme in {"https", "wss"} else "http"
        return f"{scheme}://{parsed.netloc}"

    def pending_reports_path(self) -> Path:
        return runtime_state_dir() / "runtime-pending-reports.json"

    def pending_control_reports_path(self) -> Path:
        return runtime_state_dir() / "runtime-pending-control-reports.json"

    def control_state_path(self) -> Path:
        return runtime_state_dir() / "runtime-control-state.json"

    def repo_root(self) -> Path:
        return Path(__file__).resolve().parents[1]
    
    @classmethod
    def load(cls, path: Path) -> "Config":
        """Load config from file"""
        with open(path) as f:
            data = json.load(f)
        config = cls(**data)
        config.config_path = str(path)
        return config


def load_config(path: Optional[Path] = None) -> Config:
    """Load config from file or create default"""
    if path is None:
        path = runtime_state_dir() / "runtime.json"
    
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    
    config = Config.load(path)
    config.config_path = str(path)
    return config
