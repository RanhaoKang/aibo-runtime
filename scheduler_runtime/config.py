"""
Configuration management for scheduler-runtime
"""
import json
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional


@dataclass
class Config:
    """Runtime configuration"""
    
    # Connection settings
    gateway_url: str = "ws://localhost:8888/ws/runtime"
    api_key: str = ""
    
    # Machine identification
    machine_id: str = ""
    machine_name: str = ""
    
    # Timing settings
    heartbeat_interval: int = 15  # seconds
    reconnect_interval: int = 5   # seconds
    task_poll_interval: int = 3   # seconds
    
    # Concurrency settings
    max_concurrent_tasks: int = 5
    
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
    
    @classmethod
    def load(cls, path: Path) -> "Config":
        """Load config from file"""
        with open(path) as f:
            data = json.load(f)
        return cls(**data)


def load_config(path: Optional[Path] = None) -> Config:
    """Load config from file or create default"""
    if path is None:
        path = Path.home() / ".scheduler" / "runtime.json"
    
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    
    return Config.load(path)
