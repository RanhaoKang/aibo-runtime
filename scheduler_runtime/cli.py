"""
CLI commands for scheduler-runtime
"""
import argparse
import asyncio
import json
import os
import signal
import sys
import time
from pathlib import Path

from scheduler_runtime.config import Config, load_config, runtime_state_dir
from scheduler_runtime.daemon import Daemon
from scheduler_runtime.pidfile import PidFile, PidFileError


def get_default_config_path() -> Path:
    """Get default config file path"""
    return runtime_state_dir() / "runtime.json"


def cmd_connect(args: argparse.Namespace) -> int:
    """Connect to gateway using token URL"""
    # Parse the connection URL
    try:
        gateway_url, token, suggested_machine_id = parse_connection_url(args.url)
    except Exception as e:
        print(f"Invalid connection URL: {e}")
        print("URL format: ws://host:port/path?token=xxx&machine_id=yyy")
        return 1
    
    if not token:
        print("Error: No token found in URL")
        return 1
    
    if not gateway_url:
        print("Error: No gateway URL found")
        return 1
    
    # Use suggested machine ID or generate one
    machine_id = suggested_machine_id or os.uname().nodename
    machine_name = args.name or machine_id
    
    # Create or update config
    config_path = get_default_config_path()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Load existing config if present
    if config_path.exists():
        try:
            config = Config.load(config_path)
            print(f"Updating existing config: {config_path}")
        except Exception:
            config = Config()
    else:
        config = Config()
        print(f"Creating new config: {config_path}")
    
    # Update with connection details
    config.gateway_url = gateway_url
    config.api_key = token
    config.machine_id = machine_id
    config.machine_name = machine_name
    config.foreground = args.foreground
    config.config_path = str(config_path)
    
    # Save config
    config.save(config_path)
    
    print(f"\n✓ Connected to: {gateway_url}")
    print(f"✓ Machine ID: {machine_id}")
    print(f"✓ Config saved to: {config_path}")
    print()
    
    if args.foreground:
        print("Starting runtime in foreground mode...")
        print("Press Ctrl+C to stop\n")
    else:
        print("Starting runtime in background mode...")
    
    # Start the daemon
    return _start_daemon(config, foreground=args.foreground)


def _start_daemon(config: Config, foreground: bool = False) -> int:
    """Start the daemon with given config"""
    pid_file = PidFile()
    
    if not foreground:
        # Daemonize
        try:
            pid_file.acquire()
        except PidFileError as e:
            print(f"Runtime already running: {e}")
            return 1
        
        # Fork to background
        if os.fork() > 0:
            # Parent exits
            time.sleep(0.5)
            pid = pid_file.read()
            print(f"Runtime started (PID: {pid})")
            print(f"Logs: ~/.aibo/logs/runtime.log")
            return 0
        
        # Child process
        os.setsid()
        os.umask(0)
        
        # Second fork
        if os.fork() > 0:
            sys.exit(0)
        
        # Redirect stdout/stderr to log file
        log_dir = runtime_state_dir() / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "runtime.log"
        
        sys.stdout.flush()
        sys.stderr.flush()
        
        with open(log_file, 'a') as f:
            os.dup2(f.fileno(), sys.stdout.fileno())
            os.dup2(f.fileno(), sys.stderr.fileno())
    
    # Run daemon
    daemon = Daemon(config)
    
    def signal_handler(signum, frame):
        print(f"\nReceived signal {signum}, shutting down...")
        daemon.stop()
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        asyncio.run(daemon.run())
    except Exception as e:
        print(f"Runtime error: {e}")
        if not foreground:
            pid_file.release()
        return 1
    finally:
        if not foreground:
            pid_file.release()
    
    return 0


def parse_connection_url(url: str) -> tuple[str, str, str]:
    """
    Parse connection URL from gateway
    
    URL format: http(s)://host[:port][/path]?token=xxx&machine_id=yyy
    Legacy websocket URLs are still accepted.
    
    Returns:
        (gateway_url, token, suggested_machine_id)
    """
    from urllib.parse import urlparse, parse_qs
    
    parsed = urlparse(url)
    
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https", "ws", "wss"}:
        raise ValueError(f"unsupported scheme: {parsed.scheme}")

    base_path = parsed.path.rstrip("/")
    if scheme in {"ws", "wss"} and base_path == "/ws/runtime":
        base_path = ""
    gateway_scheme = "https" if scheme == "wss" else "http" if scheme == "ws" else scheme
    gateway_url = f"{gateway_scheme}://{parsed.netloc}{base_path}"
    
    # Parse query params
    params = parse_qs(parsed.query)
    
    token = params.get("token", [""])[0]
    machine_id = params.get("machine_id", [""])[0]
    
    return gateway_url, token, machine_id


def cmd_start(args: argparse.Namespace) -> int:
    """Start the runtime daemon"""
    config_path = Path(args.config) if args.config else get_default_config_path()
    
    try:
        config = load_config(config_path)
    except FileNotFoundError:
        print(f"Config file not found: {config_path}")
        print("Run 'scheduler-runtime init' to create a config file")
        print("Or use 'scheduler-runtime connect <url>' to connect with a token URL")
        return 1
    
    # Override with CLI args
    if args.foreground:
        config.foreground = True
    if args.gateway:
        config.gateway_url = args.gateway
    if args.machine_id:
        config.machine_id = args.machine_id
    config.config_path = str(config_path)
    
    print(f"Starting runtime...")
    print(f"Gateway: {config.gateway_url}")
    print(f"Machine: {config.machine_id}")
    
    return _start_daemon(config, foreground=args.foreground)


def cmd_stop(args: argparse.Namespace) -> int:
    """Stop the runtime daemon"""
    pid_file = PidFile()
    pid = _resolve_runtime_pid(pid_file)
    
    if not pid:
        print("Runtime not running")
        return 1
    
    try:
        os.kill(pid, signal.SIGTERM)
        # Wait for process to exit
        for _ in range(30):  # 3 seconds timeout
            try:
                os.kill(pid, 0)
                time.sleep(0.1)
            except ProcessLookupError:
                break
        else:
            print(f"Runtime did not exit gracefully, killing...")
            os.kill(pid, signal.SIGKILL)
        
        pid_file.release()
        print(f"Runtime stopped")
        return 0
    except ProcessLookupError:
        pid_file.release()
        print("Runtime not running (stale PID file)")
        return 0
    except PermissionError:
        print(f"Permission denied to stop runtime (PID: {pid})")
        return 1


def cmd_status(args: argparse.Namespace) -> int:
    """Check runtime status"""
    pid_file = PidFile()
    pid = _resolve_runtime_pid(pid_file)
    
    if not pid:
        print("Runtime: not running")
        return 1
    
    try:
        os.kill(pid, 0)
        print(f"Runtime: running (PID: {pid})")
        
        # Try to get more info from health endpoint
        try:
            data = _read_health()
            print(f"  Uptime: {data.get('uptime', 'unknown')}")
            print(f"  Tasks: {data.get('tasks_running', 0)} running")
        except Exception:
            pass
        
        return 0
    except ProcessLookupError:
        pid_file.release()
        print("Runtime: not running (stale PID file)")
        return 1


def _read_health() -> dict:
    import urllib.request

    req = urllib.request.Request("http://127.0.0.1:17890/health")
    with urllib.request.urlopen(req, timeout=1) as resp:
        return json.loads(resp.read().decode())


def _resolve_runtime_pid(pid_file: PidFile) -> int | None:
    pid = pid_file.read()
    if pid:
        try:
            os.kill(pid, 0)
            return pid
        except ProcessLookupError:
            pid_file.release()
        except PermissionError:
            return pid

    try:
        health = _read_health()
    except Exception:
        return None
    health_pid = health.get("pid")
    try:
        resolved = int(health_pid)
    except (TypeError, ValueError):
        return None
    try:
        os.kill(resolved, 0)
    except ProcessLookupError:
        return None
    except PermissionError:
        return resolved
    return resolved


def cmd_logs(args: argparse.Namespace) -> int:
    """Show runtime logs"""
    log_file = runtime_state_dir() / "logs" / "runtime.log"
    
    if not log_file.exists():
        print(f"No log file found: {log_file}")
        return 1
    
    if args.follow:
        import subprocess
        try:
            subprocess.run(['tail', '-f', str(log_file)])
        except KeyboardInterrupt:
            pass
    else:
        lines = args.lines if args.lines else 50
        with open(log_file) as f:
            content = f.readlines()
            print(''.join(content[-lines:]))
    
    return 0


def cmd_init(args: argparse.Namespace) -> int:
    """Initialize config file"""
    config_path = get_default_config_path()
    
    if config_path.exists() and not args.force:
        print(f"Config already exists: {config_path}")
        print("Use --force to overwrite")
        return 1
    
    config = Config(
        gateway_url=args.gateway or "ws://localhost:8888/ws/runtime",
        machine_id=args.machine_id or os.uname().nodename,
        machine_name=args.machine_name or args.machine_id or os.uname().nodename,
        api_key=args.api_key or "",
    )
    config.config_path = str(config_path)
    
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config.save(config_path)
    
    print(f"Config created: {config_path}")
    print(f"Edit this file to set your API key and gateway URL")
    
    return 0


def main() -> int:
    """Main entry point"""
    parser = argparse.ArgumentParser(
        prog="scheduler-runtime",
        description="Scheduler Runtime Agent for worker machines",
    )
    
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # Connect command (token-based connection)
    connect_parser = subparsers.add_parser("connect", help="Connect to gateway using token URL")
    connect_parser.add_argument("url", help="Connection URL from gateway web UI (ws://host:port/path?token=xxx)")
    connect_parser.add_argument("-n", "--name", help="Machine display name (optional)")
    connect_parser.add_argument("-f", "--foreground", action="store_true", help="Run in foreground")
    
    # Start command
    start_parser = subparsers.add_parser("start", help="Start the runtime")
    start_parser.add_argument("-c", "--config", help="Config file path")
    start_parser.add_argument("-g", "--gateway", help="Gateway WebSocket URL")
    start_parser.add_argument("-m", "--machine-id", help="Machine ID")
    start_parser.add_argument("-f", "--foreground", action="store_true", help="Run in foreground")
    
    # Stop command
    subparsers.add_parser("stop", help="Stop the runtime")
    
    # Status command
    subparsers.add_parser("status", help="Check runtime status")
    
    # Logs command
    logs_parser = subparsers.add_parser("logs", help="Show runtime logs")
    logs_parser.add_argument("-f", "--follow", action="store_true", help="Follow log output")
    logs_parser.add_argument("-n", "--lines", type=int, help="Number of lines to show")
    
    # Init command
    init_parser = subparsers.add_parser("init", help="Initialize config file")
    init_parser.add_argument("-g", "--gateway", help="Gateway WebSocket URL")
    init_parser.add_argument("-m", "--machine-id", help="Machine ID")
    init_parser.add_argument("--machine-name", help="Machine display name")
    init_parser.add_argument("--api-key", help="API key for authentication")
    init_parser.add_argument("--force", action="store_true", help="Overwrite existing config")
    
    args = parser.parse_args()
    
    commands = {
        "connect": cmd_connect,
        "start": cmd_start,
        "stop": cmd_stop,
        "status": cmd_status,
        "logs": cmd_logs,
        "init": cmd_init,
    }
    
    return commands[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
