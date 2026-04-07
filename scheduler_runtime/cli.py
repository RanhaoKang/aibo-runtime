"""
CLI commands for scheduler-runtime
"""
import argparse
import json
import os
import signal
import sys
import time
from pathlib import Path

from scheduler_runtime.config import Config, load_config
from scheduler_runtime.daemon import Daemon
from scheduler_runtime.pidfile import PidFile, PidFileError


def get_default_config_path() -> Path:
    """Get default config file path"""
    return Path.home() / ".scheduler" / "runtime.json"


def cmd_start(args: argparse.Namespace) -> int:
    """Start the runtime daemon"""
    config_path = Path(args.config) if args.config else get_default_config_path()
    
    try:
        config = load_config(config_path)
    except FileNotFoundError:
        print(f"Config file not found: {config_path}")
        print("Run 'scheduler-runtime init' to create a config file")
        return 1
    
    # Override with CLI args
    if args.foreground:
        config.foreground = True
    if args.gateway:
        config.gateway_url = args.gateway
    if args.machine_id:
        config.machine_id = args.machine_id
    
    pid_file = PidFile()
    
    if not args.foreground:
        # Daemonize
        try:
            pid_file.acquire()
        except PidFileError as e:
            print(f"Runtime already running: {e}")
            return 1
        
        # Fork to background
        if os.fork() > 0:
            # Parent exits
            time.sleep(0.5)  # Give child time to start
            print(f"Runtime started (PID: {pid_file.read()})")
            return 0
        
        # Child process
        os.setsid()
        os.umask(0)
        
        # Second fork
        if os.fork() > 0:
            sys.exit(0)
        
        # Redirect stdout/stderr to log file
        log_dir = Path.home() / ".scheduler" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "runtime.log"
        
        sys.stdout.flush()
        sys.stderr.flush()
        
        with open(log_file, 'a') as f:
            os.dup2(f.fileno(), sys.stdout.fileno())
            os.dup2(f.fileno(), sys.stderr.fileno())
    else:
        # Foreground mode
        print(f"Starting runtime in foreground...")
        print(f"Config: {config_path}")
        print(f"Gateway: {config.gateway_url}")
    
    # Run daemon
    daemon = Daemon(config)
    
    def signal_handler(signum, frame):
        print(f"\nReceived signal {signum}, shutting down...")
        daemon.stop()
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        daemon.run()
    except Exception as e:
        print(f"Runtime error: {e}")
        if not args.foreground:
            pid_file.release()
        return 1
    finally:
        if not args.foreground:
            pid_file.release()
    
    return 0


def cmd_stop(args: argparse.Namespace) -> int:
    """Stop the runtime daemon"""
    pid_file = PidFile()
    pid = pid_file.read()
    
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
    pid = pid_file.read()
    
    if not pid:
        print("Runtime: not running")
        return 1
    
    try:
        os.kill(pid, 0)
        print(f"Runtime: running (PID: {pid})")
        
        # Try to get more info from health endpoint
        try:
            import urllib.request
            req = urllib.request.Request('http://localhost:17890/health')
            with urllib.request.urlopen(req, timeout=1) as resp:
                data = json.loads(resp.read().decode())
                print(f"  Uptime: {data.get('uptime', 'unknown')}")
                print(f"  Tasks: {data.get('tasks_running', 0)} running")
        except Exception:
            pass
        
        return 0
    except ProcessLookupError:
        pid_file.release()
        print("Runtime: not running (stale PID file)")
        return 1


def cmd_logs(args: argparse.Namespace) -> int:
    """Show runtime logs"""
    log_file = Path.home() / ".scheduler" / "logs" / "runtime.log"
    
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
        "start": cmd_start,
        "stop": cmd_stop,
        "status": cmd_status,
        "logs": cmd_logs,
        "init": cmd_init,
    }
    
    return commands[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
