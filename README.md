# Scheduler Runtime Agent

Worker machine agent for Scheduler. Deploy this on worker machines to enable heartbeat connection, task reception, and GUI command execution.

## Architecture

The runtime agent is a lightweight Python service that:

1. **Heartbeat** - Maintains WebSocket connection with gateway
2. **Task Reception** - Receives job assignments from scheduler
3. **Execution** - Runs tasks via tmux and reports results
4. **GUI Commands** - Handles graphical operations (screenshots, UI automation)

## Installation

```bash
cd runtime
pip install -r requirements.txt
```

## Configuration

Create `~/.scheduler/runtime.json`:

```json
{
  "gateway_url": "ws://scheduler-host:8888/ws",
  "machine_id": "worker1",
  "machine_name": "Worker 1",
  "api_key": "your-api-key",
  "heartbeat_interval": 15,
  "work_dir": "~/scheduler_work"
}
```

## Usage

### Start Agent

```bash
python -m scheduler_runtime start
```

### Run in Foreground (for debugging)

```bash
python -m scheduler_runtime start --foreground
```

### Check Status

```bash
python -m scheduler_runtime status
```

### Stop Agent

```bash
python -m scheduler_runtime stop
```

## Features

- Auto-reconnection on network failure
- Task queue with concurrency control
- Screenshot capture for remote monitoring
- Process lifecycle management via tmux
