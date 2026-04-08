# Aibo Runtime

Worker runtime agent for Aibo Scheduler. Deploy this on worker machines to enable heartbeat connection, task reception, self-update, and GUI command execution.

## Architecture

The runtime agent is a lightweight Python service that:

1. **Heartbeat** - Maintains a lightweight control connection with the gateway
2. **Task Reception** - Claims queued jobs from the scheduler
3. **Execution** - Runs tasks via tmux and reports launch state
4. **Self Update** - Polls for update instructions, pulls from Git, reinstalls dependencies, and restarts itself
5. **GUI Commands** - Handles graphical operations (screenshots, UI automation)

## Installation

```bash
cd runtime
pip install -r requirements.txt
```

## Configuration

Create `~/.aibo/runtime.json`:

```json
{
  "gateway_url": "https://runtime-gateway.example.com",
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

## Cloudflare Tunnel

If the dashboard is protected by Cloudflare Access, do not use that same hostname for runtime workers.

- Keep the dashboard on a protected hostname
- Expose runtime traffic on a separate hostname
- Point the gateway's runtime URL setting at that runtime hostname so connect tokens generate usable URLs
- Do not put Cloudflare Access in front of the runtime hostname; the runtime authenticates with the token URL itself

## Features

- Auto-reconnection on network failure
- Task queue with single-runtime concurrency control
- Poll-based control channel for updates
- Git-based self update with in-place restart
- Screenshot capture for remote monitoring
- Process lifecycle management via tmux
