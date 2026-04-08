"""
Health check HTTP server for scheduler-runtime

Provides endpoints for status monitoring and metrics.
"""
import json
import os
import time
from datetime import datetime
from typing import Dict

try:
    from aiohttp import web
    HAS_AIOHTTP = True
except ImportError:
    HAS_AIOHTTP = False

from scheduler_runtime.config import Config


class HealthServer:
    """HTTP health check server"""
    
    def __init__(self, config: Config):
        self.config = config
        self.start_time = time.time()
        self.app = None
        self.runner = None
        self.site = None
    
    async def start(self) -> None:
        """Start health server"""
        if not HAS_AIOHTTP:
            return
        
        self.app = web.Application()
        self.app.router.add_get('/health', self._handle_health)
        self.app.router.add_get('/metrics', self._handle_metrics)
        
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        
        self.site = web.TCPSite(self.runner, 'localhost', 17890)
        await self.site.start()
    
    async def stop(self) -> None:
        """Stop health server"""
        if not HAS_AIOHTTP:
            return
        
        if self.site:
            await self.site.stop()
        if self.runner:
            await self.runner.cleanup()
    
    async def _handle_health(self, request: 'web.Request') -> 'web.Response':
        """Health check endpoint"""
        uptime = time.time() - self.start_time
        
        data = {
            "status": "healthy",
            "pid": os.getpid(),
            "machine_id": self.config.machine_id,
            "uptime": self._format_duration(uptime),
            "timestamp": datetime.utcnow().isoformat(),
        }
        
        return web.json_response(data)
    
    async def _handle_metrics(self, request: 'web.Request') -> 'web.Response':
        """Metrics endpoint"""
        # This could be expanded to include more metrics
        data = {
            "machine_id": self.config.machine_id,
            "timestamp": datetime.utcnow().isoformat(),
        }
        
        return web.json_response(data)
    
    def _format_duration(self, seconds: float) -> str:
        """Format duration in human readable form"""
        if seconds < 60:
            return f"{int(seconds)}s"
        elif seconds < 3600:
            return f"{int(seconds / 60)}m"
        elif seconds < 86400:
            return f"{int(seconds / 3600)}h"
        else:
            return f"{int(seconds / 86400)}d"
