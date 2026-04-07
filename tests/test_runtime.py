"""
Test script for scheduler-runtime

Usage:
    Terminal 1: python tests/mock_gateway.py
    Terminal 2: python tests/test_runtime.py
"""
import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scheduler_runtime.config import Config
from scheduler_runtime.daemon import Daemon


async def test_basic_connection():
    """Test basic connection to mock gateway"""
    print("=" * 50)
    print("Test 1: Basic Connection")
    print("=" * 50)
    
    config = Config(
        gateway_url="ws://localhost:8889",  # Mock gateway port
        machine_id="test-worker-1",
        machine_name="Test Worker 1",
        api_key="test-api-key",
        foreground=True,
    )
    
    daemon = Daemon(config)
    
    # Run for 10 seconds then stop
    asyncio.create_task(_stop_after(daemon, 10))
    await daemon.run()
    
    print("Test 1 completed")


async def test_with_bash_env():
    """Test with bash environment configuration"""
    print("=" * 50)
    print("Test 2: Bash Environment Configuration")
    print("=" * 50)
    
    config = Config(
        gateway_url="ws://localhost:8889",
        machine_id="test-worker-2",
        machine_name="Test Worker 2",
        api_key="test-api-key",
        foreground=True,
        env_vars={
            "TEST_VAR": "hello_from_config",
            "ANOTHER_VAR": "value123",
        },
        bashrc_extra=[
            'echo "Bash environment loaded"',
            'export CUSTOM_PATH="/usr/local/custom"',
        ],
    )
    
    print(f"Bash setup:\n{config.get_bash_env_setup()}")
    
    daemon = Daemon(config)
    asyncio.create_task(_stop_after(daemon, 10))
    await daemon.run()
    
    print("Test 2 completed")


async def test_session_resume():
    """Test session resume functionality"""
    print("=" * 50)
    print("Test 3: Session Resume")
    print("=" * 50)
    
    # Create a mock session file
    with tempfile.NamedTemporaryFile(
        mode='w',
        suffix='.jsonl',
        delete=False,
        prefix='test_session_'
    ) as f:
        f.write(json.dumps({
            "type": "session",
            "id": "test-session-123",
            "timestamp": "2025-01-01T00:00:00Z",
            "cwd": "/tmp",
        }) + '\n')
        f.write(json.dumps({
            "type": "message",
            "id": "msg-1",
            "timestamp": "2025-01-01T00:00:01Z",
            "message": {
                "role": "user",
                "content": [{"type": "text", "text": "Hello"}],
            },
        }) + '\n')
        session_path = f.name
    
    print(f"Created test session: {session_path}")
    
    config = Config(
        gateway_url="ws://localhost:8889",
        machine_id="test-worker-3",
        machine_name="Test Worker 3",
        api_key="test-api-key",
        foreground=True,
        env_vars={
            "SESSION_VAR": "from_gateway",
        },
    )
    
    daemon = Daemon(config)
    
    # Simulate receiving a session_resume message after 2 seconds
    asyncio.create_task(_send_session_resume(daemon, session_path, delay=2))
    asyncio.create_task(_stop_after(daemon, 15))
    
    await daemon.run()
    
    # Cleanup
    os.unlink(session_path)
    
    print("Test 3 completed")


async def _stop_after(daemon: Daemon, seconds: float):
    """Stop daemon after specified seconds"""
    await asyncio.sleep(seconds)
    print(f"\n[TEST] Stopping daemon after {seconds}s...")
    daemon.stop()


async def _send_session_resume(daemon: Daemon, session_path: str, delay: float):
    """Simulate receiving session resume"""
    await asyncio.sleep(delay)
    
    print(f"\n[TEST] Simulating session resume for {session_path}")
    
    # Wait for connection
    for _ in range(10):
        if daemon.connected:
            break
        await asyncio.sleep(0.5)
    
    if not daemon.connected:
        print("[TEST] Daemon not connected, skipping")
        return
    
    # Simulate session resume message
    message = {
        "type": "session_resume",
        "task_id": "test-resume-001",
        "session_path": session_path,
        "env_vars": {
            "GATEWAY_VAR": "hello_from_gateway",
        },
    }
    
    await daemon._handle_session_resume(message)


async def main():
    """Main test runner"""
    print("\n" + "=" * 50)
    print("Scheduler Runtime Test Suite")
    print("=" * 50)
    print("\nMake sure mock_gateway.py is running first!")
    print("  python tests/mock_gateway.py")
    print()
    
    if len(sys.argv) < 2:
        print("Usage: python test_runtime.py <test_number>")
        print("  1 - Basic connection test")
        print("  2 - Bash environment configuration test")
        print("  3 - Session resume test")
        print("  all - Run all tests")
        return
    
    test_num = sys.argv[1]
    
    tests = {
        "1": test_basic_connection,
        "2": test_with_bash_env,
        "3": test_session_resume,
    }
    
    if test_num == "all":
        for num, test_func in tests.items():
            print(f"\n\nRunning test {num}...")
            try:
                await test_func()
            except Exception as e:
                print(f"Test {num} failed: {e}")
            await asyncio.sleep(2)  # Wait between tests
    elif test_num in tests:
        await tests[test_num]()
    else:
        print(f"Unknown test: {test_num}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nTests interrupted")
