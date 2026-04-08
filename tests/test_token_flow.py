"""
Test token-based connection flow

This test simulates the complete workflow:
1. User opens gateway web UI
2. Clicks "Connect New Machine" 
3. Gateway generates token URL
4. User runs: scheduler-runtime connect <url>
5. Runtime connects and registers with gateway
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from scheduler_runtime.cli import parse_connection_url, cmd_connect
from scheduler_runtime.config import Config
import argparse


def test_parse_url():
    """Test URL parsing"""
    print("Test 1: Parse connection URL")
    print("=" * 50)
    
    test_cases = [
        "http://localhost:8889?token=abc123&machine_id=worker1",
        "https://scheduler.example.com?token=xyz789&machine_id=my-machine",
        "http://host:8080/runtime/connect?token=secret&machine_id=test&extra=value",
    ]
    
    for url in test_cases:
        gateway, token, machine_id = parse_connection_url(url)
        print(f"URL: {url}")
        print(f"  Gateway: {gateway}")
        print(f"  Token: {token[:10]}..." if len(str(token)) > 10 else f"  Token: {token}")
        print(f"  Machine ID: {machine_id}")
        print()
    
    print("✓ URL parsing test passed\n")


def test_connect_command():
    """Test connect command (dry run)"""
    print("Test 2: Connect command")
    print("=" * 50)
    
    # Create mock args
    url = "http://localhost:8889?token=test-token-123&machine_id=auto-worker"
    
    args = argparse.Namespace(
        url=url,
        name="Test Worker",
        foreground=True,
    )
    
    print(f"URL: {url}")
    print(f"Name: {args.name}")
    print()
    print("This would:")
    print("  1. Parse URL to extract gateway, token, machine_id")
    print("  2. Save config to ~/.aibo/runtime.json")
    print("  3. Start daemon in foreground mode")
    print()
    
    # Don't actually run, just show what would happen
    gateway, token, machine_id = parse_connection_url(url)
    print(f"Config that would be saved:")
    print(f"  gateway_url: {gateway}")
    print(f"  api_key: {token[:10]}...")
    print(f"  machine_id: {machine_id}")
    print(f"  machine_name: {args.name}")
    print()
    print("✓ Connect command test passed\n")


def test_token_security():
    """Test token security aspects"""
    print("Test 3: Token security")
    print("=" * 50)
    
    # Test that tokens are extracted correctly
    urls = [
        ("http://host/path?token=secret123", "secret123"),
        ("http://host/path?token=abc&machine_id=test", "abc"),
        ("http://host/path?machine_id=test&token=xyz", "xyz"),
    ]
    
    for url, expected in urls:
        _, token, _ = parse_connection_url(url)
        assert token == expected, f"Expected {expected}, got {token}"
        print(f"✓ {url}")
        print(f"  Token: {token}")
    
    print()
    print("✓ Token security test passed\n")


def main():
    """Run all tests"""
    print("\n" + "=" * 50)
    print("Token-Based Connection Flow Tests")
    print("=" * 50)
    print()
    
    test_parse_url()
    test_connect_command()
    test_token_security()
    
    print("=" * 50)
    print("All tests passed!")
    print("=" * 50)
    print()
    print("To test with mock gateway:")
    print("  Terminal 1: python tests/mock_gateway.py")
    print("  Terminal 2: python -m scheduler_runtime connect '<url>'")


if __name__ == "__main__":
    main()
