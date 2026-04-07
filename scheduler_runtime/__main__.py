"""
Scheduler Runtime Agent - CLI Entry Point
"""
import argparse
import sys
from pathlib import Path

# Add parent to path for development
sys.path.insert(0, str(Path(__file__).parent.parent))

from scheduler_runtime.cli import main

if __name__ == "__main__":
    sys.exit(main())
