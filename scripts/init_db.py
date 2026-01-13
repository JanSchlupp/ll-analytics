#!/usr/bin/env python3
"""Initialize the LL Analytics database."""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from ll_analytics.database import init_db


def main():
    """Initialize the database."""
    print("Initializing LL Analytics database...")
    init_db()
    print("Done!")


if __name__ == "__main__":
    main()
