#!/usr/bin/env python3
"""
Run the LL Analytics application.

Usage:
    python run.py           # Run the web server
    python run.py --init    # Initialize the database
    python run.py --help    # Show help
"""

import argparse
import sys


def main():
    parser = argparse.ArgumentParser(
        description="LL Analytics - Learned League Analysis Platform"
    )
    parser.add_argument(
        "--init",
        action="store_true",
        help="Initialize the database"
    )
    parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="Host to bind to (default: 127.0.0.1)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port to bind to (default: 8000)"
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Enable auto-reload for development"
    )

    args = parser.parse_args()

    if args.init:
        from ll_analytics.database import init_db
        print("Initializing database...")
        init_db()
        print("Done!")
        return

    # Run the web server
    try:
        import uvicorn
        from ll_analytics.database import init_db

        # Ensure database exists
        init_db()

        print(f"\nStarting LL Analytics server at http://{args.host}:{args.port}")
        print("Press Ctrl+C to stop\n")

        uvicorn.run(
            "ll_analytics.api.main:app",
            host=args.host,
            port=args.port,
            reload=args.reload,
        )
    except ImportError as e:
        print(f"Missing dependency: {e}")
        print("Run: pip install -r requirements.txt")
        sys.exit(1)


if __name__ == "__main__":
    main()
