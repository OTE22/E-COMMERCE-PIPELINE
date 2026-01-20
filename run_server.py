#!/usr/bin/env python
"""
Production Server Entry Point

Starts FastAPI with production-grade configuration.
Usage:
    Development:  python run_server.py --dev
    Production:   python run_server.py
    
    Or with Gunicorn:
    gunicorn src.main:app -c gunicorn.conf.py
"""

import argparse
import os
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))


def run_dev_server():
    """Run development server with auto-reload."""
    import uvicorn
    
    uvicorn.run(
        "src.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        reload_dirs=["src"],
        log_level="debug",
        access_log=True,
    )


def run_prod_server():
    """Run production server with Uvicorn directly."""
    import uvicorn
    
    workers = int(os.getenv("WORKERS", 4))
    
    uvicorn.run(
        "src.main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8000)),
        workers=workers,
        log_level=os.getenv("LOG_LEVEL", "info"),
        access_log=True,
        proxy_headers=True,
        forwarded_allow_ips="*",
        server_header=False,
        date_header=True,
    )


def run_gunicorn():
    """Run with Gunicorn (recommended for production)."""
    import subprocess
    
    cmd = [
        "gunicorn",
        "src.main:app",
        "-c", "gunicorn.conf.py",
    ]
    
    subprocess.run(cmd)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E-Commerce Analytics API Server")
    parser.add_argument(
        "--dev",
        action="store_true",
        help="Run in development mode with auto-reload"
    )
    parser.add_argument(
        "--gunicorn",
        action="store_true", 
        help="Run with Gunicorn (production)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port to run on (default: 8000)"
    )
    
    args = parser.parse_args()
    
    if args.port:
        os.environ["PORT"] = str(args.port)
    
    if args.dev:
        print("🚀 Starting development server...")
        run_dev_server()
    elif args.gunicorn:
        print("🚀 Starting production server with Gunicorn...")
        run_gunicorn()
    else:
        print("🚀 Starting production server with Uvicorn...")
        run_prod_server()
