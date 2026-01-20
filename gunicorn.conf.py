"""
Production Server Configuration

Run FastAPI with Uvicorn workers under Gunicorn for production deployment.
"""

import multiprocessing
import os

# Server socket
bind = os.getenv("BIND", "0.0.0.0:8000")
backlog = 2048

# Worker processes
workers = int(os.getenv("WORKERS", multiprocessing.cpu_count() * 2 + 1))
worker_class = "uvicorn.workers.UvicornWorker"
worker_connections = 1000
max_requests = 10000
max_requests_jitter = 1000
timeout = 120
keepalive = 5
graceful_timeout = 30

# Process naming
proc_name = "ecommerce-analytics-api"

# Server mechanics
daemon = False
pidfile = "/tmp/gunicorn.pid"
user = None
group = None
tmp_upload_dir = None

# Logging
errorlog = "-"
loglevel = os.getenv("LOG_LEVEL", "info")
accesslog = "-"
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(D)s'

# SSL (uncomment for HTTPS)
# keyfile = "/path/to/keyfile"
# certfile = "/path/to/certfile"

# Hooks
def on_starting(server):
    """Called just before the master process is initialized."""
    pass

def on_reload(server):
    """Called before reloading workers."""
    pass

def pre_fork(server, worker):
    """Called before a worker is forked."""
    pass

def post_fork(server, worker):
    """Called after a worker has been forked."""
    pass

def pre_exec(server):
    """Called before exec()."""
    pass

def when_ready(server):
    """Called when server is ready to receive connections."""
    pass

def worker_int(worker):
    """Called when worker receives INT or QUIT signal."""
    pass

def worker_abort(worker):
    """Called when worker receives SIGABRT signal."""
    pass
