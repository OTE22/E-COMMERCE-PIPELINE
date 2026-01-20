#!/bin/bash
# ==============================================================================
# AWS EC2 Deployment Script for E-Commerce Analytics Platform
# ==============================================================================
# This script deploys the application to an EC2 instance
# 
# Prerequisites on EC2:
#   - Ubuntu 22.04 LTS
#   - Docker & Docker Compose installed
#   - Git installed
#   - AWS CLI configured (for ECR if using)
#
# Usage:
#   chmod +x deploy.sh
#   ./deploy.sh
# ==============================================================================

set -e  # Exit on any error

# Configuration
APP_NAME="ecommerce-analytics"
APP_DIR="/opt/${APP_NAME}"
REPO_URL="${REPO_URL:-https://github.com/yourusername/ecommerce-analytics.git}"
BRANCH="${BRANCH:-main}"
DOCKER_COMPOSE_FILE="docker-compose.yml"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# ==============================================================================
# Pre-flight Checks
# ==============================================================================
preflight_checks() {
    log_info "Running pre-flight checks..."
    
    # Check if running as root or with sudo
    if [[ $EUID -ne 0 ]]; then
        log_error "This script must be run as root or with sudo"
        exit 1
    fi
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed"
        exit 1
    fi
    
    # Check Docker Compose
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        log_error "Docker Compose is not installed"
        exit 1
    fi
    
    log_info "Pre-flight checks passed ✓"
}

# ==============================================================================
# Install Dependencies (First-time Setup)
# ==============================================================================
install_dependencies() {
    log_info "Installing system dependencies..."
    
    apt-get update
    apt-get install -y \
        apt-transport-https \
        ca-certificates \
        curl \
        gnupg \
        lsb-release \
        nginx \
        certbot \
        python3-certbot-nginx
    
    # Install Docker if not present
    if ! command -v docker &> /dev/null; then
        log_info "Installing Docker..."
        curl -fsSL https://get.docker.com -o get-docker.sh
        sh get-docker.sh
        systemctl enable docker
        systemctl start docker
    fi
    
    log_info "Dependencies installed ✓"
}

# ==============================================================================
# Clone/Update Repository
# ==============================================================================
update_code() {
    log_info "Updating application code..."
    
    if [ -d "$APP_DIR" ]; then
        cd "$APP_DIR"
        git fetch origin
        git checkout "$BRANCH"
        git pull origin "$BRANCH"
    else
        git clone -b "$BRANCH" "$REPO_URL" "$APP_DIR"
        cd "$APP_DIR"
    fi
    
    log_info "Code updated ✓"
}

# ==============================================================================
# Configure Environment
# ==============================================================================
configure_environment() {
    log_info "Configuring environment..."
    
    cd "$APP_DIR"
    
    # Create .env if it doesn't exist
    if [ ! -f .env ]; then
        if [ -f .env.example ]; then
            cp .env.example .env
            log_warn "Created .env from .env.example - PLEASE CONFIGURE IT!"
        else
            log_error ".env.example not found"
            exit 1
        fi
    fi
    
    # Set production values
    sed -i 's/APP_ENV=.*/APP_ENV=production/' .env
    sed -i 's/DEBUG=.*/DEBUG=false/' .env
    
    log_info "Environment configured ✓"
}

# ==============================================================================
# Build and Deploy
# ==============================================================================
deploy_application() {
    log_info "Deploying application..."
    
    cd "$APP_DIR"
    
    # Pull latest images
    docker-compose pull || true
    
    # Build custom images
    docker-compose build --no-cache
    
    # Stop existing containers
    docker-compose down --remove-orphans || true
    
    # Start services
    docker-compose up -d
    
    # Wait for services to be healthy
    log_info "Waiting for services to start..."
    sleep 10
    
    # Run database migrations
    log_info "Running database migrations..."
    docker-compose exec -T api alembic upgrade head || log_warn "Migration failed or not needed"
    
    log_info "Application deployed ✓"
}

# ==============================================================================
# Configure Nginx
# ==============================================================================
configure_nginx() {
    log_info "Configuring Nginx..."
    
    # Create Nginx config
    cat > /etc/nginx/sites-available/${APP_NAME} << 'EOF'
server {
    listen 80;
    server_name _;
    
    # Security headers
    add_header X-Frame-Options "SAMEORIGIN" always;
    add_header X-Content-Type-Options "nosniff" always;
    add_header X-XSS-Protection "1; mode=block" always;
    
    # API and frontend
    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_cache_bypass $http_upgrade;
        proxy_read_timeout 86400;
    }
    
    # Health check endpoint
    location /health {
        proxy_pass http://127.0.0.1:8000/api/v1/health;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
    }
    
    # Static files caching
    location ~* \.(js|css|png|jpg|jpeg|gif|ico|svg|woff|woff2)$ {
        proxy_pass http://127.0.0.1:8000;
        expires 30d;
        add_header Cache-Control "public, immutable";
    }
}
EOF
    
    # Enable site
    ln -sf /etc/nginx/sites-available/${APP_NAME} /etc/nginx/sites-enabled/
    rm -f /etc/nginx/sites-enabled/default
    
    # Test and reload
    nginx -t
    systemctl reload nginx
    
    log_info "Nginx configured ✓"
}

# ==============================================================================
# Setup SSL (Let's Encrypt)
# ==============================================================================
setup_ssl() {
    DOMAIN=$1
    EMAIL=$2
    
    if [ -z "$DOMAIN" ] || [ -z "$EMAIL" ]; then
        log_warn "Skipping SSL setup - domain and email required"
        log_info "To enable SSL later, run: certbot --nginx -d yourdomain.com"
        return
    fi
    
    log_info "Setting up SSL for $DOMAIN..."
    
    # Update Nginx config with domain
    sed -i "s/server_name _;/server_name ${DOMAIN};/" /etc/nginx/sites-available/${APP_NAME}
    
    # Get SSL certificate
    certbot --nginx -d "$DOMAIN" --email "$EMAIL" --agree-tos --non-interactive
    
    # Setup auto-renewal
    systemctl enable certbot.timer
    systemctl start certbot.timer
    
    log_info "SSL configured ✓"
}

# ==============================================================================
# Health Check
# ==============================================================================
health_check() {
    log_info "Running health check..."
    
    # Wait for API to be ready
    MAX_RETRIES=30
    RETRY_COUNT=0
    
    while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
        if curl -s http://localhost:8000/api/v1/health/live > /dev/null; then
            log_info "Health check passed ✓"
            return 0
        fi
        RETRY_COUNT=$((RETRY_COUNT + 1))
        sleep 2
    done
    
    log_error "Health check failed after $MAX_RETRIES attempts"
    docker-compose logs --tail=50 api
    return 1
}

# ==============================================================================
# Cleanup
# ==============================================================================
cleanup() {
    log_info "Cleaning up..."
    
    # Remove unused Docker resources
    docker system prune -f
    
    log_info "Cleanup complete ✓"
}

# ==============================================================================
# Main
# ==============================================================================
main() {
    log_info "Starting deployment of ${APP_NAME}..."
    
    preflight_checks
    
    # First-time setup flag
    if [ "$1" == "--first-time" ]; then
        install_dependencies
    fi
    
    update_code
    configure_environment
    deploy_application
    configure_nginx
    
    # SSL setup if domain provided
    if [ -n "$2" ] && [ -n "$3" ]; then
        setup_ssl "$2" "$3"
    fi
    
    health_check
    cleanup
    
    log_info "=================================================="
    log_info "Deployment complete!"
    log_info "=================================================="
    log_info "API:       http://localhost:8000/docs"
    log_info "Dashboard: http://localhost:8000"
    log_info "Health:    http://localhost:8000/api/v1/health"
    log_info "=================================================="
}

# Run main with all arguments
main "$@"
