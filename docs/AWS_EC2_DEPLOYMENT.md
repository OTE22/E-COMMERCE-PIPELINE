# AWS EC2 Deployment Guide

## Prerequisites

### EC2 Instance Requirements
- **Instance Type**: t3.medium or larger (recommended: t3.large for production)
- **AMI**: Ubuntu 22.04 LTS
- **Storage**: 30GB+ EBS (gp3 recommended)
- **Security Group**:
  - Port 22 (SSH)
  - Port 80 (HTTP)
  - Port 443 (HTTPS)

### RDS Database (Recommended)
- PostgreSQL 15+
- db.t3.micro for testing, db.t3.small+ for production
- Enable automated backups

### ElastiCache Redis (Optional)
- cache.t3.micro for testing
- cache.t3.small+ for production

---

## Quick Deployment

### 1. SSH into EC2
```bash
ssh -i your-key.pem ubuntu@your-ec2-public-ip
```

### 2. First-Time Setup
```bash
# Clone the repository
sudo git clone https://github.com/yourusername/ecommerce-analytics.git /opt/ecommerce-analytics
cd /opt/ecommerce-analytics

# Run deployment script (first-time)
sudo chmod +x scripts/deploy.sh
sudo ./scripts/deploy.sh --first-time
```

### 3. Configure Environment
```bash
sudo nano /opt/ecommerce-analytics/.env
```

Update these values:
```env
APP_ENV=production
DEBUG=false
SECRET_KEY=your-secure-random-key-here

# RDS Database
DATABASE_HOST=your-rds-endpoint.amazonaws.com
DATABASE_PORT=5432
DATABASE_NAME=ecommerce_analytics
DATABASE_USER=postgres
DATABASE_PASSWORD=your-rds-password

# ElastiCache Redis (or local)
REDIS_URL=redis://your-elasticache-endpoint:6379/0
```

### 4. Restart Services
```bash
cd /opt/ecommerce-analytics
sudo docker-compose down
sudo docker-compose up -d
```

### 5. Enable SSL (Optional)
```bash
sudo ./scripts/deploy.sh "" your-domain.com your-email@example.com
```

---

## Manual Deployment Steps

### Install Docker
```bash
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo systemctl enable docker
sudo systemctl start docker
```

### Install Docker Compose
```bash
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
```

### Deploy Application
```bash
cd /opt/ecommerce-analytics
sudo docker-compose up -d --build
```

### Run Migrations
```bash
sudo docker-compose exec api alembic upgrade head
```

---

## Enable Auto-Start on Reboot

```bash
# Copy service file
sudo cp /opt/ecommerce-analytics/scripts/ecommerce-analytics.service /etc/systemd/system/

# Enable service
sudo systemctl daemon-reload
sudo systemctl enable ecommerce-analytics
sudo systemctl start ecommerce-analytics
```

---

## Nginx Reverse Proxy

### Install Nginx
```bash
sudo apt-get install -y nginx
```

### Configure
```bash
sudo nano /etc/nginx/sites-available/ecommerce-analytics
```

```nginx
server {
    listen 80;
    server_name your-domain.com;
    
    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

### Enable Site
```bash
sudo ln -s /etc/nginx/sites-available/ecommerce-analytics /etc/nginx/sites-enabled/
sudo rm /etc/nginx/sites-enabled/default
sudo nginx -t
sudo systemctl reload nginx
```

---

## SSL with Let's Encrypt

```bash
sudo apt-get install -y certbot python3-certbot-nginx
sudo certbot --nginx -d your-domain.com
```

---

## Monitoring & Logs

### View Logs
```bash
# All services
sudo docker-compose logs -f

# API only
sudo docker-compose logs -f api

# Last 100 lines
sudo docker-compose logs --tail=100 api
```

### Check Status
```bash
# Docker containers
sudo docker-compose ps

# Health check
curl http://localhost:8000/api/v1/health
```

### Monitor Resources
```bash
# Container stats
sudo docker stats

# System resources
htop
```

---

## Scaling

### Horizontal Scaling (Multiple API Workers)
```bash
# Scale to 3 API workers
sudo docker-compose up -d --scale api=3
```

### AWS Auto Scaling
1. Create an AMI from your configured instance
2. Create a Launch Template
3. Create an Auto Scaling Group
4. Configure Application Load Balancer

---

## Troubleshooting

### Container Won't Start
```bash
# Check logs
sudo docker-compose logs api

# Check Docker status
sudo systemctl status docker
```

### Database Connection Issues
```bash
# Test connection from container
sudo docker-compose exec api python -c "from src.database.connection import init_database; import asyncio; asyncio.run(init_database())"
```

### Port Already in Use
```bash
# Find process using port
sudo lsof -i :8000
sudo kill -9 <PID>
```

### Reset Everything
```bash
sudo docker-compose down -v
sudo docker system prune -a
sudo docker-compose up -d --build
```

---

## Security Checklist

- [ ] Change default passwords
- [ ] Enable AWS Security Groups
- [ ] Enable RDS encryption
- [ ] Enable SSL/TLS
- [ ] Set up CloudWatch alarms
- [ ] Enable AWS GuardDuty
- [ ] Regular security updates
- [ ] Backup configuration
