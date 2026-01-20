# Superset Configuration
import os

# Flask App Builder configuration
ROW_LIMIT = 5000
SUPERSET_WEBSERVER_PORT = 8088

# Secret key - change in production!
SECRET_KEY = os.environ.get('SUPERSET_SECRET_KEY', 'superset-secret-key')

# Database connection
SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL', 'postgresql+psycopg2://ecommerce:secure_password@postgres:5432/ecommerce_analytics')

# Redis for caching
CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 300,
    'CACHE_KEY_PREFIX': 'superset_',
    'CACHE_REDIS_URL': os.environ.get('REDIS_URL', 'redis://redis:6379/1'),
}

# Enable feature flags
FEATURE_FLAGS = {
    'ENABLE_TEMPLATE_PROCESSING': True,
    'DASHBOARD_NATIVE_FILTERS': True,
    'DASHBOARD_CROSS_FILTERS': True,
    'DASHBOARD_NATIVE_FILTERS_SET': True,
    'ALERT_REPORTS': True,
}

# CORS configuration
ENABLE_CORS = True
CORS_OPTIONS = {
    'supports_credentials': True,
    'allow_headers': ['*'],
    'resources': ['*'],
    'origins': ['*'],
}

# Disable example data loading in production
SUPERSET_LOAD_EXAMPLES = False
