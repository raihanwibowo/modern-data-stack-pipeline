# Superset specific config
# Optimized for low-resource environments (5 CPU cores, 6GB RAM)
import os
from datetime import timedelta

# ===========================
# Flask App Builder Configuration
# ===========================
# Reduced limits for low-resource environment
ROW_LIMIT = 10000  # Reduced from 50000
VIZ_ROW_LIMIT = 5000  # Reduced from 10000
SAMPLES_ROW_LIMIT = 500  # Reduced from 1000

# Flask configuration
SECRET_KEY = os.environ.get('SUPERSET_SECRET_KEY', 'your_secret_key_change_this_in_production')

# The SQLAlchemy connection string for Superset metadata
SQLALCHEMY_DATABASE_URI = 'sqlite:////app/superset_home/superset.db'

# ===========================
# Security Configuration
# ===========================
# Flask-WTF flag for CSRF
WTF_CSRF_ENABLED = True
WTF_CSRF_EXEMPT_LIST = []
WTF_CSRF_TIME_LIMIT = None

# Session configuration
PERMANENT_SESSION_LIFETIME = timedelta(days=7)
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SECURE = False  # Set to True if using HTTPS
SESSION_COOKIE_SAMESITE = 'Lax'

# ===========================
# Feature Flags (Optimized)
# ===========================
FEATURE_FLAGS = {
    'ENABLE_TEMPLATE_PROCESSING': True,
    'EMBEDDED_SUPERSET': False,          # Disabled to save resources
    'ALERT_REPORTS': False,              # Disabled (requires Celery/Redis)
    'DASHBOARD_NATIVE_FILTERS': True,    # Modern filter handling
    'DASHBOARD_CROSS_FILTERS': False,    # Disabled to save resources
    'DASHBOARD_FILTERS_EXPERIMENTAL': False,
    'GENERIC_CHART_AXES': True,
    'ENABLE_EXPLORE_JSON_CSRF_PROTECTION': True,
    'GLOBAL_ASYNC_QUERIES': False,       # Disabled (requires Redis/Celery)
    'VERSIONED_EXPORT': True,            # Better import/export
    'DASHBOARD_RBAC': False,             # Disabled to save resources
    'ENABLE_TEMPLATE_REMOVE_FILTERS': True,
    'THUMBNAILS': False,                 # Disabled to save memory
    'TAGGING_SYSTEM': False,             # Disabled to save resources
}

# ===========================
# Performance & Caching (File-based)
# ===========================
SUPERSET_WEBSERVER_TIMEOUT = 180  # Reduced from 300

# Simple file-based cache (no Redis required)
CACHE_CONFIG = {
    'CACHE_TYPE': 'FileSystemCache',
    'CACHE_DEFAULT_TIMEOUT': 60 * 30,  # 30 minutes (reduced from 1 day)
    'CACHE_DIR': '/app/superset_home/cache',
    'CACHE_THRESHOLD': 100  # Max number of cached items
}

# Data cache configuration (file-based)
DATA_CACHE_CONFIG = {
    'CACHE_TYPE': 'FileSystemCache',
    'CACHE_DEFAULT_TIMEOUT': 60 * 30,  # 30 minutes
    'CACHE_DIR': '/app/superset_home/data_cache',
    'CACHE_THRESHOLD': 50  # Reduced for low memory
}

# Filter state cache (simple cache)
FILTER_STATE_CACHE_CONFIG = {
    'CACHE_TYPE': 'SimpleCache',
    'CACHE_DEFAULT_TIMEOUT': 60 * 10,  # 10 minutes
}

# Explore form data cache (simple cache)
EXPLORE_FORM_DATA_CACHE_CONFIG = {
    'CACHE_TYPE': 'SimpleCache',
    'CACHE_DEFAULT_TIMEOUT': 60 * 10,  # 10 minutes
}

# ===========================
# Database & Query Configuration
# ===========================
# Allow additional database engines
PREVENT_UNSAFE_DB_CONNECTIONS = False

# SQL Lab configuration (reduced timeouts)
SQLLAB_TIMEOUT = 120  # Reduced from 300
SQLLAB_ASYNC_TIME_LIMIT_SEC = 60 * 10  # 10 minutes (reduced from 1 hour)
SQLLAB_VALIDATION_TIMEOUT = 5  # Reduced from 10

# Enable SQL Lab with limits
SQLLAB_CTAS_NO_LIMIT = False  # Enforce limits

# ===========================
# ClickHouse Specific Configuration
# ===========================
# Query cost estimation
QUERY_COST_FORMATTERS_BY_ENGINE = {
    'clickhouse': lambda cost: f'ClickHouse Query Cost: {cost:.2f}',
}

# ===========================
# Visualization & UI Configuration
# ===========================
# Set this API key to enable Mapbox visualizations
MAPBOX_API_KEY = os.environ.get('MAPBOX_API_KEY', '')

# Enable dashboard edit mode by default
DASHBOARD_EDIT_MODE = True

# Enable dashboard auto-refresh (limited intervals)
DASHBOARD_AUTO_REFRESH_MODE = "fetch"
DASHBOARD_AUTO_REFRESH_INTERVALS = [
    [60, "1 minute"],
    [300, "5 minutes"],
    [1800, "30 minutes"],
]

# ===========================
# Logging Configuration
# ===========================
# Minimal logging to save resources
ENABLE_PROXY_FIX = True
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'WARNING')  # Changed from INFO

# ===========================
# Resource Optimization
# ===========================
# Reduced workers for low CPU/RAM
SUPERSET_WORKERS = int(os.environ.get('SUPERSET_WORKERS', 2))  # Reduced from 4

# Disable compression to save CPU
COMPRESS_REGISTER = False

# ===========================
# Database Connection Pool (Optimized)
# ===========================
# Note: SQLite doesn't support connection pooling
# These settings only apply to PostgreSQL/MySQL metadata databases
# SQLALCHEMY_POOL_SIZE = 3
# SQLALCHEMY_POOL_TIMEOUT = 20
# SQLALCHEMY_MAX_OVERFLOW = 5
# SQLALCHEMY_POOL_RECYCLE = 1800
# SQLALCHEMY_POOL_PRE_PING = True

# ===========================
# Jinja Template Configuration
# ===========================
JINJA_CONTEXT_ADDONS = {
    'my_custom_function': lambda x: x * 2,
}

# Enable Jinja templating in SQL Lab
ENABLE_TEMPLATE_PROCESSING = True

# ===========================
# Additional Memory Optimizations
# ===========================
# Limit concurrent queries
SQLLAB_MAX_CONCURRENT_QUERIES = 2

# Reduce chart data point limits
SAMPLES_ROW_LIMIT = 500
FILTER_SELECT_ROW_LIMIT = 1000

# Disable expensive features
ENABLE_JAVASCRIPT_CONTROLS = False
