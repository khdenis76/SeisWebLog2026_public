import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "logs"
os.makedirs(LOG_DIR, exist_ok=True)

VERSION_FILE = BASE_DIR / "version.txt"
APP_VERSION = VERSION_FILE.read_text(encoding="utf-8").strip() if VERSION_FILE.exists() else "dev"

SECRET_KEY = 'dev-secret-key'
DEBUG = True

ALLOWED_HOSTS = ["127.0.0.1", "10.100.146.140", "172.21.77.154"]

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'core',
    'baseproject',
    'rov',
    'project_users.apps.ProjectUsersConfig',
    'source.apps.SourceConfig',
    'fleet.apps.FleetConfig',
    'reports.apps.ReportsConfig',
    "svp.apps.SvpConfig",
    'noar.apps.NoarConfig'
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    "core.middleware.ActiveProjectMiddleware",
    "baseproject.middleware.request_id.RequestIDMiddleware",
    "baseproject.middleware.audit_middleware.AuditRequestMiddleware",
]

ROOT_URLCONF = 'mysite.urls'

TEMPLATES = [{
    'BACKEND': 'django.template.backends.django.DjangoTemplates',
    'DIRS': [BASE_DIR / 'core' / 'templates'],
    'APP_DIRS': True,
    'OPTIONS': {
        'context_processors': [
            'django.template.context_processors.debug',
            'django.template.context_processors.request',
            'django.contrib.auth.context_processors.auth',
            'django.contrib.messages.context_processors.messages',
            "core.context_processors.theme_context",
            "core.context_processors.app_version",
            "core.context_processors.version_info",
        ]
    },
}]

WSGI_APPLICATION = 'mysite.wsgi.application'

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': BASE_DIR / 'db.sqlite3'
    }
}

AUTH_PASSWORD_VALIDATORS = [
    {'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator'},
    {'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator', 'OPTIONS': {'min_length': 8}},
    {'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator'},
    {'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator'},
]

# -------------------------------------------------------------------
# LOGGING
# Windows-safe for DEBUG=True:
# - use FileHandler instead of TimedRotatingFileHandler in development
# - use rotating handlers only when DEBUG=False
# -------------------------------------------------------------------
TECH_HANDLER_CLASS = "logging.FileHandler" if DEBUG else "logging.handlers.TimedRotatingFileHandler"
AUDIT_HANDLER_CLASS = "logging.FileHandler" if DEBUG else "logging.handlers.TimedRotatingFileHandler"

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,

    "formatters": {
        "verbose": {
            "()": "utils.logging_formatters.SafeExtraFormatter",
            "format": (
                "[%(asctime)s] %(levelname)s "
                "request_id=%(request_id)s "
                "user=%(username)s "
                "project=%(project_name)s "
                "method=%(method)s path=%(path)s "
                "status=%(status_code)s duration_ms=%(duration_ms)s "
                "%(name)s :: %(message)s"
            ),
        },
        "standard": {
            "()": "utils.logging_formatters.SafeExtraFormatter",
            "format": "[%(asctime)s] %(levelname)s %(name)s :: %(message)s",
        },
    },

    "filters": {},

    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "standard",
        },

        "tech_file": {
            "class": TECH_HANDLER_CLASS,
            "filename": str(LOG_DIR / "technical.log"),
            "formatter": "standard",
            "encoding": "utf-8",
            "delay": True,
            **({} if DEBUG else {
                "when": "midnight",
                "backupCount": 30,
            }),
        },

        "audit_file": {
            "class": AUDIT_HANDLER_CLASS,
            "filename": str(LOG_DIR / "audit.log"),
            "formatter": "verbose",
            "encoding": "utf-8",
            "delay": True,
            **({} if DEBUG else {
                "when": "midnight",
                "backupCount": 90,
            }),
        },

        "error_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": str(LOG_DIR / "errors.log"),
            "maxBytes": 10 * 1024 * 1024,
            "backupCount": 10,
            "formatter": "standard",
            "encoding": "utf-8",
            "delay": True,
        },
    },

    "loggers": {
        "django": {
            "handlers": ["console", "tech_file"],
            "level": "INFO",
            "propagate": True,
        },
        "django.request": {
            "handlers": ["error_file", "tech_file", "console"],
            "level": "ERROR",
            "propagate": False,
        },
        "seisweblog.tech": {
            "handlers": ["console", "tech_file", "error_file"],
            "level": "INFO",
            "propagate": False,
        },
        "seisweblog.audit": {
            "handlers": ["console", "audit_file"],
            "level": "INFO",
            "propagate": False,
        },
    },
}

LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'Europe/Bratislava'
USE_I18N = True
USE_TZ = True

STATIC_URL = 'static/'
STATICFILES_DIRS = [BASE_DIR / 'core' / 'static']
STATIC_ROOT = BASE_DIR / 'staticfiles'

MEDIA_URL = 'media/'
MEDIA_ROOT = BASE_DIR / 'media'

LOGIN_URL = 'login'
LOGIN_REDIRECT_URL = 'dashboard'
LOGOUT_REDIRECT_URL = 'login'

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'
DATA_UPLOAD_MAX_NUMBER_FILES = 1000