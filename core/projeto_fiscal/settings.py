import os
import dj_database_url
from pathlib import Path

# ... (outras configs padrão do Django)

INSTALLED_APPS = [
    # ... apps padrão
    'core',
]

# Configuração do Banco de Dados (Neon.tech)
# Crie um arquivo .env com: DATABASE_URL=postgres://user:pass@endpoint.neon.tech/neondb?sslmode=require
DATABASES = {
    'default': dj_database_url.config(
        default='sqlite:///db.sqlite3',
        conn_max_age=600,
        ssl_require=True
    )
}

# Configurações de Template para Bootstrap (Opcional, mas recomendado)
TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        # ...
    },
]