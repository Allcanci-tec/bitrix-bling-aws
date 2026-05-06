#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from pathlib import Path
from dotenv import load_dotenv

# Carrega o .env da pasta backend antes de importar o app
BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"

load_dotenv(dotenv_path=ENV_PATH, override=True)

print(f"[WSGI] .env carregado de: {ENV_PATH}")
print(f"[WSGI] BITRIX_WEBHOOK_URL: {'SET' if os.getenv('BITRIX_WEBHOOK_URL') else 'NOT SET'}")
print(f"[WSGI] BLING_CLIENT_ID: {'SET' if os.getenv('BLING_CLIENT_ID') else 'NOT SET'}")
print(f"[WSGI] BLING_CLIENT_SECRET: {'SET' if os.getenv('BLING_CLIENT_SECRET') else 'NOT SET'}")

from api.index import app