#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
token_auto_renewer.py

Renovador automático de token Bling para EC2

Responsável por renovar tokens Bling automaticamente antes de expirar.
Roda como daemon em background thread, verificando a cada 15 minutos.

Depende de: api/bling_token_manager.py
"""

import os
import json
import time
import threading
import traceback
from datetime import datetime
from api.bling_token_manager import refresh_bling_token, load_tokens, TOKENS_FILE

# ============================================================================
# CONFIGURAÇÃO
# ============================================================================

CHECK_INTERVAL_SECONDS = int(os.getenv("BLING_RENEW_CHECK_SECONDS", "900"))  # 15 min
RENEW_BEFORE_SECONDS = int(os.getenv("BLING_RENEW_BEFORE_SECONDS", "1800"))  # 30 min

# ============================================================================
# ESTADO DO SCHEDULER
# ============================================================================

_scheduler_running = False
_scheduler_thread = None
_last_check = None
_last_refresh = None
_invalid_grant_detected = False


def _renew_once():
    """Executa uma única verificação e renovação se necessário"""
    global _last_check, _last_refresh, _invalid_grant_detected
    
    try:
        _last_check = int(time.time())
        tokens = load_tokens()
        
        if not tokens or not tokens.get('access_token'):
            print("[AUTO-RENEW] ⚠️ Nenhum token carregado", flush=True)
            return
        
        saved_at = tokens.get('saved_at', 0)
        expires_in = tokens.get('expires_in', 21600)
        time_left = (saved_at + expires_in) - int(time.time())
        
        print(f"[AUTO-RENEW] ⏰ Token expira em {time_left}s ({time_left/3600:.1f}h)", flush=True)
        
        if time_left < RENEW_BEFORE_SECONDS:
            print(f"[AUTO-RENEW] 🔄 Renovando token (tempo restante: {time_left}s)", flush=True)
            result = refresh_bling_token()
            
            if result:
                _last_refresh = int(time.time())
                print(f"[AUTO-RENEW] ✅ Token renovado com sucesso!", flush=True)
            else:
                print(f"[AUTO-RENEW] ❌ Falha ao renovar", flush=True)
    
    except Exception as e:
        print(f"[AUTO-RENEW] ❌ Erro: {e}", flush=True)
        traceback.print_exc()


def _scheduler_loop():
    """Loop contínuo do scheduler"""
    global _scheduler_running
    
    print(f"[AUTO-RENEW] 🚀 Iniciado! Verifica a cada {CHECK_INTERVAL_SECONDS}s", flush=True)
    
    while _scheduler_running:
        try:
            _renew_once()
            time.sleep(CHECK_INTERVAL_SECONDS)
        except Exception as e:
            print(f"[AUTO-RENEW] ❌ Erro no loop: {e}", flush=True)
            time.sleep(CHECK_INTERVAL_SECONDS)


def iniciar_auto_renewer():
    """Inicia o daemon de renovação automática"""
    global _scheduler_running, _scheduler_thread
    
    if _scheduler_running:
        return True
    
    _scheduler_running = True
    _scheduler_thread = threading.Thread(target=_scheduler_loop, daemon=True)
    _scheduler_thread.start()
    return True


def parar_auto_renewer():
    """Para o daemon de renovação"""
    global _scheduler_running
    _scheduler_running = False


def obter_status_scheduler():
    """Retorna status do scheduler em JSON"""
    try:
        tokens = load_tokens()
        saved_at = tokens.get('saved_at', 0)
        expires_in = tokens.get('expires_in', 21600)
        time_left = (saved_at + expires_in) - int(time.time())
        
        return {
            "running": _scheduler_running,
            "check_interval": CHECK_INTERVAL_SECONDS,
            "renew_before": RENEW_BEFORE_SECONDS,
            "last_check": _last_check,
            "last_refresh": _last_refresh,
            "token_expires_in": max(0, time_left),
            "tokens_file": str(TOKENS_FILE),
            "timestamp": int(time.time())
        }
    except Exception as e:
        return {
            "error": str(e),
            "running": _scheduler_running,
            "timestamp": int(time.time())
        }
