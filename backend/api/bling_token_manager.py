"""
🔐 Gerenciador de Tokens Bling

Responsável por:
- Renovar tokens via OAuth2
- Armazenar tokens em tokens.json
- Prover tokens válidos para requisições
"""

import os
import json
import time
import threading
import requests
from pathlib import Path
from datetime import datetime

# ============================================================================
# CONFIGURAÇÃO
# ============================================================================

TOKEN_URL = "https://www.bling.com.br/Api/v3/oauth/token"
BLING_CLIENT_ID = os.getenv("BLING_CLIENT_ID", "").strip()
BLING_CLIENT_SECRET = os.getenv("BLING_CLIENT_SECRET", "").strip()

# Determinar caminho de tokens.json
def get_tokens_file_path():
    """Retorna o caminho correto do arquivo de tokens baseado no ambiente"""
    # Priorizar arquivo na raiz do projeto (backend/)
    local_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'tokens.json')
    if os.path.exists(local_file):
        return local_file
    
    # Procurar na raiz do projeto (dois níveis acima)
    project_root_file = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'tokens.json')
    if os.path.exists(project_root_file):
        return project_root_file
    
    # Em EC2/Linux, usar /tmp
    if os.path.exists('/tmp'):
        return '/tmp/tokens.json'
    
    # Fallback para arquivo local no backend
    return local_file

TOKENS_FILE = get_tokens_file_path()

# Lock para prevenir renovações paralelas
_token_lock = threading.Lock()
_renewal_in_progress = False
_cached_token = None

# ============================================================================
# CARREGAR E SALVAR TOKENS
# ============================================================================

def load_tokens():
    """Carrega tokens do arquivo tokens.json"""
    try:
        if os.path.exists(TOKENS_FILE):
            with open(TOKENS_FILE, "r", encoding="utf-8") as f:
                tokens = json.load(f)
                return tokens
    except Exception as e:
        print(f"[TOKEN-MGR] ⚠️ Erro ao carregar tokens: {e}")
    
    # Se não conseguiu carregar arquivo, tenta variáveis de ambiente
    return {
        "access_token": os.getenv("BLING_ACCESS_TOKEN", "").strip(),
        "refresh_token": os.getenv("BLING_REFRESH_TOKEN", "").strip(),
        "expires_in": 21600,  # 6 horas
    }

def save_tokens(tokens_data):
    """Salva tokens no arquivo tokens.json"""
    try:
        tokens_data["saved_at"] = int(time.time())
        os.makedirs(os.path.dirname(TOKENS_FILE) or ".", exist_ok=True)
        with open(TOKENS_FILE, "w", encoding="utf-8") as f:
            json.dump(tokens_data, f, ensure_ascii=False, indent=2)
        print(f"[TOKEN-MGR] ✅ Tokens salvos em: {TOKENS_FILE}")
        return True
    except Exception as e:
        print(f"[TOKEN-MGR] ⚠️ Erro ao salvar tokens: {e}")
        return False

# ============================================================================
# RENOVAÇÃO DE TOKEN
# ============================================================================

def refresh_bling_token():
    """
    Renova o access_token usando refresh_token
    
    Retorna:
        (success: bool, tokens: dict ou error_msg: str)
    """
    global _renewal_in_progress, _cached_token
    
    with _token_lock:
        if _renewal_in_progress:
            print(f"[TOKEN-MGR] ⏳ Renovação já em progresso - aguardando...")
            time.sleep(2)
            return True, load_tokens()
        
        _renewal_in_progress = True
    
    try:
        tokens = load_tokens()
        refresh_token = tokens.get("refresh_token", "").strip()
        
        if not refresh_token:
            print(f"[TOKEN-MGR] ❌ refresh_token não encontrado")
            return False, "refresh_token não encontrado"
        
        if not BLING_CLIENT_ID or not BLING_CLIENT_SECRET:
            print(f"[TOKEN-MGR] ❌ Credenciais Bling não configuradas")
            return False, "Credenciais Bling não configuradas"
        
        print(f"[TOKEN-MGR] 🔄 Renovando token Bling (refresh_token: {refresh_token[:20]}...)")
        
        response = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
            },
            auth=(BLING_CLIENT_ID, BLING_CLIENT_SECRET),
            headers={
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            timeout=30
        )
        
        if response.status_code == 200:
            new_tokens = response.json()
            tokens.update(new_tokens)
            tokens["saved_at"] = int(time.time())
            save_tokens(tokens)
            _cached_token = tokens
            
            print(f"[TOKEN-MGR] ✅ Token renovado com sucesso!")
            print(f"[TOKEN-MGR]    Expires in: {new_tokens.get('expires_in', 0)} segundos")
            
            _renewal_in_progress = False
            return True, tokens
        else:
            error_msg = f"HTTP {response.status_code}: {response.text[:200]}"
            print(f"[TOKEN-MGR] ❌ Erro ao renovar: {error_msg}")
            _renewal_in_progress = False
            return False, error_msg
    
    except Exception as e:
        print(f"[TOKEN-MGR] ❌ Exceção ao renovar token: {e}")
        _renewal_in_progress = False
        return False, str(e)

# ============================================================================
# OBTER TOKEN VÁLIDO
# ============================================================================

def get_valid_bling_token():
    """
    Retorna um token Bling válido
    - Usa cache se disponível
    - Carrega de tokens.json se não estiver em cache
    - Retorna None se não encontrar token
    """
    global _cached_token
    
    if _cached_token and _cached_token.get("access_token"):
        return _cached_token["access_token"]
    
    tokens = load_tokens()
    access_token = tokens.get("access_token", "").strip()
    
    if access_token:
        _cached_token = tokens
        return access_token
    
    print(f"[TOKEN-MGR] ❌ Token Bling não disponível")
    return None

def make_bling_request_with_auto_refresh(method, url, **kwargs):
    """
    Faz requisição para Bling com auto-refresh em caso de 401
    
    Parâmetros:
        method: GET, POST, PUT, PATCH, DELETE
        url: URL da API Bling
        **kwargs: parâmetros adicionais para requests
    
    Retorna:
        response: Response do requests
    """
    token = get_valid_bling_token()
    if not token:
        raise ValueError("Token Bling não disponível")
    
    headers = kwargs.get("headers", {})
    headers["Authorization"] = f"Bearer {token}"
    headers["Content-Type"] = "application/json"
    kwargs["headers"] = headers
    
    response = requests.request(method, url, timeout=30, **kwargs)
    
    # Se 401, tentar renovar e retry
    if response.status_code == 401:
        print(f"[TOKEN-MGR] ⚠️ Token expirado (401), tentando renovar...")
        success, _ = refresh_bling_token()
        
        if success:
            token = get_valid_bling_token()
            headers["Authorization"] = f"Bearer {token}"
            response = requests.request(method, url, timeout=30, **kwargs)
    
    return response
