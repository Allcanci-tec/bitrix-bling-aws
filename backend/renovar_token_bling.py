#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import time
import secrets
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlencode, urlparse, parse_qs
import requests
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("BLING_CLIENT_ID", "").strip()
CLIENT_SECRET = os.getenv("BLING_CLIENT_SECRET", "").strip()

AUTH_URL = "https://www.bling.com.br/Api/v3/oauth/authorize"
TOKEN_URL = "https://www.bling.com.br/Api/v3/oauth/token"
REDIRECT_URI = os.getenv("BLING_REDIRECT_URI", "https://extrator-contratos.vercel.app/callback").strip()
TOKENS_FILE = "tokens.json"

STATE = secrets.token_urlsafe(24)

oauth_result = {
    "code": None,
    "state": None,
    "error": None,
    "error_description": None,
}


def salvar_tokens(tokens):
    agora = int(time.time())
    expires_in = int(tokens.get("expires_in", 3600))
    
    dados = {
        "access_token": tokens.get("access_token", ""),
        "refresh_token": tokens.get("refresh_token", ""),
        "token_type": tokens.get("token_type", "Bearer"),
        "expires_in": expires_in,
        "saved_at": agora,
        "obtained_at": agora,
        "expires_at": agora + expires_in,
    }
    
    with open(TOKENS_FILE, "w", encoding="utf-8") as f:
        json.dump(dados, f, indent=2, ensure_ascii=False)
    
    return dados


def trocar_code_por_token(code):
    print("[INFO] Trocando authorization_code por tokens...")
    
    response = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": REDIRECT_URI,
        },
        auth=(CLIENT_ID, CLIENT_SECRET),
        headers={
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        timeout=30,
    )
    
    print(f"[INFO] HTTP token status: {response.status_code}")
    
    if response.status_code != 200:
        print("[ERRO] Falha ao trocar code por token.")
        print(response.text)
        return None
    
    return response.json()


class CallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        
        if parsed.path != "/callback":
            self.send_response(404)
            self.end_headers()
            self.wfile.write("Rota não encontrada.".encode("utf-8"))
            return
        
        query = parse_qs(parsed.query)
        
        code = query.get("code", [None])[0]
        state = query.get("state", [None])[0]
        error = query.get("error", [None])[0]
        error_description = query.get("error_description", [None])[0]
        
        print("[DEBUG] Callback recebido")
        print(f"[DEBUG] code: {code}")
        print(f"[DEBUG] state: {state}")
        print(f"[DEBUG] error: {error}")
        
        oauth_result["code"] = code
        oauth_result["state"] = state
        oauth_result["error"] = error
        oauth_result["error_description"] = error_description
        
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        
        if error:
            html = f"<html><body><h2>❌ Erro: {error}</h2><p>{error_description}</p></body></html>"
        elif code:
            html = "<html><body><h2>✅ Autorização recebida!</h2><p>Pode fechar esta aba.</p></body></html>"
        else:
            html = "<html><body><h2>⚠️ Callback sem code</h2><p>Volte ao terminal.</p></body></html>"
        
        self.wfile.write(html.encode("utf-8"))
        threading.Thread(target=self.server.shutdown, daemon=True).start()
    
    def log_message(self, format, *args):
        return


def montar_url_autorizacao():
    params = {
        "response_type": "code",
        "client_id": CLIENT_ID,
        "state": STATE,
        "redirect_uri": REDIRECT_URI,
    }
    return f"{AUTH_URL}?{urlencode(params)}"


def main():
    if not CLIENT_ID or not CLIENT_SECRET:
        print("[ERRO] BLING_CLIENT_ID ou BLING_CLIENT_SECRET não encontrado no .env")
        return
    
    print("=" * 60)
    print(" GERADOR DE TOKEN BLING VIA OAUTH")
    print("=" * 60)
    print(f"\nREDIRECT_URI: {REDIRECT_URI}")
    print(f"TOKENS_FILE: {os.path.abspath(TOKENS_FILE)}\n")
    
    auth_url = montar_url_autorizacao()
    
    print("[INFO] Abrindo navegador...")
    webbrowser.open(auth_url)
    
    print(f"[INFO] URL de Autorização: {auth_url[:80]}...\n")
    
    # Se não for localhost, esperar o callback
    if "localhost" not in REDIRECT_URI and "127.0.0.1" not in REDIRECT_URI:
        print("[INFO] Aguardando você autorizar no Bling...")
        print("[INFO] Deixe este script aberto enquanto autoriza.")
        print("[INFO] Pode levar até 60 segundos após autorizar.\n")
        
        # Esperar até 60 segundos pelo arquivo de tokens
        for i in range(60):
            if os.path.exists(TOKENS_FILE):
                try:
                    with open(TOKENS_FILE) as f:
                        data = json.load(f)
                        if data.get('access_token'):
                            print("\n✅ TOKENS SALVOS COM SUCESSO!")
                            print(f"   Arquivo: {os.path.abspath(TOKENS_FILE)}")
                            print(f"   Access: {data['access_token'][:20]}...")
                            print(f"   Refresh: {data['refresh_token'][:20]}...")
                            return
                except:
                    pass
            
            time.sleep(1)
        
        print("\n⏱️ Timeout: Tokens não foram salvos em 60 segundos.")
        print("Verifique se autorização funcionou e tente novamente.")
        return
    
    # Para localhost, usar servidor HTTP
    HOST = "localhost"
    PORT = 8080
    
    try:
        server = HTTPServer((HOST, PORT), CallbackHandler)
        print(f"✅ Servidor HTTP ouvindo em http://{HOST}:{PORT}\n")
        server.serve_forever()
    except OSError as e:
        print(f"❌ Erro ao iniciar servidor na porta {PORT}: {e}")
        return
    
    if oauth_result["error"]:
        print(f"\n[ERRO] Bling: {oauth_result['error']}")
        print(f"       {oauth_result['error_description']}")
        return
    
    code = oauth_result["code"]
    if not code:
        print("\n[ERRO] Nenhum code recebido.")
        return
    
    print("\n[OK] Authorization code recebido!")
    tokens = trocar_code_por_token(code)
    
    if not tokens:
        return
    
    dados = salvar_tokens(tokens)
    
    print("\n" + "=" * 60)
    print(" ✅ TOKEN GERADO COM SUCESSO!")
    print("=" * 60)
    print(f"Arquivo:    {os.path.abspath(TOKENS_FILE)}")
    print(f"Access:     {dados['access_token'][:20]}...")
    print(f"Refresh:    {dados['refresh_token'][:20]}...")
    print(f"Expira em:  {dados['expires_in']} segundos")
    print("=" * 60)


if __name__ == "__main__":
    main()
