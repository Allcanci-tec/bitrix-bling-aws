import os, json, time, tempfile, threading, sys, io
from flask import Flask, request, redirect, jsonify
import requests
from flask_cors import CORS
from datetime import datetime, timedelta
import hashlib
from pathlib import Path
from difflib import SequenceMatcher, get_close_matches
import traceback
# unicodedata e re removidos - não mais necessários

# Import do webhook handler
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from webhook_handler import processar_webhook_bitrix, is_deal_processed, save_processed_deal
    WEBHOOK_HANDLER_AVAILABLE = True
    print(f"[WEBHOOK] ✅ Módulo webhook_handler importado com sucesso!", flush=True)
except ImportError as e:
    WEBHOOK_HANDLER_AVAILABLE = False
    print(f"[WEBHOOK] ⚠️ Módulo webhook_handler não encontrado: {e}", flush=True)
    # Definir versões stub para evitar erros
    def is_deal_processed(deal_id):
        return False
    def save_processed_deal(deal_id):
        pass

# Import da função de criar pedido (com fallback para relative e absoluto)
try:
    from api.pedidos_vendas import criar_pedido_venda_bling
    PEDIDO_FUNCTION_AVAILABLE = True
    print(f"[INLINE] ✅ Função criar_pedido_venda_bling carregada (api.pedidos_vendas)", flush=True)
except ImportError:
    try:
        from pedidos_vendas import criar_pedido_venda_bling
        PEDIDO_FUNCTION_AVAILABLE = True
        print(f"[INLINE] ✅ Função criar_pedido_venda_bling carregada (pedidos_vendas)", flush=True)
    except ImportError as e:
        PEDIDO_FUNCTION_AVAILABLE = False
        print(f"[INLINE] ⚠️ Função criar_pedido_venda_bling não encontrada: {e}", flush=True)

# Import do gerenciador de tokens
try:
    from api.bling_token_manager import get_valid_bling_token, make_bling_request_with_auto_refresh, refresh_bling_token
    TOKEN_MANAGER_AVAILABLE = True
    print(f"[TOKEN-MGR] ✅ Gerenciador de tokens carregado (api.bling_token_manager)", flush=True)
except ImportError:
    try:
        from .bling_token_manager import get_valid_bling_token, make_bling_request_with_auto_refresh, refresh_bling_token
        TOKEN_MANAGER_AVAILABLE = True
        print(f"[TOKEN-MGR] ✅ Gerenciador de tokens carregado (.bling_token_manager)", flush=True)
    except ImportError as e2:
        TOKEN_MANAGER_AVAILABLE = False
        print(f"[TOKEN-MGR] ⚠️ Gerenciador de tokens não encontrado: {e2}", flush=True)
try:
    # Adiciona o diretório pai ao path para importar do projeto
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from validacao_nomes import verificar_nome_bloqueado
    VALIDACAO_NOMES_AVAILABLE = True
except ImportError as e:
    VALIDACAO_NOMES_AVAILABLE = False
    print(f"[VALIDACAO] ⚠️ Módulo validacao_nomes não encontrado: {e}", flush=True)

# 🔐 Import da validação de stage (gatekeeper para pedidos)
try:
    from api.validacao_stage import validar_stage_para_pedido
    VALIDACAO_STAGE_AVAILABLE = True
    print(f"[STAGE-VALIDATION] ✅ Validação de stage carregada (api.validacao_stage)", flush=True)
except ImportError:
    try:
        from validacao_stage import validar_stage_para_pedido
        VALIDACAO_STAGE_AVAILABLE = True
        print(f"[STAGE-VALIDATION] ✅ Validação de stage carregada (validacao_stage)", flush=True)
    except ImportError as e:
        VALIDACAO_STAGE_AVAILABLE = False
        print(f"[STAGE-VALIDATION] ⚠️ Validação de stage não encontrada: {e}", flush=True)
        # Fallback: sempre aceitar (não é ideal, mas garante que o sistema continue rodando)
        def validar_stage_para_pedido(stage_id):
            return True, "Fallback: validação desativada"

# 📦 Import do carregador de cache (Vendedores Bling + Usuários Bitrix)
try:
    from cache_loader import get_cache
    CACHE_AVAILABLE = True
    CACHE_MANAGER = get_cache()
    print(f"[CACHE] ✅ Cache manager carregado com sucesso!", flush=True)
except ImportError as e:
    CACHE_AVAILABLE = False
    CACHE_MANAGER = None
    print(f"[CACHE] ⚠️ Cache manager não encontrado: {e}", flush=True)

# 🔄 Import do auto-renovador de token (CRÍTICO PARA EC2)
try:
    from token_auto_renewer import iniciar_auto_renewer, parar_auto_renewer, obter_status_scheduler
    AUTO_RENEWER_AVAILABLE = True
    print(f"[AUTO-RENEW] ✅ Auto-renewer carregado com sucesso!", flush=True)
except ImportError as e:
    AUTO_RENEWER_AVAILABLE = False
    print(f"[AUTO-RENEW] ⚠️ Auto-renewer não disponível: {e}", flush=True)
    # Stubs se não conseguir importar
    def iniciar_auto_renewer():
        return False
    def parar_auto_renewer():
        pass
    def obter_status_scheduler():
        return {'status': 'não_disponível', 'error': 'APScheduler não instalado'}

# ⚠️ COMENTADO: UTF-8 wrapper pode entupir o buffer no Windows
# Se você precisar de emojis, execute o PowerShell como Unicode
# Descomente se tiver problemas com caracteres especiais:
# if sys.stdout.encoding != 'utf-8':
#     sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
# if sys.stderr.encoding != 'utf-8':
#     sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Cache para evitar criações duplicadas simultâneas
_contato_creation_cache = {}
_cache_lock = threading.Lock()

# FUNÇÕES DE NORMALIZAÇÃO DE NOME REMOVIDAS
# Substituídas por mapeamento direto Bitrix ID -> Bling ID
# para maior confiabilidade e performance

def _verificar_saude_api_bling(access_token, timeout=5):
    """
    Verifica se a API do Bling está funcionando.
    Retorna: (está_viva, motivo)
    """
    try:
        headers = {"Authorization": f"Bearer {access_token}"}
        # Endpoint leve para healthcheck
        url = f"{BLING_API_BASE}/contatos?limite=1"
        
        response = requests.get(url, headers=headers, timeout=timeout)
        
        if response.status_code == 200:
            print(f"[BLING] ✅ API está ONLINE")
            return (True, "OK")
        elif response.status_code in [503, 502, 504]:
            print(f"[BLING] 🚨 API está OFFLINE - HTTP {response.status_code}")
            return (False, f"API indisponível (HTTP {response.status_code})")
        elif response.status_code == 401:
            print(f"[BLING] ⚠️ Token inválido - HTTP 401")
            return (False, "Token expirado ou inválido")
        else:
            print(f"[BLING] ⚠️ API respondeu com HTTP {response.status_code}")
            return (False, f"Erro HTTP {response.status_code}")
            
    except requests.exceptions.Timeout:
        print(f"[BLING] ⏱️ TIMEOUT no healthcheck")
        return (False, "API timeout - muito lenta")
    except requests.exceptions.ConnectionError:
        print(f"[BLING] 🌐 Erro de conexão com API Bling")
        return (False, "Erro de conexão - sem internet?")
    except Exception as e:
        print(f"[BLING] 💥 Erro ao verificar saúde: {e}")
        return (False, f"Erro interno: {str(e)}")

def _resolver_nome_usuario_bitrix(user_id) -> tuple:
    """
    Resolve nome COMPLETO do usuário Bitrix usando CACHE
    (Rápido - O(1) lookup no JSON)
    
    Retorna: (nome_usuario, nome_vendedor)
    Exemplo: (436) → ("Tarik Della Santina Mohallem", "Tarik Della Santina Mohallem")
    """
    
    if not CACHE_AVAILABLE or not CACHE_MANAGER:
        print(f"[CACHE] ⚠️ Cache não disponível - retornando None")
        return None, None
    
    user_name, vendor_name = CACHE_MANAGER.resolve_vendor(user_id)
    
    if user_name:
        print(f"[CACHE] ✅ Usuário encontrado: ID {user_id} → {user_name}")
    else:
        print(f"[CACHE] ⚠️ Usuário ID {user_id} não encontrado no cache")
    
    return user_name, vendor_name

def _fazer_requisicao_com_retry(metodo, url, headers=None, json_data=None, timeout=10, max_tentativas=3):
    """
    Faz requisição HTTP com retry automático para erros 503, 502, 504
    """
    import time
    
    headers = headers or {}
    
    for tentativa in range(1, max_tentativas + 1):
        try:
            print(f"[REQUISICAO] Tentativa {tentativa}/{max_tentativas} - {metodo.upper()} {url[:80]}")
            
            if metodo.lower() == 'get':
                response = requests.get(url, headers=headers, timeout=timeout)
            elif metodo.lower() == 'post':
                response = requests.post(url, headers=headers, json=json_data, timeout=timeout)
            elif metodo.lower() == 'put':
                response = requests.put(url, headers=headers, json=json_data, timeout=timeout)
            else:
                raise ValueError(f"Método HTTP não suportado: {metodo}")
            
            # SUCESSO
            if response.status_code in [200, 201]:
                print(f"[REQUISICAO] ✅ Sucesso - HTTP {response.status_code}")
                return (True, response)
            
            # ERRO TRANSITÓRIO - RETRY
            if response.status_code in [503, 502, 504]:
                print(f"[REQUISICAO] ⚠️ Erro transitório HTTP {response.status_code} - API indisponível")
                if tentativa < max_tentativas:
                    tempo_espera = 2 ** tentativa  # 2, 4, 8 segundos
                    print(f"[REQUISICAO] ⏳ Aguardando {tempo_espera}s antes de retry...")
                    time.sleep(tempo_espera)
                    continue
                else:
                    print(f"[REQUISICAO] ❌ Falha permanente - tentativas esgotadas")
                    return (False, response, "API Bling indisponível - tente novamente mais tarde")
            
            # ERRO IMEDIATO (não fazer retry)
            if response.status_code in [400, 401, 403, 404]:
                print(f"[REQUISICAO] ❌ Erro permanente HTTP {response.status_code}")
                return (False, response, f"Erro HTTP {response.status_code}")
            
            # OUTRO ERRO
            print(f"[REQUISICAO] ⚠️ Erro HTTP {response.status_code}")
            return (False, response, f"Erro HTTP {response.status_code}")
                
        except requests.exceptions.Timeout:
            print(f"[REQUISICAO] ⏱️ TIMEOUT")
            if tentativa < max_tentativas:
                tempo_espera = 2 ** tentativa
                print(f"[REQUISICAO] ⏳ Aguardando {tempo_espera}s before retry...")
                time.sleep(tempo_espera)
            else:
                return (False, None, "Timeout na requisição - API muito lenta")
                
        except requests.exceptions.ConnectionError:
            print(f"[REQUISICAO] 🌐 Erro de conexão")
            if tentativa < max_tentativas:
                time.sleep(2 ** tentativa)
            else:
                return (False, None, "Erro de conexão - sem internet?")
                
        except Exception as e:
            print(f"[REQUISICAO] 💥 Exceção: {e}")
            return (False, None, f"Erro interno: {str(e)}")
    
    return (False, None, "Falha após todas as tentativas")

# FUNÇÕES HELPER DE CACHE

def _is_recently_processed(cache_key, ttl_seconds=60):
    """Verifica se um contato foi processado recentemente"""
    with _cache_lock:
        if cache_key in _contato_creation_cache:
            created_at = _contato_creation_cache[cache_key]['timestamp']
            if time.time() - created_at < ttl_seconds:
                return _contato_creation_cache[cache_key]['result']
        return None

def _cache_contato_result(cache_key, result):
    """Armazena resultado no cache"""
    with _cache_lock:
        _contato_creation_cache[cache_key] = {
            'result': result,
            'timestamp': time.time()
        }
        
        # Limpeza do cache (manter só últimos 100)
        if len(_contato_creation_cache) > 100:
            oldest_keys = sorted(_contato_creation_cache.keys(), 
                               key=lambda k: _contato_creation_cache[k]['timestamp'])[:50]
            for old_key in oldest_keys:
                del _contato_creation_cache[old_key]

def _get_contato_cache_key(nome, cnpj_limpo):
    """Gera chave de cache para contato baseado em nome e CNPJ"""
    chave = f"{nome}_{cnpj_limpo}".lower().strip()
    return hashlib.md5(chave.encode()).hexdigest()


def _process_webhook_inline_sync(payload, bitrix_url, bling_endpoint_url):
    """
    NOVA VERSÃO SÍNCRONA: Processa webhook COMPLETAMENTE antes de retornar.
    
    Fluxo:
    1. Validar payload
    2. Buscar dados do Bitrix
    3. Criar contato no Bling
    4. Criar pedido no Bling
    5. Retornar (sucesso, mensagem)
    
    ⚠️ NENHUMA THREAD! NENHUMA ASYNC! Tudo SÍNCRONO E BLOQUEANTE!
    
    Returns:
        (sucesso: bool, mensagem: str)
    """
    print(f"\n{'='*70}")
    print(f"[INLINE] ===== PROCESSAMENTO INLINE SÍNCRONO =====")
    print(f"{'='*70}\n")
    
    try:
        # ====================================================================
        # 1. EXTRAIR DADOS DO PAYLOAD
        # ====================================================================
        
        print("[INLINE] 1️⃣ Validando payload...")
        sys.stdout.flush()
        
        data = payload.get('data', {})
        fields = data.get('FIELDS', {})
        
        deal_id = fields.get('ID')
        stage_id_raw = fields.get('STAGE_ID', '')
        stage_id = stage_id_raw.upper() if stage_id_raw else ''
        deal_title = fields.get('TITLE', 'Deal')
        contact_id_bitrix = fields.get('CONTACT_ID')
        company_id_bitrix = fields.get('COMPANY_ID')
        opportunity = fields.get('OPPORTUNITY', 0)
        
        print(f"[INLINE] 🔍 DADOS DO PAYLOAD:")
        print(f"[INLINE]    • Deal ID: {deal_id}")
        print(f"[INLINE]    • Stage (raw): {repr(stage_id_raw)}")
        print(f"[INLINE]    • Stage (upper): {repr(stage_id)}")
        print(f"[INLINE]    • Título: {deal_title}")
        print(f"[INLINE]    • Contact ID: {contact_id_bitrix}")
        print(f"[INLINE]    • Company ID: {company_id_bitrix}")
        print(f"[INLINE]    • Valor: R$ {opportunity}")
        sys.stdout.flush()
        
        if not deal_id:
            msg = "❌ Deal ID não encontrado no payload"
            print(f"[INLINE] {msg}")
            sys.stdout.flush()
            return (False, msg)
        
        if 'WON' not in stage_id:
            msg = f"⏭️ Deal não está em WON. Stage recebido: '{stage_id_raw}'"
            print(f"[INLINE] {msg}")
            sys.stdout.flush()
            return (False, msg)
        
        # ✅ ATIVADO: Verificar deduplicação (evitar processar mesma deal 2x)
        if is_deal_processed(deal_id):
            msg = f"⏭️ Deal #{deal_id} já foi processada anteriormente (DUPLICATE - IGNORAR)"
            print(f"[INLINE] {msg}")
            sys.stdout.flush()
            return (False, msg)
        
        print(f"[INLINE] ✅ Primeira vez processando deal #{deal_id}")
        sys.stdout.flush()
        print(f"[INLINE]    Stage: {stage_id}")
        print(f"[INLINE]    Valor: R$ {opportunity}")
        
        # ✅ PRÉ-LOG: Mostrar qual é o responsável da deal AGORA
        print(f"[INLINE]")
        print(f"[INLINE] 👤 INFORMAÇÕES DO RESPONSÁVEL:")
        print(f"[INLINE]    • ASSIGNED_BY_ID (responsável da deal): {repr(fields.get('ASSIGNED_BY_ID'))}")
        
        # ====================================================================
        # 2. BUSCAR DADOS COMPLETOS DO BITRIX
        # ====================================================================
        
        print(f"\n[INLINE] 2️⃣ Buscando dados do Bitrix...")
        
        # Headers corretos para Bitrix (evita SSL/EOF errors)
        bitrix_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            'Connection': 'close'
        }
        
        # Buscar dados da deal
        try:
            resp_deal = requests.post(
                bitrix_url + 'crm.deal.get',
                json={'id': deal_id},
                headers=bitrix_headers,
                timeout=15
            )
            
            if resp_deal.status_code != 200:
                error_detail = resp_deal.text[:200]
                msg = f"Erro ao buscar deal: HTTP {resp_deal.status_code} - {error_detail}"
                print(f"[INLINE] ❌ {msg}")
                return (False, msg)
            
            deal_data = resp_deal.json().get('result', {})
            print(f"[INLINE] ✅ Deal encontrada")
            
            # ── Re-verificar stage com dado fresco do Bitrix (não confiar só no payload do webhook) ──
            stage_id_real = (deal_data.get('STAGE_ID', '') or '').upper()
            print(f"[INLINE] 🔄 Stage real (Bitrix API): '{stage_id_real}'  |  Stage do webhook: '{stage_id}'")
            if 'WON' not in stage_id_real:
                msg = f"⏭️ Deal #{deal_id} não está em WON (stage atual: '{stage_id_real}'). Ignorando."
                print(f"[INLINE] {msg}")
                sys.stdout.flush()
                return (False, msg)
            
        except Exception as e:
            msg = f"Exception ao buscar deal: {str(e)}"
            print(f"[INLINE] ❌ {msg}")
            return (False, msg)
        
        # Buscar empresa (se houver company_id)
        empresa = {}
        if company_id_bitrix:
            try:
                resp_empresa = requests.post(
                    bitrix_url + 'crm.company.get',
                    json={'id': company_id_bitrix},
                    headers=bitrix_headers,
                    timeout=15
                )
                
                if resp_empresa.status_code == 200:
                    empresa = resp_empresa.json().get('result', {})
                    print(f"[INLINE] ✅ Empresa encontrada: {empresa.get('TITLE', 'Unknown')}")
                    
                    # 🔍 DEBUG: Mostrar todos os campos da empresa
                    print(f"\n[INLINE] 🔍 === CAMPOS DA EMPRESA DO BITRIX ===")
                    print(f"[INLINE]    TITLE (Nome): '{empresa.get('TITLE', '')}'")
                    print(f"[INLINE]    UF_CRM_FANTASIA (Fantasia/Razão): '{empresa.get('UF_CRM_FANTASIA', '')}'")
                    print(f"[INLINE]    LATIN_NAME: '{empresa.get('LATIN_NAME', '')}'")
                    print(f"[INLINE]    INN: '{empresa.get('INN', '')}'")
                    print(f"[INLINE]    UF_CRM_1713291425 (CNPJ): '{empresa.get('UF_CRM_1713291425', '')}'")
                    # Mostrar TODOS os campos UF_CRM_* para ajudar a identificar
                    print(f"[INLINE]    Campos UF_CRM_*:")
                    for key, value in empresa.items():
                        if key.startswith('UF_CRM_'):
                            print(f"[INLINE]      {key}: '{value}'")
                    print(f"[INLINE]")
                    
            except Exception as e:
                print(f"[INLINE] ⚠️ Não conseguiu buscar empresa: {e}")
        
        # Buscar contato (se houver contact_id)
        contato_bitrix = {}
        if contact_id_bitrix:
            try:
                resp_contato = requests.post(
                    bitrix_url + 'crm.contact.get',
                    json={'id': contact_id_bitrix},
                    headers=bitrix_headers,
                    timeout=15
                )
                
                if resp_contato.status_code == 200:
                    contato_bitrix = resp_contato.json().get('result', {})
                    print(f"[INLINE] ✅ Contato encontrado: {contato_bitrix.get('NAME', 'Unknown')}")
                    
            except Exception as e:
                print(f"[INLINE] ⚠️ Não conseguiu buscar contato: {e}")
        
        # Buscar produtos
        produtos = []
        try:
            resp_produtos = requests.post(
                bitrix_url + 'crm.deal.productrows.get',
                json={'id': deal_id},
                headers=bitrix_headers,
                timeout=15
            )
            
            if resp_produtos.status_code == 200:
                produtos = resp_produtos.json().get('result', [])
                print(f"[INLINE] ✅ {len(produtos)} produto(s) encontrado(s)")
                
        except Exception as e:
            print(f"[INLINE] ⚠️ Não conseguiu buscar produtos: {e}")
        
        if not produtos:
            msg = "❌ Deal sem produtos - NÃO será processada"
            print(f"[INLINE] {msg}")
            sys.stdout.flush()
            return (False, msg)
        
        print(f"[INLINE] ✅ Deal tem {len(produtos)} produto(s)")
        sys.stdout.flush()
        
        # ====================================================================
        # 3. VALIDAÇÃO CRÍTICA: BLOQUEIO DE EMPRESA INVÁLIDA
        # ====================================================================
        
        # Preparar dados de empresa para a função
        empresa_data = {
            **empresa,           # Todos os dados da empresa do Bitrix
            'bitrix_company_id': company_id_bitrix
        }
        
        # ════════════════════════════════════════════════════════════════════════
        # 🆕 BUSCA DO RESPONSÁVEL: Extrair nome de quem está responsável pela deal
        # ════════════════════════════════════════════════════════════════════════
        print(f"\n[INLINE] {'='*70}")
        print(f"[INLINE] 🔍 BUSCANDO NOME DO RESPONSÁVEL DA DEAL (ASSIGNED_BY_ID)")
        print(f"[INLINE] {'='*70}")
        
        assigned_by_id = deal_data.get('ASSIGNED_BY_ID') if deal_data else None
        nome_responsavel_contato = ''
        
        print(f"[INLINE] 📌 ASSIGNED_BY_ID (ID do responsável): {repr(assigned_by_id)}")
        
        if assigned_by_id:
            print(f"[INLINE] 📥 Resolvendo nome do usuário via CACHE...")
            
            # 🚀 USANDO CACHE (muito mais rápido que user.get)
            user_name, vendor_name = _resolver_nome_usuario_bitrix(assigned_by_id)
            
            if user_name:
                nome_responsavel_contato = user_name
                print(f"[INLINE] ✅ RESPONSÁVEL EXTRAÍDO COM SUCESSO (DO CACHE):")
                print(f"[INLINE]    • ID Bitrix: {assigned_by_id}")
                print(f"[INLINE]    • Nome completo: '{nome_responsavel_contato}'")
            else:
                print(f"[INLINE] ❌ Usuário não encontrado no cache")
        else:
            print(f"[INLINE] ⚠️  ASSIGNED_BY_ID não informado no payload")
        
        # ════════════════════════════════════════════════════════════════════════
        # RESULTADO FINAL DO RESPONSÁVEL
        # ════════════════════════════════════════════════════════════════════════
        print(f"[INLINE]")
        print(f"[INLINE] {'='*70}")
        print(f"[INLINE] ✅ PREPARANDO RESPONSÁVEL PARA TRÁFEGO")
        print(f"[INLINE] {'='*70}")
        
        if nome_responsavel_contato:
            empresa_data['responsavel_representante'] = nome_responsavel_contato
            print(f"[INLINE] ✅ RESPONSÁVEL FOI ADICIONADO COM SUCESSO!")
            print(f"[INLINE]    • 👤 Nome extraído do CACHE: '{nome_responsavel_contato}'")
            print(f"[INLINE]    • 📍 Armazenado em: empresa_data['responsavel_representante']")
            print(f"[INLINE]")
            print(f"[INLINE] 🔄 PRÓXIMOS PASSOS:")
            print(f"[INLINE]    1️⃣ Este nome será usado para resolver vendedor no Bling")
            print(f"[INLINE]    2️⃣ Tentará mapeamento direto (Bitrix ID → Bling ID)")
            print(f"[INLINE]    3️⃣ Se encontrado: será usado como vendedor do contato")
            print(f"[INLINE]    4️⃣ Campo Bling: 'vendedor': {{'id': VENDOR_ID}}")
        else:
            print(f"[INLINE] ⚠️  RESPONSÁVEL NÃO ENCONTRADO")
            print(f"[INLINE]    • Possíveis causas:")
            print(f"[INLINE]      - ASSIGNED_BY_ID não informado no deal")
            print(f"[INLINE]      - user.get retornou resposta vazia")
            print(f"[INLINE]      - Erro de conexão com Bitrix")
            print(f"[INLINE]    • Ação: Deal será criada SEM representante")
        
        print(f"[INLINE]")
        
        # 🚫 VALIDAÇÃO CRÍTICA: BLOQUEIO DE CONTATO ALEATÓRIO
        nome_empresa = empresa_data.get('TITLE', '').strip()
        cnpj_empresa = empresa_data.get('UF_CRM_1713291425', '')
        cnpj_limpo = ''.join(c for c in str(cnpj_empresa) if c.isdigit()) if cnpj_empresa else ''
        
        # ✅ EXIGÊNCIA OBRIGATÓRIA: CNPJ válido com 14 dígitos
        empresa_tem_cnpj_valido = bool(cnpj_limpo and len(cnpj_limpo) == 14)
        empresa_tem_nome_valido = bool(nome_empresa and nome_empresa not in ['', 'SEM NOME', 'Cliente', 'Empresa'])
        
        print(f"\n[INLINE] 🔒 VALIDAÇÃO DE EMPRESA (VERSÃO RIGOROSA):")
        print(f"[INLINE]    ✓ CNPJ válido (14 dígitos)? {empresa_tem_cnpj_valido} ('{cnpj_limpo}')")
        print(f"[INLINE]    ✓ Nome válido? {empresa_tem_nome_valido} ('{nome_empresa}')")
        sys.stdout.flush()
        
        if not empresa_tem_cnpj_valido:
            msg = f"""❌ BLOQUEADO: CNPJ obrigatório não preenchido
[INLINE]    • Nome da Deal: {deal_title}
[INLINE]    • Company ID: {company_id_bitrix}
[INLINE]    • CNPJ empresa: '{cnpj_limpo}' ({len(cnpj_limpo) if cnpj_limpo else 0} dígitos, esperado 14)
[INLINE]    
[INLINE]    🚫 NÃO é permitido processar deals SEM CNPJ válido!
[INLINE]    ℹ️ Solução: Preencha o campo UF_CRM_1713291425 (CNPJ) com um CNPJ válido de 14 dígitos no Bitrix"""
            print(f"[INLINE] {msg}")
            sys.stdout.flush()
            return (False, msg)
        
        print(f"\n[INLINE] 3️⃣ Obtendo token Bling...")
        sys.stdout.flush()
        
        # Token do Bling (com renovação automática via Vercel KV)
        if TOKEN_MANAGER_AVAILABLE:
            try:
                bling_token = get_valid_bling_token()
                print(f"[INLINE]    ✅ Token obtido via gerenciador de tokens")
                sys.stdout.flush()
            except Exception as e:
                print(f"[INLINE]    ❌ Erro ao obter token via gerenciador: {e}")
                sys.stdout.flush()
                bling_token = None
        else:
            bling_token = os.getenv("BLING_ACCESS_TOKEN", "").strip()
            if bling_token:
                print(f"[INLINE]    ✅ Token obtido via variável de ambiente")
            else:
                print(f"[INLINE]    ⚠️ Token não encontrado em variáveis de ambiente")
            sys.stdout.flush()
        
        if not bling_token:
            msg = "❌ BLING_ACCESS_TOKEN não configurado - FALHA na autenticação"
            print(f"[INLINE] {msg}")
            print(f"[INLINE] Verifique: 1) Se tokens.json existe 2) Se .env.local tem BLING_ACCESS_TOKEN")
            sys.stdout.flush()
            return (False, msg)
        
        print(f"[INLINE]    Token preview: {bling_token[:20]}...")
        print(f"[INLINE] ✅ Autenticação OK - prosseguindo")
        sys.stdout.flush()
        
        try:
            # ════════════════════════════════════════════════════════════════════════
            # 4️⃣ CRIAR CONTATO NO BLING COM O RESPONSÁVEL ENCONTRADO
            # ════════════════════════════════════════════════════════════════════════
            print(f"\n[INLINE] {'='*70}")
            print(f"[INLINE] 4️⃣ CRIANDO CONTATO NO BLING")
            print(f"[INLINE] {'='*70}")
            
            responsavel_final = empresa_data.get('responsavel_representante', '')
            print(f"[INLINE]")
            print(f"[INLINE] 📝 Dados para CRIAR CONTATO:")
            print(f"[INLINE]    • Empresa: '{empresa_data.get('TITLE', 'SEM NOME')}'")
            print(f"[INLINE]    • CNPJ: {empresa_data.get('UF_CRM_1713291425', 'NÃO INFORMADO')}")
            print(f"[INLINE]    • Responsável: '{responsavel_final if responsavel_final else '(vazio - nenhum encontrado)'}'")
            print(f"[INLINE]")
            
            nome_responsavel_contato_final = empresa_data.get('responsavel_representante', None)

            # Resolver vendedor_id via VENDEDOR_MAP (Bitrix user ID -> Bling numeric vendor ID)
            # FASE 1: mapeamento estático direto
            print(f"\n[INLINE] {'='*70}")
            print(f"[INLINE] 🔧 RESOLVENDO VENDEDOR NO BLING")
            print(f"[INLINE] {'='*70}")
            
            vendedor_id_resolvido = None
            bitrix_assigned_id_str = str(assigned_by_id) if assigned_by_id else None
            
            print(f"[INLINE]")
            print(f"[INLINE] 📌 DADOS PARA RESOLUÇÃO:")
            print(f"[INLINE]    • Nome do responsável (Bitrix): '{nome_responsavel_contato_final}'")
            print(f"[INLINE]    • ID do responsável (Bitrix): {bitrix_assigned_id_str}")
            print(f"[INLINE]")
            print(f"[INLINE] FASE 1: Mapeamento estático (VENDEDOR_MAP)")
            print(f"[INLINE]    • Bitrix ID: '{bitrix_assigned_id_str}'")
            print(f"[INLINE]    • Mapeamentos disponíveis: {list(VENDEDOR_MAP.keys())}")
            
            if bitrix_assigned_id_str and bitrix_assigned_id_str in VENDEDOR_MAP:
                vendedor_id_resolvido = VENDEDOR_MAP[bitrix_assigned_id_str]
                print(f"[INLINE] ✅ ENCONTRADO EM MAPEAMENTO ESTÁTICO:")
                print(f"[INLINE]    • Bitrix ID {bitrix_assigned_id_str} → Bling ID {vendedor_id_resolvido}")
            else:
                print(f"[INLINE] ⚠️ Bitrix ID não encontrado em VENDEDOR_MAP")
                print(f"[INLINE]")
                print(f"[INLINE] FASE 2: Busca dinâmica via FUZZY MATCHING")
                
                # FASE 2: buscar dinamicamente pelo nome do responsável via API Bling /vendedores
                nome_para_busca = nome_responsavel_contato_final or ''
                if nome_para_busca:
                    print(f"[INLINE]    • 🔍 Searching in Bling API /vendedores...")
                    print(f"[INLINE]    • 👤 Nome para buscar: '{nome_para_busca}'")
                    print(f"[INLINE]    • 📊 Estratégia: Fuzzy matching com múltiplos critérios")
                    
                    vendedor_id_resolvido = buscar_vendedor_por_nome_flexivel(bling_token, nome_para_busca)
                    
                    if vendedor_id_resolvido:
                        print(f"[INLINE] ✅ ENCONTRADO NA API BLING:")
                        print(f"[INLINE]    • Nome Bitrix: '{nome_para_busca}'")
                        print(f"[INLINE]    • Bling Vendor ID: {vendedor_id_resolvido}")
                    else:
                        print(f"[INLINE] ❌ Não encontrado na API Bling")
                        print(f"[INLINE]    • Nome '{nome_para_busca}' não teve match com vendedores")
                        print(f"[INLINE]    • Verifique os vendedores cadastrados:")
                        print(f"[INLINE]      - Acesse Bling > Configurações > Vendedores")
                        print(f"[INLINE]      - Compare com o nome: '{nome_para_busca}'")
                else:
                    print(f"[INLINE]    • ⚠️ Nenhum nome de responsável para buscar")
            
            print(f"[INLINE]")
            print(f"[INLINE] {'='*70}")
            print(f"[INLINE] 📋 RESULTADO FINAL:")
            print(f"[INLINE] {'='*70}")
            if vendedor_id_resolvido:
                print(f"[INLINE] ✅ Vendedor RESOLVIDO com sucesso:")
                print(f"[INLINE]    • 🏢 Nome: '{nome_responsavel_contato_final}'")
                print(f"[INLINE]    • 🔢 ID Bling: {vendedor_id_resolvido}")
                print(f"[INLINE]    • 📝 Será usado em:")
                print(f"[INLINE]      - Contato: campo 'vendedor': {{'id': {vendedor_id_resolvido}}}")
                print(f"[INLINE]      - Pedido: campo 'vendedor': {{'id': {vendedor_id_resolvido}}}")
            else:
                print(f"[INLINE] ⚠️ Vendedor NÃO RESOLVIDO:")
                print(f"[INLINE]    • Nome pesquisado: '{nome_responsavel_contato_final}'")
                print(f"[INLINE]    • Motivo: Nenhuma correspondência encontrada no Bling")
                print(f"[INLINE]    • Contato será criado SEM vendedor específico")
                print(f"[INLINE]    • Pedido será criado SEM vendedor específico")
            
            print(f"[INLINE]")
            print(f"[INLINE] {'='*70}")
            print(f"[INLINE] 🔗 CHAMANDO criar_contato_bling()")
            print(f"[INLINE] {'='*70}")
            print(f"[INLINE]    • vendedor_nome: {repr(nome_responsavel_contato_final)}")
            print(f"[INLINE]    • vendedor_id: {repr(vendedor_id_resolvido)}")
            print(f"[INLINE]    • deal_title: {repr(deal_title)}")
            print(f"[INLINE]")
            print(f"[INLINE] 🎯 ESTRATÉGIA:")
            print(f"[INLINE]    • Sempre usar NOME do cache (não ID fixo)")
            print(f"[INLINE]    • Se encontrou ID no Bling: usar ID + Nome")
            print(f"[INLINE]    • Se não encontrou: usar só Nome")
            print(f"[INLINE]")

            # Chamar a função existente do usuário
            # IMPORTANTE: Passar vendedor_nome (do cache) - NUNCA vendedor_id fixo!
            # IMPORTANTE: criar_contato_bling retorna uma TUPLE (contato_dict, error)
            contato_result, contato_error = criar_contato_bling(bling_token, empresa_data, vendedor_id=vendedor_id_resolvido, vendedor_nome=nome_responsavel_contato_final, deal_title=deal_title)
            
            print(f"[INLINE]")
            print(f"[INLINE] 📥 Retorno de criar_contato_bling():")
            print(f"[INLINE]    • Tipo do resultado: {type(contato_result).__name__}")
            
            # Extrair ID do resultado
            contato_id_bling = None
            if isinstance(contato_result, dict):
                contato_id_bling = contato_result.get('id')
                print(f"[INLINE]    • ID do contato: {contato_id_bling}")
            
            if not contato_id_bling:
                msg = f"Falha ao criar contato no Bling: {contato_error or 'ID não retornado'}"
                print(f"[INLINE] ❌ {msg}")
                return (False, msg)
            
            print(f"[INLINE] ✅ Contato criado: ID {contato_id_bling}")
            
        except Exception as e:
            msg = f"Exception ao criar contato Bling: {type(e).__name__}: {str(e)}"
            print(f"[INLINE] ❌ {msg}")
            import traceback
            print(traceback.format_exc())
            return (False, msg)
        
        # ====================================================================
        # 4. CRIAR PEDIDO DE VENDA NO BLING - USANDO FUNÇÃO EXISTENTE
        # ====================================================================
        
        print(f"\n[INLINE] 4️⃣ Criando pedido de venda no Bling usando función existente...")
        
        # Montar itens com estrutura correta
        itens = []
        
        # Usar o mesmo mapeamento do webhook_handler
        try:
            from backend.webhook_handler import _get_codigo_bling_para_produto
        except ImportError:
            try:
                from webhook_handler import _get_codigo_bling_para_produto
            except ImportError:
                # Fallback: função simples
                def _get_codigo_bling_para_produto(nome):
                    return None
        
        for produto in produtos:
            try:
                quantidade = float(produto.get('QUANTITY', 1.0))
                preco = float(produto.get('PRICE', 0))
                product_id = produto.get('PRODUCT_ID')
                product_name = produto.get('PRODUCT_NAME', '').strip()
                
                # Garantir que sempre há descrição (obrigatório no Bling)
                if not product_name:
                    product_name = f"Produto {product_id}" if product_id else "Produto"
                
                # Procurar código correto no mapeamento
                codigo_bling, nome_exato_bling = _get_codigo_bling_para_produto(product_name)
                
                if not codigo_bling or not nome_exato_bling:
                    print(f"[INLINE]    ❌ Produto '{product_name}' não mapeado - PULANDO")
                    continue  # Pula produtos sem código válido do Bling
                
                # 🔍 BUSCAR ID DO PRODUTO NO BLING (CRÍTICO!)
                try:
                    from backend.api.index import buscar_produto_bling_por_codigo
                except:
                    try:
                        from api.index import buscar_produto_bling_por_codigo
                    except:
                        # Stub: não encontrar ID = pular produto
                        print(f"[INLINE]    ⚠️ Função de busca não disponível")
                        buscar_produto_bling_por_codigo = lambda t, c: None
                
                id_produto_bling = buscar_produto_bling_por_codigo(bling_token, codigo_bling)
                
                if not id_produto_bling:
                    print(f"[INLINE]    ❌ Produto '{codigo_bling}' não encontrado no Bling - PULANDO")
                    continue  # Pula produtos não encontrados
                
                # ✅ FORMATO CORRETO: "produto": {"id": id_interno}
                item = {
                    "produto": {"id": id_produto_bling},
                    "quantidade": quantidade,
                    "valor": float(preco),
                    "unidade": "UN",
                    "descricao": nome_exato_bling,
                    "aliquotaIPI": 0
                }
                itens.append(item)
                print(f"[INLINE]    ✅ Item adicionado: {codigo_bling} (ID: {id_produto_bling})")
                
            except (ValueError, TypeError) as e:
                print(f"[INLINE]    ⚠️ Erro ao processar produto: {e}")
                continue
        
        print(f"[INLINE]    {len(itens)} item(s) no pedido")
        
        # ====================================================================
        # ADICIONAR ITEM FIXO: FONTE 1A 12V
        # ====================================================================
        # Este item é adicionado AUTOMATICAMENTE em todo pedido com valor R$ 0
        print(f"\n[INLINE] 📦 Adicionando item fixo: FONTE 1A 12V...")
        
        try:
            # 🔍 Buscar ID da FONTE no Bling
            id_fonte_bling = buscar_produto_bling_por_codigo(bling_token, "PAV0014")
            
            if id_fonte_bling:
                # ✅ ITEM COM ID (correto)
                item_fonte = {
                    "produto": {"id": id_fonte_bling},
                    "quantidade": 1.0,
                    "valor": 0.0,
                    "unidade": "UN",
                    "descricao": "FONTE 1A 12V",
                    "aliquotaIPI": 0
                }
                itens.append(item_fonte)
                print(f"[INLINE]    ✅ FONTE adicionada COM ID: {id_fonte_bling}")
                print(f"[INLINE]    Total: {len(itens)} item(s) (incluindo FONTE)")
            else:
                print(f"[INLINE]    ❌ FONTE não encontrada no Bling (PAV0014)")
                print(f"[INLINE]    ⚠️ IGNORANDO item fixo para evitar alerta")
                print(f"[INLINE]    Total: {len(itens)} item(s) (FONTE não adicionada)")
                
        except Exception as e:
            print(f"[INLINE]    ⚠️ Erro ao adicionar FONTE: {e}")
            print(f"[INLINE]    Total: {len(itens)} item(s) (FONTE não adicionada)")
        
        try:
            # Data prevista para geração de parcelas (30 dias a partir de hoje)
            hoje = datetime.now()
            data_parcelas = (hoje + timedelta(days=30)).strftime('%Y-%m-%d')
            
            # Montar o payload (mesma estrutura usada pela função do usuário)
            pedido_payload = {
                'contato': {'id': contato_id_bling},
                'itens': itens,
                'data': hoje.strftime('%Y-%m-%d'),
                'dataPrevista': data_parcelas
                # 🧪 COMENTADO: Não preencher observações no pedido de venda
                # 'observacoes': f'Deal #{deal_id} - {deal_title}'
            }

            # Adicionar vendedor ao pedido se o ID foi resolvido via VENDEDOR_MAP
            if vendedor_id_resolvido:
                pedido_payload['vendedor'] = {'id': vendedor_id_resolvido}
                print(f"[INLINE] 📋 Vendedor adicionado ao pedido: ID {vendedor_id_resolvido}")
            else:
                print(f"[INLINE] ⚠️ Vendedor não definido - pedido criado sem vendedor")
            
            # Chamar a função existente do usuário
            if not PEDIDO_FUNCTION_AVAILABLE:
                msg = "Função criar_pedido_venda_bling não está disponível"
                print(f"[INLINE] ❌ {msg}")
                return (False, msg)
            
            sucesso, resultado, mensagem = criar_pedido_venda_bling(bling_token, pedido_payload)
            
            if not sucesso:
                msg = f"Falha ao criar pedido no Bling: {mensagem}"
                print(f"[INLINE] ❌ {msg}")
                return (False, msg)
            
            # Extrair ID do pedido do resultado
            pedido_id = None
            if isinstance(resultado, dict):
                # A API retorna { "data": { "id": ..., "numero": ... } }
                pedido_id = resultado.get('data', {}).get('id')
                if not pedido_id:
                    # Fallback: se não houver estrutura aninhada
                    pedido_id = resultado.get('id')
            
            if not pedido_id:
                msg = "Pedido criado na Bling mas ID não extraído"
                print(f"[INLINE] ❌ {msg}")
                return (False, msg)
            
            print(f"[INLINE] ✅ Pedido criado: ID {pedido_id}")
            
        except Exception as e:
            msg = f"Exception ao criar pedido Bling: {type(e).__name__}: {str(e)}"
            print(f"[INLINE] ❌ {msg}")
            import traceback
            print(traceback.format_exc())
            return (False, msg)
        
        # ====================================================================
        # 5. RESUMO FINAL E VALIDAÇÃO
        # ====================================================================
        
        print(f"\n[INLINE] {'='*70}")
        print(f"[INLINE] ✅ PROCESSAMENTO CONCLUÍDO COM SUCESSO!")
        print(f"[INLINE] {'='*70}")
        print(f"[INLINE]")
        print(f"[INLINE] 📊 RESUMO DO FLUXO BITRIX → BLING:")
        print(f"[INLINE]")
        print(f"[INLINE] 1️⃣ EXTRAÇÃO DE DADOS (Bitrix):")
        print(f"[INLINE]    • Deal ID: {deal_id}")
        print(f"[INLINE]    • Deal Title: {deal_title}")
        print(f"[INLINE]    • Responsável (ASSIGNED_BY_ID): {assigned_by_id}")
        print(f"[INLINE]    • Responsável Nome: {nome_responsavel_contato_final or 'Não extraído'}")
        print(f"[INLINE]")
        print(f"[INLINE] 2️⃣ RESOLUÇÃO DE VENDEDOR:")
        print(f"[INLINE]    • Mapeamento Direto (VENDEDOR_MAP): {'✅ Encontrado' if bitrix_assigned_id_str in VENDEDOR_MAP else '❌ Não encontrado'}")
        if bitrix_assigned_id_str in VENDEDOR_MAP:
            print(f"[INLINE]    • Bling Vendor ID (direto): {VENDEDOR_MAP.get(bitrix_assigned_id_str)}")
        else:
            print(f"[INLINE]    • Busca Dinâmica via API: {'✅ Encontrado' if vendedor_id_resolvido else '❌ Não encontrado'}")
            if vendedor_id_resolvido:
                print(f"[INLINE]    • Bling Vendor ID (API): {vendedor_id_resolvido}")
        print(f"[INLINE]")
        print(f"[INLINE] 3️⃣ CONTATO BLING:")
        print(f"[INLINE]    • ID Criado: {contato_id_bling}")
        print(f"[INLINE]    • Vendor Associado: {vendedor_id_resolvido or 'Nenhum'}")
        print(f"[INLINE]")
        print(f"[INLINE] 4️⃣ PEDIDO DE VENDA (Vendas):")
        print(f"[INLINE]    • ID Criado: {pedido_id}")
        print(f"[INLINE]    • Quantidade de Itens: {len(itens)}")
        print(f"[INLINE]    • Vendor Associado: {vendedor_id_resolvido or 'Nenhum'}")
        print(f"[INLINE]")
        print(f"[INLINE] 🔗 CONEXÃO BLING:")
        print(f"[INLINE]    • Contato → Pedido: ✅ Associado")
        print(f"[INLINE]    • Contato → Vendedor: {'✅ Sim' if vendedor_id_resolvido else '❌ Não'}")
        print(f"[INLINE]    • Pedido → Vendedor: {'✅ Sim' if vendedor_id_resolvido else '❌ Não'}")
        print(f"[INLINE]")
        print(f"[INLINE] {'='*70}")
        
        msg = f"Deal #{deal_id} processada com sucesso. Contato ID: {contato_id_bling}, Pedido ID (Vendas): {pedido_id}"
        print(f"\n[INLINE] ✅ {msg}")
        print(f"[INLINE] ===== FIM DO PROCESSAMENTO INLINE (SUCESSO) =====\n")
        
        # Marcar deal como processada para evitar duplicatas
        try:
            save_processed_deal(deal_id)
            print(f"[INLINE] 📌 Deal #{deal_id} marcada como processada")
        except Exception as e:
            print(f"[INLINE] ⚠️ Erro ao marcar deal como processada: {e}")
        
        return (True, msg)
        
    except Exception as e:
        msg = f"Erro geral no processamento inline: {type(e).__name__}: {str(e)}"
        print(f"[INLINE] ❌ {msg}")
        import traceback
        print(traceback.format_exc())
        return (False, msg)


app = Flask(__name__)
CORS(app)

# ════════════════════════════════════════════════════════════════════════════
# 🔄 INICIALIZAR AUTO-RENEWER DE TOKEN NO STARTUP
# ════════════════════════════════════════════════════════════════════════════

_auto_renewer_iniciado = False

@app.before_request
def _startup_auto_renewer():
    """Inicializa auto-renewer na primeira requisição"""
    global _auto_renewer_iniciado
    
    if not _auto_renewer_iniciado and AUTO_RENEWER_AVAILABLE:
        print(f"\n[STARTUP] 🚀 Iniciando auto-renewer de token na primeira requisição...")
        try:
            sucesso = iniciar_auto_renewer()
            if sucesso:
                _auto_renewer_iniciado = True
                print(f"[STARTUP] ✅ Auto-renewer iniciado com sucesso!")
            else:
                print(f"[STARTUP] ⚠️ Falha ao iniciar auto-renewer")
        except Exception as e:
            print(f"[STARTUP] ❌ Erro ao iniciar auto-renewer: {type(e).__name__}: {str(e)}")

@app.teardown_appcontext
def _shutdown_auto_renewer(exception=None):
    """Para o auto-renewer ao desligar a aplicação"""
    if AUTO_RENEWER_AVAILABLE:
        try:
            parar_auto_renewer()
        except Exception as e:
            print(f"[SHUTDOWN] ⚠️ Erro ao parar auto-renewer: {e}")

# ════════════════════════════════════════════════════════════════════════════
# ROTA DE DEBUG: Status do scheduler
# ════════════════════════════════════════════════════════════════════════════

@app.route("/api/scheduler-status", methods=["GET"])
def get_scheduler_status():
    """Retorna status do scheduler de renovação de token"""
    try:
        status = obter_status_scheduler()
        return jsonify({
            'status': 'ok',
            'scheduler': status,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

@app.route("/webhook-bitrix", methods=["POST"])
def handle_webhook():
    """
    Recebe e processa o webhook do Bitrix24 DE FORMA SÍNCRONA.
    Retorna debug_info na resposta JSON para visibilidade em Vercel.
    """
    debug_info = []
    
    try:
        # ====================================================================
        # 1. EXTRAIR PAYLOAD
        # ====================================================================
        
        debug_info.append("1. Extraindo payload...")
        
        data = request.get_json(silent=True)
        if data is None:
            debug_info.append("1a. JSON vazio, tentando form-data...")
            data_flat = request.form.to_dict()
            data = parse_flat_bitrix_data(data_flat)

        if not data:
            debug_info.append("1b. Payload vazio - REJEITAR")
            return jsonify({
                "error": "Empty payload", 
                "status": "rejected",
                "debug": debug_info
            }), 400
        
        debug_info.append("1c. Payload recebido OK")
        
        deal_id = data.get('data', {}).get('FIELDS', {}).get('ID', 'UNKNOWN')
        debug_info.append(f"1d. Deal ID: {deal_id}")
        
        # ====================================================================
        # 2. PROCESSAR WEBHOOK DE FORMA SÍNCRONA
        # ====================================================================
        
        debug_info.append("2. Iniciando processamento síncrono...")
        debug_info.append(f"2a. WEBHOOK_HANDLER_AVAILABLE: {WEBHOOK_HANDLER_AVAILABLE}")
        
        bitrix_url = os.getenv('BITRIX_WEBHOOK_URL')
        bling_endpoint_url = f"{request.host_url}bling/pedidos-vendas"
        
        if not bitrix_url:
            debug_info.append("2b. Erro: BITRIX_WEBHOOK_URL não configurada")
        else:
            debug_info.append(f"2b. Bitrix URL: {bitrix_url[:50]}...")
        
        debug_info.append("2c. Chamando função de processamento...")
        
        # ⚠️ IMPORTANTE: NAO usar threads! Processar sincronamente!
        try:
            if WEBHOOK_HANDLER_AVAILABLE:
                debug_info.append("2d. Usando webhook_handler.processar_webhook_bitrix()...")
                sucesso, mensagem = processar_webhook_bitrix(data, bitrix_url, bling_endpoint_url)
                debug_info.append(f"2e. Retorno webhook_handler: sucesso={sucesso}")
            else:
                debug_info.append("2d. Usando _process_webhook_inline_sync()...")
                sucesso, mensagem = _process_webhook_inline_sync(data, bitrix_url, bling_endpoint_url)
                debug_info.append(f"2e. Retorno inline_sync: sucesso={sucesso}")
            
            debug_info.append(f"2f. Mensagem: {mensagem[:100]}")
        except Exception as process_error:
            debug_info.append(f"2e. ERRO durante processamento: {str(process_error)[:100]}")
            sucesso = False
            mensagem = str(process_error)
        
        # ====================================================================
        # 3. RETORNAR RESULTADO APÓS PROCESSAMENTO COMPLETO
        # ====================================================================
        
        debug_info.append("3. Preparando resposta...")
        
        if sucesso:
            debug_info.append(f"3a. Sucesso! Mensagem: {mensagem[:50]}")
            return jsonify({
                "status": "processed",
                "success": True,
                "message": mensagem,
                "deal_id": deal_id,
                "debug": debug_info
            }), 200
        else:
            debug_info.append(f"3a. Falha! Mensagem: {mensagem[:50]}")
            return jsonify({
                "status": "processed",
                "success": False,
                "message": mensagem,
                "deal_id": deal_id,
                "debug": debug_info
            }), 200  # Retorna 200 mesmo em erro (Bitrix não precisa saber detalhes)
    
    except Exception as e:
        debug_info.append(f"ERRO INESPERADO: {type(e).__name__}: {str(e)[:100]}")
        
        return jsonify({
            "status": "error",
            "success": False,
            "message": f"Internal error: {str(e)[:100]}",
            "debug": debug_info
        }), 500


@app.route("/webhook-diagnostico", methods=["GET"])
def webhook_diagnostico():
    """
    Endpoint de diagnóstico - mostra o status das variáveis de ambiente
    Útil para debugar problemas com o processamento do webhook
    """
    diagnostico = {
        "timestamp": datetime.now().isoformat(),
        "webhook_handler_available": WEBHOOK_HANDLER_AVAILABLE,
        "env_vars": {
            "BLING_ACCESS_TOKEN": "***" if os.getenv('BLING_ACCESS_TOKEN') else "NOT SET",
            "BLING_CLIENT_ID": "***" if os.getenv('BLING_CLIENT_ID') else "NOT SET",
            "BLING_CLIENT_SECRET": "***" if os.getenv('BLING_CLIENT_SECRET') else "NOT SET",
            "BITRIX_WEBHOOK_URL": "SET" if os.getenv('BITRIX_WEBHOOK_URL') else "NOT SET",
            "QSTASH_TOKEN": "***" if os.getenv('QSTASH_TOKEN') else "NOT SET",
        },
        "imports": {
            "webhook_handler": WEBHOOK_HANDLER_AVAILABLE,
            "validacao_nomes": VALIDACAO_NOMES_AVAILABLE,
        },
        "potential_issues": []
    }
    
    # Checar problemas
    if not os.getenv('BLING_ACCESS_TOKEN'):
        diagnostico["potential_issues"].append("BLING_ACCESS_TOKEN not configured - webhook will fail!")
    
    if not os.getenv('BITRIX_WEBHOOK_URL'):
        diagnostico["potential_issues"].append("BITRIX_WEBHOOK_URL not configured")
    
    if not WEBHOOK_HANDLER_AVAILABLE:
        diagnostico["potential_issues"].append("webhook_handler module not imported - using fallback inline processing")
    
    return jsonify(diagnostico), 200


@app.route("/bling/auto-renew", methods=["GET", "POST"])
def bling_auto_renew():
    """
    🤖 RENOVAÇÃO AUTOMÁTICA DE TOKENS
    
    Endpoint que renova tokens Bling automaticamente se estiverem expirados.
    Pode ser chamado:
    - Via GET/POST direto
    - Via QStash scheduler a cada 30 minutos
    
    Retorna:
    - 200: Token renovado com sucesso ou já válido
    - 500: Erro na renovação
    """
    
    try:
        print(f"\n[AUTO-RENEW] 🤖 Verificando token de Bling...")
        
        # Carregar tokens
        tokens = load_tokens()
        
        if not tokens:
            print(f"[AUTO-RENEW] ⚠️ Nenhum token carregado")
            return jsonify({
                "status": "no_tokens",
                "message": "Nenhum token disponível para renovação",
                "timestamp": datetime.now().isoformat()
            }), 500
        
        # Verificar expiração
        if not is_token_expired(tokens):
            tempo_restante = tokens.get("expires_in", 0) - (time.time() - tokens.get("saved_at", time.time()))
            print(f"[AUTO-RENEW] ✅ Token ainda válido por {int(tempo_restante/60)} minutos")
            return jsonify({
                "status": "valid",
                "message": f"Token válido por mais {int(tempo_restante/60)} minutos",
                "expires_in_minutes": int(tempo_restante/60),
                "timestamp": datetime.now().isoformat()
            }), 200
        
        # Token expirado - renovar
        print(f"[AUTO-RENEW] 🔄 Token expirado, renovando...")
        
        new_tokens = refresh_token()
        
        if not new_tokens:
            print(f"[AUTO-RENEW] ❌ Falha ao renovar token")
            return jsonify({
                "status": "renewal_failed",
                "message": "Falha ao renovar token",
                "timestamp": datetime.now().isoformat()
            }), 500
        
        print(f"[AUTO-RENEW] ✅ Token renovado com sucesso!")
        return jsonify({
            "status": "renewed",
            "message": "Token renovado com sucesso",
            "expires_in": new_tokens.get("expires_in"),
            "expires_in_hours": new_tokens.get("expires_in", 0) / 3600,
            "next_renewal": datetime.now().isoformat(),
            "timestamp": datetime.now().isoformat()
        }), 200
        
    except Exception as e:
        print(f"[AUTO-RENEW] ❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500


CLIENT_ID = os.getenv("BLING_CLIENT_ID")
CLIENT_SECRET = os.getenv("BLING_CLIENT_SECRET")
BITRIX_WEBHOOK = os.getenv("BITRIX_WEBHOOK_URL", "").strip()

AUTH_URL = "https://www.bling.com.br/Api/v3/oauth/authorize"
TOKEN_URL = "https://www.bling.com.br/Api/v3/oauth/token"

def get_redirect_uri(request_obj=None):
    """
    Detecta a REDIRECT_URI correta baseada no ambiente:
    - Se há ngrok, usa ngrok
    - Se há request, usa o host atual
    - Senão, usa .env ou default Vercel
    """
    # Se tem ngrok configurado e está rodando localmentedef get_tokens_file_path(): 
    ngrok_url = os.getenv("NGROK_URL", "").strip()
    if ngrok_url:
        return f"{ngrok_url}/callback"
    
    # Se tem request object, usa o host atual
    if request_obj:
        return f"{request_obj.host_url.rstrip('/')}/callback"
    
    # Fallback: usar .env ou Vercel default
    return os.getenv("BLING_REDIRECT_URI", "https://backend-inky-six-95.vercel.app/callback")

def get_tokens_file_path():
    """Retorna o caminho correto do arquivo de tokens baseado no sistema"""

    # 1. Se tiver caminho definido no .env, usa ele
    env_tokens_file = os.getenv("BLING_TOKENS_FILE", "").strip()
    if env_tokens_file:
        return env_tokens_file

    # 2. Usa tokens.json dentro da pasta backend
    backend_tokens = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "tokens.json"
    )
    if os.path.exists(backend_tokens):
        return backend_tokens

    # 3. Usa tokens.json na raiz do projeto, se existir
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    project_tokens = os.path.join(project_root, "tokens.json")
    if os.path.exists(project_tokens):
        return project_tokens

    # 4. Último fallback
    return os.path.join(tempfile.gettempdir(), "tokens.json")

TOKENS_FILE = get_tokens_file_path()
print(f"📁 Arquivo de tokens: {TOKENS_FILE}")

BLING_API_BASE = "https://www.bling.com.br/Api/v3"
BLING_API_URL = "https://www.bling.com.br/Api/v3/contatos"
BLING_PROPOSTAS_URL = "https://www.bling.com.br/Api/v3/propostas-comerciais"
BLING_VENDEDORES_URL = "https://www.bling.com.br/Api/v3/vendedores"

# Cache de tokens em memória
_cached_tokens = None
_token_lock = threading.Lock()
_auto_refresh_running = False

def save_tokens(data):
    """Salva tokens no arquivo (local) ou ignora gracefully em Vercel"""
    global _cached_tokens
    data["saved_at"] = int(time.time())
    
    # Tentar salvar em arquivo (sucesso em local, pode falhar em Vercel)
    try:
        with open(TOKENS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"[TOKENS] ✅ Tokens salvos em: {TOKENS_FILE}")
    except Exception as e:
        print(f"[TOKENS] ⚠️ Não foi possível salvar em arquivo (esperado em Vercel): {e}")
        print(f"[TOKENS] ℹ️ Usando tokens do .env em Vercel")
    
    _cached_tokens = data

def load_tokens():
    """Carrega tokens do arquivo ou do .env (Vercel)"""
    global _cached_tokens
    if _cached_tokens:
        return _cached_tokens
    
    # Tentar carregar do arquivo primeiro (local)
    if os.path.exists(TOKENS_FILE):
        try:
            with open(TOKENS_FILE, "r", encoding="utf-8") as f:
                _cached_tokens = json.load(f)
                print(f"[TOKENS] ✅ Carregado do arquivo: {TOKENS_FILE}")
                return _cached_tokens
        except Exception as e:
            print(f"[TOKENS] ⚠️ Erro ao ler arquivo: {e}")
    
    # Vercel: carregar do .env (environment variables)
    access_token = os.getenv('BLING_ACCESS_TOKEN', '').strip()
    refresh_token = os.getenv('BLING_REFRESH_TOKEN', '').strip()
    
    if access_token and refresh_token:
        _cached_tokens = {
            'access_token': access_token,
            'refresh_token': refresh_token,
            'expires_in': 86400,  # Assume 24h (pode variar com Bitrix)
            'token_type': 'Bearer',
            'saved_at': int(time.time()),
            'source': 'environment_variables'
        }
        print(f"[TOKENS] ✅ Carregado do .env (Vercel)")
        return _cached_tokens
    
    print(f"[TOKENS] ❌ Nenhuma fonte de tokens disponível")
    return None

def is_token_expired(tokens):
    """Verifica se o token está expirado"""
    expires_in = tokens.get("expires_in")
    saved_at = tokens.get("saved_at")
    if not expires_in or not saved_at:
        return True
    return time.time() > saved_at + expires_in

# ============================================================================
# MAPEAMENTO: Bitrix User ID → Nome do Representante
# ============================================================================
MAPA_NOMES_REPRESENTANTES = {
    '1': 'Cleiton de Oliveira Alves',
    '46': 'Bruno Ferman Campolina Silva',
    '408': 'Rennifer Allison Ney Araújo Lima',
    '423': 'Nayara Tavares',
    '436': 'Tarik Della Santina Mohallem',
}

# MAPEAMENTO BITRIX USER ID -> BLING VENDEDOR ID
# ⚠️  CRÍTICO: DEVE SER IDÊNTICO AO VENDEDOR_MAP em backend/webhook_handler.py
# 🆕 Agora usando cache de nomes ao invés de IDs fixos para representantes não mapeados
VENDEDOR_MAP = {
    "1": 15596408666,       # Cleiton de Oliveira Alves
    "46": 15596468677,      # Bruno Ferman Campolina Silva
    "408": 15596468785,     # Rennifer/Allison Ney Araújo Lima 
    "423": 15596718349,     # Nayara Tavares
    # 436 e novos IDs: usarão NOME do cache, não ID fixo!
}

# MAPEAMENTO DE PRODUTOS BLING - ESTRUTURA COMPLETA
# Inclui todos os 15 produtos da APAE + variações
PRODUTOS_BLING_MAPPING = {
    # 1. RECARGA
    "RECARGA DE PINCEL PARA QUADRO BRANCO": {
        "codigo": "RF05RF",
        "nome_bling": "Recarga de pincel para quadro branco"
    },
    # 2. PONTEIRAS
    "PONTEIRAS PARA REPOSIÇÃO": {
        "codigo": "PAV0016", 
        "nome_bling": "PONTEIRA"
    },
    "PONTEIRA": {
        "codigo": "PAV0016",
        "nome_bling": "PONTEIRA"
    },
    # 3. MÁQUINA FILL INK INJECTOR
    "MÁQUINA FILL INK INJECTOR - 4G": {
        "codigo": "PAV0500",
        "nome_bling": "MÁQUINA FILL INK INJECTOR - 4G"
    },
    # 4. PINCÉIS FILL ECO MARKER
    "PINCÉIS FILL ECO MARKER": {
        "codigo": "PAV1001",
        "nome_bling": "PINCÉIS FILL ECO MARKER"
    },
    # 5-7. PINCÉIS FILL CHEIO (variações por cor)
    "PINCEL FILL CHEIO - AZUL": {
        "codigo": "PAV0010",
        "nome_bling": "PINCEL FILL CHEIO - AZUL"
    },
    "PINCEL FILL CHEIO - PRETO": {
        "codigo": "PAV0009",
        "nome_bling": "PINCEL FILL CHEIO - PRETO"
    },
    "PINCEL FILL CHEIO - VERMELHO": {
        "codigo": "PAV0008",
        "nome_bling": "PINCEL FILL CHEIO - VERMELHO"
    },
    # 8-10. PINCÉIS FILL VAZIO (variações por cor)
    "PINCEL FILL VAZIO - AZUL": {
        "codigo": "PAV0004",
        "nome_bling": "PINCEL FILL VAZIO - AZUL"
    },
    "PINCEL FILL VAZIO - PRETO": {
        "codigo": "PAV0002",
        "nome_bling": "PINCEL FILL VAZIO - PRETO"
    },
    "PINCEL FILL VAZIO - VERMELHO": {
        "codigo": "PAV0003",
        "nome_bling": "PINCEL FILL VAZIO - VERMELHO"
    },
    # 11. TINTA FILL MASTER COLOR
    "TINTA FILL MASTER COLOR (500ML)": {
        "codigo": "PAV1000",
        "nome_bling": "TINTA FILL MASTER COLOR 500ML"
    },
    "TINTA FILL MASTER COLOR 500ML": {
        "codigo": "PAV1000",
        "nome_bling": "TINTA FILL MASTER COLOR 500ML"
    },
    # 12-14. TINTA FILL 500ML (variações por cor)
    "TINTA FILL 500ML - AZUL": {
        "codigo": "PAV0007",
        "nome_bling": "TINTA FILL 500ML - AZUL"
    },
    "TINTA FILL 500ML - PRETA": {
        "codigo": "PAV0005",
        "nome_bling": "TINTA FILL 500ML - PRETA"
    },
    "TINTA FILL 500ML - VERMELHA": {
        "codigo": "PAV0006",
        "nome_bling": "TINTA FILL 500ML - VERMELHA"
    },
    # 15. KIT APAGADOR
    "KIT APAGADOR POR PROFESSOR": {
        "codigo": "PAV1003",
        "nome_bling": "KIT APAGADOR: Estojo Apagador Fill Master Clean 3P; Capa Protetora Fill; Feltro para estojo apagador 3un."
    },
    "KIT APAGADOR": {
        "codigo": "PAV1003",
        "nome_bling": "KIT APAGADOR: Estojo Apagador Fill Master Clean 3P; Capa Protetora Fill; Feltro para estojo apagador 3un."
    },
    # 16. FONTE 1A 12V
    "FONTE 1A 12V": {
        "codigo": "PAV0014",
        "nome_bling": "FONTE 1A 12V"
    },
    "13 FONTE 1A 12V": {
        "codigo": "PAV0014",
        "nome_bling": "FONTE 1A 12V"
    },
    "13 FONTE": {
        "codigo": "PAV0014",
        "nome_bling": "FONTE 1A 12V"
    },
}

def mapear_produto_para_codigo_bling(nome_produto):
    """
    Mapeia o nome do produto extraído para o código e nome do Bling
    Suporta busca exata, por palavras-chave e por padrões de cores
    
    ⚠️  IMPORTANTE: 
    - NUNCA usar preço como critério de identificação
    - A identificação deve ser 100% baseada no NOME do produto
    - Preço é APENAS um atributo do item, não uma chave de validação
    - RF05RF deve ser encontrado com PRICE=0 ou PRICE=10.99 igualmente
    """
    nome_normalizado = nome_produto.upper().strip()
    
    # ═══════════════════════════════════════════════════════════════════════
    # 1️⃣ BUSCA EXATA - Procura correspondência perfeita
    # ═══════════════════════════════════════════════════════════════════════
    produto_info = PRODUTOS_BLING_MAPPING.get(nome_normalizado)
    if produto_info:
        codigo = produto_info["codigo"]
        nome_bling = produto_info["nome_bling"]
        print(f"[BLING] 🎯 Produto mapeado EXATO: '{nome_produto}' → {codigo} → '{nome_bling}'")
        return {"codigo": codigo, "nome": nome_bling}
    
    # ═══════════════════════════════════════════════════════════════════════
    # 2️⃣ BUSCA INTELIGENTE POR PADRÕES - Reconhece variações
    # ═══════════════════════════════════════════════════════════════════════
    
    print(f"\n[BLING] 🔍 Busca por padrões para: '{nome_produto}'")
    
    # ─────────────────────────────────────────────────────────────────────
    # PADRÃO: RECARGA + PINCEL
    # ─────────────────────────────────────────────────────────────────────
    if "RECARGA" in nome_normalizado and "PINCEL" in nome_normalizado:
        print(f"[BLING] 📌 Padrão detectado: RECARGA + PINCEL")
        produto_info = PRODUTOS_BLING_MAPPING.get("RECARGA DE PINCEL PARA QUADRO BRANCO")
        if produto_info:
            codigo = produto_info["codigo"]
            nome_bling = produto_info["nome_bling"]
            print(f"[BLING] ✓ Mapeado por padrão RECARGA+PINCEL: {codigo} → '{nome_bling}'")
            return {"codigo": codigo, "nome": nome_bling}
    
    # ─────────────────────────────────────────────────────────────────────
    # PADRÃO: PONTEIRA
    # ─────────────────────────────────────────────────────────────────────
    if "PONTEIRA" in nome_normalizado:
        print(f"[BLING] 📌 Padrão detectado: PONTEIRA")
        produto_info = PRODUTOS_BLING_MAPPING.get("PONTEIRA")
        if produto_info:
            codigo = produto_info["codigo"]
            nome_bling = produto_info["nome_bling"]
            print(f"[BLING] ✓ Mapeado por padrão PONTEIRA: {codigo} → '{nome_bling}'")
            return {"codigo": codigo, "nome": nome_bling}
    
    # ─────────────────────────────────────────────────────────────────────
    # PADRÃO: MÁQUINA + INJECTOR
    # ─────────────────────────────────────────────────────────────────────
    if "MÁQUINA" in nome_normalizado and "INJECTOR" in nome_normalizado:
        print(f"[BLING] 📌 Padrão detectado: MÁQUINA + INJECTOR")
        produto_info = PRODUTOS_BLING_MAPPING.get("MÁQUINA FILL INK INJECTOR - 4G")
        if produto_info:
            codigo = produto_info["codigo"]
            nome_bling = produto_info["nome_bling"]
            print(f"[BLING] ✓ Mapeado por padrão MÁQUINA+INJECTOR: {codigo} → '{nome_bling}'")
            return {"codigo": codigo, "nome": nome_bling}
    
    # ─────────────────────────────────────────────────────────────────────
    # PADRÃO: ECO MARKER
    # ─────────────────────────────────────────────────────────────────────
    if "ECO" in nome_normalizado and "MARKER" in nome_normalizado:
        print(f"[BLING] 📌 Padrão detectado: ECO MARKER")
        produto_info = PRODUTOS_BLING_MAPPING.get("PINCÉIS FILL ECO MARKER")
        if produto_info:
            codigo = produto_info["codigo"]
            nome_bling = produto_info["nome_bling"]
            print(f"[BLING] ✓ Mapeado por padrão ECO+MARKER: {codigo} → '{nome_bling}'")
            return {"codigo": codigo, "nome": produto_info["nome_bling"]}
    
    # ─────────────────────────────────────────────────────────────────────
    # PADRÃO: PINCEL FILL CHEIO - Cores variadas (AZUL, PRETO, VERMELHO)
    # ─────────────────────────────────────────────────────────────────────
    if "PINCEL" in nome_normalizado and "FILL" in nome_normalizado and "CHEIO" in nome_normalizado:
        print(f"[BLING] 📌 Padrão detectado: PINCEL FILL CHEIO (com cor)")
        
        # Normalizar nomes de cores (PRETO == PRETA, VERMELHO == VERMELHA)
        nome_busca = nome_normalizado.replace(" - ", " - ").replace("PRETA", "PRETO").replace("VERMELHA", "VERMELHO")
        
        # Procurar correspondência
        for chave in PRODUTOS_BLING_MAPPING.keys():
            if "PINCEL FILL CHEIO" in chave and (
                ("AZUL" in nome_busca and "AZUL" in chave) or
                ("PRETO" in nome_busca and "PRETO" in chave) or
                ("VERMELHO" in nome_busca and "VERMELHO" in chave)
            ):
                produto_info = PRODUTOS_BLING_MAPPING[chave]
                codigo = produto_info["codigo"]
                nome_bling = produto_info["nome_bling"]
                print(f"[BLING] ✓ Mapeado por padrão PINCEL CHEIO (cor): {codigo} → '{nome_bling}'")
                return {"codigo": codigo, "nome": nome_bling}
        
        # Se não encontrou com cor, usar genérico com cor inferida
        if "AZUL" in nome_normalizado:
            produto_info = PRODUTOS_BLING_MAPPING.get("PINCEL FILL CHEIO - AZUL")
        elif "PRETO" in nome_normalizado or "PRETA" in nome_normalizado:
            produto_info = PRODUTOS_BLING_MAPPING.get("PINCEL FILL CHEIO - PRETO")
        elif "VERMELHO" in nome_normalizado or "VERMELHA" in nome_normalizado:
            produto_info = PRODUTOS_BLING_MAPPING.get("PINCEL FILL CHEIO - VERMELHO")
        
        if produto_info:
            return {"codigo": produto_info["codigo"], "nome": produto_info["nome_bling"]}
    
    # ─────────────────────────────────────────────────────────────────────
    # PADRÃO: PINCEL FILL VAZIO - Cores variadas
    # ─────────────────────────────────────────────────────────────────────
    if "PINCEL" in nome_normalizado and "FILL" in nome_normalizado and "VAZIO" in nome_normalizado:
        print(f"[BLING] 📌 Padrão detectado: PINCEL FILL VAZIO (com cor)")
        
        nome_busca = nome_normalizado.replace(" - ", " - ").replace("PRETA", "PRETO").replace("VERMELHA", "VERMELHO")
        
        for chave in PRODUTOS_BLING_MAPPING.keys():
            if "PINCEL FILL VAZIO" in chave and (
                ("AZUL" in nome_busca and "AZUL" in chave) or
                ("PRETO" in nome_busca and "PRETO" in chave) or
                ("VERMELHO" in nome_busca and "VERMELHO" in chave)
            ):
                produto_info = PRODUTOS_BLING_MAPPING[chave]
                codigo = produto_info["codigo"]
                nome_bling = produto_info["nome_bling"]
                print(f"[BLING] ✓ Mapeado por padrão PINCEL VAZIO (cor): {codigo} → '{nome_bling}'")
                return {"codigo": codigo, "nome": nome_bling}
        
        if "AZUL" in nome_normalizado:
            produto_info = PRODUTOS_BLING_MAPPING.get("PINCEL FILL VAZIO - AZUL")
        elif "PRETO" in nome_normalizado or "PRETA" in nome_normalizado:
            produto_info = PRODUTOS_BLING_MAPPING.get("PINCEL FILL VAZIO - PRETO")
        elif "VERMELHO" in nome_normalizado or "VERMELHA" in nome_normalizado:
            produto_info = PRODUTOS_BLING_MAPPING.get("PINCEL FILL VAZIO - VERMELHO")
        
        if produto_info:
            return {"codigo": produto_info["codigo"], "nome": produto_info["nome_bling"]}
    
    # ─────────────────────────────────────────────────────────────────────
    # PADRÃO: TINTA FILL MASTER COLOR
    # ─────────────────────────────────────────────────────────────────────
    if "TINTA" in nome_normalizado and "FILL" in nome_normalizado and "MASTER" in nome_normalizado:
        print(f"[BLING] 📌 Padrão detectado: TINTA FILL MASTER COLOR")
        produto_info = PRODUTOS_BLING_MAPPING.get("TINTA FILL MASTER COLOR (500ML)")
        if produto_info:
            codigo = produto_info["codigo"]
            nome_bling = produto_info["nome_bling"]
            print(f"[BLING] ✓ Mapeado por padrão TINTA MASTER: {codigo} → '{nome_bling}'")
            return {"codigo": codigo, "nome": nome_bling}
    
    # ─────────────────────────────────────────────────────────────────────
    # PADRÃO: TINTA FILL 500ML - Cores variadas
    # ─────────────────────────────────────────────────────────────────────
    if "TINTA" in nome_normalizado and "FILL" in nome_normalizado and "500ML" in nome_normalizado:
        print(f"[BLING] 📌 Padrão detectado: TINTA FILL 500ML (com cor)")
        
        # Normalizar: PRETA→PRETO, VERMELHA→VERMELHO
        nome_busca = nome_normalizado.replace("PRETA", "PRETO").replace("VERMELHA", "VERMELHO")
        
        for chave in PRODUTOS_BLING_MAPPING.keys():
            if "TINTA FILL 500ML" in chave and (
                ("AZUL" in nome_busca and "AZUL" in chave) or
                ("PRETO" in nome_busca and "PRETO" in chave) or
                ("VERMELHO" in nome_busca and "VERMELHO" in chave)
            ):
                produto_info = PRODUTOS_BLING_MAPPING[chave]
                codigo = produto_info["codigo"]
                nome_bling = produto_info["nome_bling"]
                print(f"[BLING] ✓ Mapeado por padrão TINTA 500ML (cor): {codigo} → '{nome_bling}'")
                return {"codigo": codigo, "nome": nome_bling}
        
        # Se não encontrou exato, tentar por cor
        if "AZUL" in nome_normalizado:
            produto_info = PRODUTOS_BLING_MAPPING.get("TINTA FILL 500ML - AZUL")
        elif "PRETO" in nome_normalizado or "PRETA" in nome_normalizado:
            produto_info = PRODUTOS_BLING_MAPPING.get("TINTA FILL 500ML - PRETA")
        elif "VERMELHO" in nome_normalizado or "VERMELHA" in nome_normalizado:
            produto_info = PRODUTOS_BLING_MAPPING.get("TINTA FILL 500ML - VERMELHA")
        
        if produto_info:
            return {"codigo": produto_info["codigo"], "nome": produto_info["nome_bling"]}
    
    # ─────────────────────────────────────────────────────────────────────
    # PADRÃO: KIT APAGADOR
    # ─────────────────────────────────────────────────────────────────────
    if "KIT" in nome_normalizado and "APAGADOR" in nome_normalizado:
        print(f"[BLING] 📌 Padrão detectado: KIT APAGADOR")
        produto_info = PRODUTOS_BLING_MAPPING.get("KIT APAGADOR POR PROFESSOR")
        if produto_info:
            codigo = produto_info["codigo"]
            nome_bling = produto_info["nome_bling"]
            print(f"[BLING] ✓ Mapeado por padrão KIT APAGADOR: {codigo} → '{nome_bling}'")
            return {"codigo": codigo, "nome": nome_bling}
    
    # ─────────────────────────────────────────────────────────────────────
    # PADRÃO: FONTE 1A 12V
    # ─────────────────────────────────────────────────────────────────────
    if "FONTE" in nome_normalizado and "12V" in nome_normalizado:
        print(f"[BLING] 📌 Padrão detectado: FONTE 1A 12V")
        produto_info = PRODUTOS_BLING_MAPPING.get("FONTE 1A 12V")
        if produto_info:
            codigo = produto_info["codigo"]
            nome_bling = produto_info["nome_bling"]
            print(f"[BLING] ✓ Mapeado por padrão FONTE 1A 12V: {codigo} → '{nome_bling}'")
            return {"codigo": codigo, "nome": nome_bling}
    
    # ═══════════════════════════════════════════════════════════════════════
    # 3️⃣ NENHUM PADRÃO ENCONTRADO
    # ═══════════════════════════════════════════════════════════════════════
    print(f"[BLING] ❌ ERRO: Produto NÃO MAPEADO: '{nome_produto}'")
    print(f"[BLING] 💡 Adicione este produto ao dicionário PRODUTOS_BLING_MAPPING")
    return None

def buscar_produto_bling_por_codigo(access_token, codigo_produto):
    """
    Busca produto no Bling pelo código usando a API v3 oficial.
    
    🔍 ESTRATÉGIA DE BUSCA (em ordem):
    1. Tenta com parâmetro 'pesquisa' (método comprovado funcionar)
    2. Depois com 'codigos[]' (busca por array)
    3. Tenta DOIS tipos: Produto (P) primeiro, depois Serviço (S)
    
    ⚠️  IMPORTANTE: Isso garante que produtos cadastrados incorretamente como Serviço 
    ainda sejam encontrados (ex: RF05RF pode estar como Serviço em vez de Produto)
    
    Retorna:
    - Dicionário com produto encontrado (contém 'id' + dados completos)
    - None se NÃO encontrado após TODAS as tentativas
    """
    
    # MODO FALLBACK: se token é fallback, retornar None
    if access_token == "FALLBACK_TOKEN":
        print(f"[PRODUTO-API-V3] 🔄 MODO FALLBACK ATIVO - não buscando produto")
        return None
    
    try:
        print(f"\n[PRODUTO-API-V3] === BUSCA PRODUTO: '{codigo_produto}' ===")
        
        headers = {"Authorization": f"Bearer {access_token}"}
        url = f"{BLING_API_BASE}/produtos"
        
        # ════════════════════════════════════════════════════════════════════
        # MÉTODO 1: Tentar com parâmetro 'pesquisa' (comprovado funcionar)
        # ════════════════════════════════════════════════════════════════════
        print(f"[PRODUTO-API-V3] [MÉTODO 1] Tentando com 'pesquisa={codigo_produto}'...")
        produto_encontrado = _buscar_com_pesquisa(url, headers, codigo_produto)
        if produto_encontrado:
            print(f"[PRODUTO-API-V3] ✅ ENCONTRADO COM PESQUISA! ID: {produto_encontrado.get('id')}")
            return produto_encontrado
        
        # ════════════════════════════════════════════════════════════════════
        # MÉTODO 2: Tentar com parâmetro 'codigos[]' (array)
        # ════════════════════════════════════════════════════════════════════
        print(f"[PRODUTO-API-V3] [MÉTODO 2] Tentando com 'codigos[]={codigo_produto}'...")
        produto_encontrado = _buscar_com_codigos_array(url, headers, codigo_produto)
        if produto_encontrado:
            print(f"[PRODUTO-API-V3] ✅ ENCONTRADO COM CODIGOS[]! ID: {produto_encontrado.get('id')}")
            return produto_encontrado
        
        # ════════════════════════════════════════════════════════════════════
        # MÉTODO 3: Busca parcial + listar e filtrar (último recurso)
        # ════════════════════════════════════════════════════════════════════
        print(f"[PRODUTO-API-V3] [MÉTODO 3] Tentando busca parcial + filtro...")
        produto_encontrado = _buscar_com_filtro_manual(url, headers, codigo_produto)
        if produto_encontrado:
            print(f"[PRODUTO-API-V3] ✅ ENCONTRADO COM FILTRO! ID: {produto_encontrado.get('id')}")
            return produto_encontrado
        
        # ════════════════════════════════════════════════════════════════════
        # NENHUM MÉTODO FUNCIONOU
        # ════════════════════════════════════════════════════════════════════
        print(f"[PRODUTO-API-V3] ❌ NENHUM MÉTODO ENCONTROU: '{codigo_produto}'")
        print(f"[PRODUTO-API-V3] ⚠️  CRÍTICO: Este produto NÃO pode ser enviado sem ID!")
        print(f"[PRODUTO-API-V3] ⚠️  O item será IGNORADO do pedido para evitar alerta!")
        return None
                    
                    
    except requests.exceptions.Timeout:
        print(f"[PRODUTO-API-V3] ⏱️ TIMEOUT - conexão demorou mais que 30s")
        print(f"[PRODUTO-API-V3] ❌ FALHA CRÍTICA: Não foi possível buscar o produto")
        return None
        
    except requests.exceptions.ConnectionError:
        print(f"[PRODUTO-API-V3] 🌐 ERRO DE CONEXÃO - sem acesso à internet ou API offline")
        print(f"[PRODUTO-API-V3] ❌ FALHA CRÍTICA: Não foi possível buscar o produto")
        return None
        
    except Exception as e:
        print(f"[PRODUTO-API-V3] 💥 EXCEÇÃO INESPERADA: {e}")
        import traceback
        print(f"[PRODUTO-API-V3] Traceback: {traceback.format_exc()}")
        print(f"[PRODUTO-API-V3] ❌ FALHA CRÍTICA: Não foi possível buscar o produto")
        return None


# ═══════════════════════════════════════════════════════════════════════════
# FUNÇÕES AUXILIARES DE BUSCA DE PRODUTO
# ═══════════════════════════════════════════════════════════════════════════

def _buscar_com_pesquisa(url, headers, codigo_produto):
    """Busca usando parâmetro 'pesquisa' (método comprovado)"""
    try:
        for tipo in ["P", "S"]:
            params = {
                "pesquisa": codigo_produto,
                "criterio": 2,
                "tipo": tipo,
                "limite": 20
            }
            
            tipo_str = "Produto" if tipo == "P" else "Serviço"
            print(f"[PRODUTO-API-V3]    → Buscando como {tipo_str}...")
            
            response = requests.get(url, headers=headers, params=params, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                produtos = result.get('data', [])
                
                for produto in produtos:
                    if produto.get('codigo', '').upper() == codigo_produto.upper():
                        return produto
        
        return None
    except Exception as e:
        print(f"[PRODUTO-API-V3]    Erro na busca com 'pesquisa': {e}")
        return None


def _buscar_com_codigos_array(url, headers, codigo_produto):
    """Busca usando parâmetro 'codigos[]' (array)"""
    try:
        for tipo in ["P", "S"]:
            params = {
                "codigos[]": codigo_produto,
                "criterio": 2,
                "tipo": tipo,
                "limite": 10
            }
            
            tipo_str = "Produto" if tipo == "P" else "Serviço"
            print(f"[PRODUTO-API-V3]    → Buscando como {tipo_str}...")
            
            response = requests.get(url, headers=headers, params=params, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                produtos = result.get('data', [])
                
                for produto in produtos:
                    if produto.get('codigo', '').upper() == codigo_produto.upper():
                        return produto
        
        return None
    except Exception as e:
        print(f"[PRODUTO-API-V3]    Erro na busca com 'codigos[]': {e}")
        return None


def _buscar_com_filtro_manual(url, headers, codigo_produto):
    """Busca usando filtro manual - lista produtos e filtra por código"""
    try:
        # Extrair primeira palavra ou caractere do código para busca parcial
        palavras = codigo_produto.split()
        primeiro_termo = palavras[0] if palavras else codigo_produto
        
        for tipo in ["P", "S"]:
            params = {
                "pesquisa": primeiro_termo,
                "criterio": 2,
                "tipo": tipo,
                "limite": 50
            }
            
            tipo_str = "Produto" if tipo == "P" else "Serviço"
            print(f"[PRODUTO-API-V3]    → Buscando com termo '{primeiro_termo}' como {tipo_str}...")
            
            response = requests.get(url, headers=headers, params=params, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                produtos = result.get('data', [])
                
                for produto in produtos:
                    if produto.get('codigo', '').upper() == codigo_produto.upper():
                        return produto
        
        return None
    except Exception as e:
        print(f"[PRODUTO-API-V3]    Erro na busca com filtro manual: {e}")
        return None


# FUNÇÕES AUXILIARES PARA BLING
def buscar_contato_bling_por_cnpj(access_token, cnpj_limpo):
    """Busca contato no Bling pelo CNPJ (OTIMIZADO: máximo 2 requisições, timeout reduzido)"""
    try:
        headers = {"Authorization": f"Bearer {access_token}"}
        import time
        
        # Tentativa 1: buscar por numeroDocumento
        url = f"{BLING_API_BASE}/contatos?numeroDocumento={cnpj_limpo}"
        print(f"[BLING] 🔍 Busca 1/3 - CNPJ exato: {cnpj_limpo}")
        
        inicio = time.time()
        try:
            response = requests.get(url, headers=headers, timeout=10)  # REDUZIDO: 30 → 10 segundos
            tempo = time.time() - inicio
            print(f"[BLING] ✓ Resposta em {tempo:.2f}s - HTTP {response.status_code}")
        except requests.exceptions.Timeout:
            print(f"[BLING] ⏱️ TIMEOUT na busca 1 - pulando...")
            response = None
        
        if response and response.status_code == 200:
            contatos = response.json().get('data', [])
            print(f"[BLING] ✓ Encontrados: {len(contatos)}")
            
            for contato in contatos:
                nome = contato.get('nome', 'N/A')
                id_cont = contato.get('id', 'N/A')
                
                # Rejeitar contatos problemáticos
                if any(x in nome.upper() for x in ['TARIFA', 'AVULSA', 'PIX']):
                    print(f"[BLING] 🚫 REJEITADO: {nome}")
                    continue
                
                print(f"[BLING] ✅ ACHADO por CNPJ exato: ID {id_cont} - {nome}")
                return contato
        
        # Tentativa 2: busca com pesquisa
        url2 = f"{BLING_API_BASE}/contatos?pesquisa={cnpj_limpo}"
        print(f"[BLING] 🔍 Busca 2/3 - Pesquisa: {cnpj_limpo}")
        
        inicio2 = time.time()
        try:
            response2 = requests.get(url2, headers=headers, timeout=10)
            tempo2 = time.time() - inicio2
            print(f"[BLING] ✓ Resposta em {tempo2:.2f}s - HTTP {response2.status_code}")
        except requests.exceptions.Timeout:
            print(f"[BLING] ⏱️ TIMEOUT na busca 2")
            response2 = None
        
        if response2 and response2.status_code == 200:
            contatos2 = response2.json().get('data', [])
            print(f"[BLING] ✓ Encontrados: {len(contatos2)}")
            
            for contato in contatos2:
                nome = contato.get('nome', 'N/A')
                id_cont = contato.get('id', 'N/A')
                
                if any(x in nome.upper() for x in ['TARIFA', 'AVULSA', 'PIX']):
                    continue
                
                print(f"[BLING] ✅ ACHADO por pesquisa: ID {id_cont} - {nome}")
                return contato
        
        # Tentativa 3: busca sem formatação (com hífens/espaços)
        # Tenta formatos comuns para CNPJ: XX.XXX.XXX/XXXX-XX ou XX XXX XXX XXXX XX
        cnpj_com_formatacao = f"{cnpj_limpo[:2]}.{cnpj_limpo[2:5]}.{cnpj_limpo[5:8]}/{cnpj_limpo[8:12]}-{cnpj_limpo[12:]}"
        url3 = f"{BLING_API_BASE}/contatos?pesquisa={requests.utils.quote(cnpj_com_formatacao, safe='')}"
        print(f"[BLING] 🔍 Busca 3/3 - CNPJ formatado: {cnpj_com_formatacao}")
        
        inicio3 = time.time()
        try:
            response3 = requests.get(url3, headers=headers, timeout=10)
            tempo3 = time.time() - inicio3
            print(f"[BLING] ✓ Resposta em {tempo3:.2f}s - HTTP {response3.status_code}")
        except requests.exceptions.Timeout:
            print(f"[BLING] ⏱️ TIMEOUT na busca 3")
            return None
        
        if response3.status_code == 200:
            contatos3 = response3.json().get('data', [])
            print(f"[BLING] ✓ Encontrados: {len(contatos3)}")
            
            for contato in contatos3:
                nome = contato.get('nome', 'N/A')
                id_cont = contato.get('id', 'N/A')
                
                if any(x in nome.upper() for x in ['TARIFA', 'AVULSA', 'PIX']):
                    continue
                
                print(f"[BLING] ✅ ACHADO por CNPJ formatado: ID {id_cont} - {nome}")
                return contato
        
        print(f"[BLING] ❌ CNPJ não encontrado em nenhuma busca: {cnpj_limpo}")
        return None
        
    except Exception as e:
        print(f"[BLING] 💥 Erro na busca CNPJ: {e}")
        import traceback
        traceback.print_exc()
        return None

def buscar_contato_bling_por_nome(access_token, nome):
    """Busca contato no Bling pelo nome (OTIMIZADO: máximo 2 requisições)"""
    try:
        headers = {"Authorization": f"Bearer {access_token}"}
        
        # OTIMIZAÇÃO: Apenas 2 estratégias (em vez de 5-6)
        search_terms = []
        
        # 1. Nome completo (trimmed)
        search_terms.append(nome.strip())
        
        # 2. Termo curto: primeiras 3 palavras significativas
        words = [w for w in nome.upper().split() if len(w) > 2 and w not in ('DE', 'DA', 'DO', 'DAS', 'DOS', 'E', 'EM', 'EE', 'CENTRO')]
        if len(words) >= 2:
            search_terms.append(' '.join(words[:3]))  # Primeiras 3 palavras
        
        print(f"[BLING] 🔍 BUSCA OTIMIZADA por nome: '{nome}'")
        print(f"[BLING] 💬 Estratégias (máx 2): {search_terms}")
        
        for i, search_term in enumerate(search_terms, 1):
            import time
            inicio = time.time()
            
            url = f"{BLING_API_BASE}/contatos?pesquisa={requests.utils.quote(search_term, safe='')}&limite=50"
            print(f"[BLING] 🔍 Tentativa {i}/2: '{search_term}'...")
            
            try:
                response = requests.get(url, headers=headers, timeout=10)  # REDUZIDO: 30 → 10 segundos
                tempo_decorrido = time.time() - inicio
                print(f"[BLING] ✓ Resposta em {tempo_decorrido:.2f}s - status={response.status_code}")
            except requests.exceptions.Timeout:
                print(f"[BLING] ⏱️ TIMEOUT na tentativa {i} ({search_term}) - pulando...")
                continue
            
            if response.status_code == 200:
                result = response.json()
                contatos = result.get('data', [])
                print(f"[BLING] 🔍 Contatos encontrados: {len(contatos)}")
            
                for contato in contatos:
                    nome_encontrado = contato.get('nome', 'N/A')
                    id_encontrado = contato.get('id', 'N/A')
                    
                    if "TARIFA" in nome_encontrado.upper() or "AVULSA" in nome_encontrado.upper() or "PIX" in nome_encontrado.upper():
                        print(f"[BLING] 🚨 REJEITANDO contato problemático: ID {id_encontrado} - {nome_encontrado}")
                        continue
                    
                    # ESTRATÉGIA APRIMORADA de matching para nomes truncados (DENTRO do loop)
                    nome_busca_norm = nome.upper().strip().replace('CENTRO DE EDUCAÇÃO ESPECIAL', 'CENTRO EDUCACAO ESPECIAL')
                    nome_encontrado_norm = nome_encontrado.upper().strip().replace('CENTRO DE EDUCAÇÃO ESPECIAL', 'CENTRO EDUCACAO ESPECIAL')
                    
                    # 1. Match EXATO (após normalização)
                    if nome_busca_norm == nome_encontrado_norm:
                        print(f"[BLING] ✅ MATCH EXATO: ID {id_encontrado} - {nome_encontrado}")
                        return contato
                    
                    # 2. Match por INÍCIO (nome pode estar truncado com 'M' no final)
                    if (nome_busca_norm.startswith(nome_encontrado_norm) or 
                        nome_encontrado_norm.startswith(nome_busca_norm) or
                        (nome_encontrado.endswith(' M') and nome_busca_norm.startswith(nome_encontrado_norm.rstrip(' M')))):
                        print(f"[BLING] ✅ MATCH POR PREFIXO/TRUNCADO: ID {id_encontrado} - {nome_encontrado}")
                        print(f"[BLING] 🔍 Nome buscado: '{nome_busca_norm}'")
                        print(f"[BLING] 🔍 Nome encontrado: '{nome_encontrado_norm}'")
                        return contato
                    
                    # 3. Match por PALAVRAS-CHAVE (70% similaridade)
                    palavras_busca = set(p for p in nome_busca_norm.split() if len(p) > 2)
                    palavras_encontrado = set(p for p in nome_encontrado_norm.split() if len(p) > 2)
                    
                    if palavras_busca and palavras_encontrado:
                        palavras_comuns = len(palavras_busca & palavras_encontrado)
                        total_palavras = max(len(palavras_busca), len(palavras_encontrado))
                        score = palavras_comuns / total_palavras if total_palavras > 0 else 0
                        
                        if score >= 0.7:  # 70% de similaridade
                            print(f"[BLING] ✅ MATCH POR SIMILARIDADE ({score:.1%}): ID {id_encontrado} - {nome_encontrado}")
                            return contato
                        else:
                            print(f"[BLING] ➡️ Similaridade baixa ({score:.1%}): {nome_encontrado}")
                
                # Se encontrou contatos mas nenhum match, continuar próxima estratégia
                if contatos:
                    print(f"[BLING] ➡️ {len(contatos)} contatos encontrados mas nenhum match suficiente")
            else:
                print(f"[BLING] ❌ Erro na busca tentativa {i}/2: {response.status_code}")
        
        print(f"[BLING] ❌ Contato NÃO encontrado após 2 tentativas: {nome}")
        return None
    except Exception as e:
        print(f"[BLING] 💥 Erro ao buscar contato por nome: {e}")
        return None


# ==================== FUNÇÃO AUXILIAR: EXTRAIR UF DO CEP ====================
def extrair_uf_do_cep(cep: str) -> str:
    """
    Extrai o UF/Estado de um CEP usando a API viacep.com.br
    
    Args:
        cep: CEP no formato "12345-678" ou "12345678"
    
    Returns:
        Sigla de 2 letras (MG, SP, RJ, etc) ou 'MG' como padrão
    
    Exemplos:
        extrair_uf_do_cep('30670-565')  # Retorna: 'MG'
        extrair_uf_do_cep('01310100')   # Retorna: 'SP'
    """
    try:
        # Limpar CEP: remover formatação
        cep_limpo = ''.join(c for c in str(cep) if c.isdigit())
        
        # Validar: CEP deve ter 8 dígitos
        if not cep_limpo or len(cep_limpo) != 8:
            print(f"[VIACEP] ⚠️  CEP inválido para busca: {repr(cep)} (limpou em {repr(cep_limpo)})")
            return 'MG'  # Padrão
        
        print(f"[VIACEP] 🔍 Buscando UF para CEP: {cep_limpo}")
        
        # Chamar API viacep
        response = requests.get(
            f"https://viacep.com.br/ws/{cep_limpo}/json/",
            timeout=5
        )
        
        if response.status_code == 200:
            dados = response.json()
            
            # Verificar se retornou erro (CEP não encontrado)
            if dados.get('erro'):
                print(f"[VIACEP] ⚠️  CEP não encontrado na base viacep: {cep_limpo}")
                return 'MG'
            
            uf = dados.get('uf', '').upper()
            cidade = dados.get('localidade', 'Desconhecida')
            
            if uf and len(uf) == 2:
                print(f"[VIACEP] ✅ CEP {cep_limpo} → UF: {uf} (Cidade: {cidade})")
                return uf
            else:
                print(f"[VIACEP] ⚠️  UF inválido retornado: {repr(uf)}")
                return 'MG'
        else:
            print(f"[VIACEP] ⚠️  API retornou status {response.status_code}")
            return 'MG'
    
    except requests.exceptions.Timeout:
        print(f"[VIACEP] ⏱️  TIMEOUT ao chamar API viacep (CEP: {cep})")
        return 'MG'
    except requests.exceptions.ConnectionError as e:
        print(f"[VIACEP] 🌐 Erro de conexão: {e}")
        return 'MG'
    except Exception as e:
        print(f"[VIACEP] 💥 Erro ao extrair UF do CEP {cep}: {e}")
        return 'MG'


# ═══════════════════════════════════════════════════════════════════════════
# 🎯 FUNÇÕES DE FUZZY MATCHING - ENCONTRAR VENDEDOR MAIS SIMILAR
# ═══════════════════════════════════════════════════════════════════════════

def buscar_todos_vendedores_bling(access_token):
    """
    Busca TODOS os vendedores cadastrados no Bling via API v3.
    
    IMPORTANTE: O nome do vendedor está em 'contato.nome', não em um campo direto!
    
    Retorna:
        Lista de dicionários com {id, nome} ou None em caso de erro
    
    Exemplo de retorno:
        [
            {"id": 15596408666, "nome": "Cleiton de Oliveira Alves"},
            {"id": 15596468677, "nome": "Bruno Ferman Campolina Silva"},
            {"id": 15596468785, "nome": "Allison Ney Araújo Lima"},
            {"id": 15596718349, "nome": "Nayara Tavares"},
        ]
    """
    try:
        print(f"\n[VENDEDOR-FUZZY] 🔍 Buscando TODOS os vendedores do Bling...")
        
        headers = {"Authorization": f"Bearer {access_token}"}
        url = f"{BLING_API_BASE}/vendedores"
        
        # Parâmetros para buscar TODOS (limite máximo permitido)
        params = {"limite": 100, "pagina": 1}
        
        print(f"[VENDEDOR-FUZZY] 🌐 URL: {url}")
        print(f"[VENDEDOR-FUZZY] 📋 Parâmetros: {params}")
        
        response = requests.get(url, headers=headers, params=params, timeout=15)
        
        print(f"[VENDEDOR-FUZZY] 📥 Status: {response.status_code}")
        
        if response.status_code != 200:
            print(f"[VENDEDOR-FUZZY] ❌ Erro HTTP {response.status_code}")
            print(f"[VENDEDOR-FUZZY]    Resposta: {response.text[:200]}")
            return None
        
        data = response.json()
        vendedores_raw = data.get('data', [])
        
        # ═══════════════════════════════════════════════════════════════════
        # PROCESSAR: Extrair nome de contato.nome e montar lista final
        # ═══════════════════════════════════════════════════════════════════
        vendedores = []
        for vendor_raw in vendedores_raw:
            vendor_id = vendor_raw.get('id')
            # ✅ IMPORTANTE: Nome está em contato.nome
            vendor_nome = vendor_raw.get('contato', {}).get('nome', 'SEM NOME')
            
            vendedores.append({
                'id': vendor_id,
                'nome': vendor_nome
            })
        
        print(f"[VENDEDOR-FUZZY] ✅ Total de vendedores processados: {len(vendedores)}")
        
        if vendedores:
            print(f"[VENDEDOR-FUZZY] 📍 Listando vendedores:")
            for idx, vendedor in enumerate(vendedores, 1):
                print(f"[VENDEDOR-FUZZY]    {idx}. ID: {vendedor['id']:15} | Nome: '{vendedor['nome']}'")
        
        return vendedores
        
    except requests.exceptions.Timeout:
        print(f"[VENDEDOR-FUZZY] ⏱️ TIMEOUT ao buscar vendedores")
        return None
    except requests.exceptions.ConnectionError as e:
        print(f"[VENDEDOR-FUZZY] 🌐 Erro de conexão: {e}")
        return None
    except Exception as e:
        print(f"[VENDEDOR-FUZZY] 💥 Exceção: {e}")
        traceback.print_exc()
        return None


def encontrar_vendedor_por_nome(nome_responsavel_bitrix, vendedores_bling):
    """
    Faz FUZZY MATCHING entre o nome do responsável do Bitrix e os vendedores do Bling.
    
    ESTRATÉGIA:
    1. Busca correspondência exata (ignorando capitalização)
    2. Se não encontrar exato, usa difflib.get_close_matches() para buscar similar
    3. Retorna o vendedor que mais se parece
    
    Args:
        nome_responsavel_bitrix: String com o nome do Bitrix (ex: "Nayara Tavares")
        vendedores_bling: Lista de dicts com {id, nome}
    
    Retorna:
        Dict {id, nome} do vendedor encontrado ou None
    
    Exemplos:
        encontrar_vendedor_por_nome("Nayara", [{"id": 123, "nome": "Nayara Tavares"}])
        -> {"id": 123, "nome": "Nayara Tavares"} ✅
        
        encontrar_vendedor_por_nome("Rennifer", [{"id": 456, "nome": "Allison Ney"}])
        -> {"id": 456, "nome": "Allison Ney"} ✅ (fuzzy match)
    """
    
    if not nome_responsavel_bitrix or not vendedores_bling:
        print(f"[VENDEDOR-FUZZY] ⚠️ Dados inválidos para fuzzy match")
        return None
    
    print(f"\n[VENDEDOR-FUZZY] 🎯 INICIANDO FUZZY MATCH")
    print(f"[VENDEDOR-FUZZY]    Nome do responsável (Bitrix): '{nome_responsavel_bitrix}'")
    print(f"[VENDEDOR-FUZZY]    Total de vendedores para comparar: {len(vendedores_bling)}")
    
    nome_normalizado = nome_responsavel_bitrix.strip().upper()
    
    # ════════════════════════════════════════════════════════════════════════
    # PASSO 1: BUSCA EXATA (sem case-sensitivity)
    # ════════════════════════════════════════════════════════════════════════
    print(f"\n[VENDEDOR-FUZZY] 1️⃣ PASSO 1: Buscando correspondência EXATA...")
    
    for vendedor in vendedores_bling:
        vendedor_nome = vendedor.get('nome', '').upper()
        vendedor_id = vendedor.get('id')
        
        # Comparação exata
        if vendedor_nome == nome_normalizado:
            print(f"[VENDEDOR-FUZZY] ✅ MATCH EXATO! '{nome_responsavel_bitrix}' = '{vendedor.get('nome')}'")
            print(f"[VENDEDOR-FUZZY]    Vendedor ID: {vendedor_id}")
            return vendedor
        
        # Comparação parcial (verificar se o nome do Bitrix está CONTIDO no nome do Bling)
        if nome_normalizado in vendedor_nome or vendedor_nome in nome_normalizado:
            print(f"[VENDEDOR-FUZZY] ✅ MATCH PARCIAL! '{nome_responsavel_bitrix}' contém/contido em '{vendedor.get('nome')}'")
            print(f"[VENDEDOR-FUZZY]    Vendedor ID: {vendedor_id}")
            return vendedor
    
    # ════════════════════════════════════════════════════════════════════════
    # PASSO 2: FUZZY MATCHING (SequenceMatcher)
    # ════════════════════════════════════════════════════════════════════════
    print(f"\n[VENDEDOR-FUZZY] 2️⃣ PASSO 2: Buscando correspondência SIMILAR (fuzzy)...")
    print(f"[VENDEDOR-FUZZY]    Usando difflib.SequenceMatcher para calcular similaridade...")
    
    melhor_match = None
    melhor_score = 0
    
    for vendedor in vendedores_bling:
        vendedor_nome = vendedor.get('nome', '')
        
        # Calcular score de similaridade
        ratio = SequenceMatcher(None, nome_normalizado, vendedor_nome.upper()).ratio()
        
        print(f"[VENDEDOR-FUZZY]    • '{nome_responsavel_bitrix}' vs '{vendedor_nome}' = {ratio*100:.1f}%")
        
        if ratio > melhor_score:
            melhor_score = ratio
            melhor_match = vendedor
    
    if melhor_match and melhor_score >= 0.6:  # Threshold de 60%
        print(f"\n[VENDEDOR-FUZZY] ✅ FUZZY MATCH ENCONTRADO (score: {melhor_score*100:.1f}%)")
        print(f"[VENDEDOR-FUZZY]    Melhor correspondência: '{melhor_match.get('nome')}'")
        print(f"[VENDEDOR-FUZZY]    Vendedor ID: {melhor_match.get('id')}")
        return melhor_match
    
    # ════════════════════════════════════════════════════════════════════════
    # PASSO 3: NENHUM MATCH ENCONTRADO
    # ════════════════════════════════════════════════════════════════════════
    print(f"\n[VENDEDOR-FUZZY] ❌ NENHUM MATCH ENCONTRADO!")
    print(f"[VENDEDOR-FUZZY]    Melhor score obtido: {melhor_score*100:.1f}% (threshold: 60%)")
    print(f"[VENDEDOR-FUZZY]    Será necessário usar um vendedor padrão ou skippear")
    return None


def resolver_vendedor_por_nome_dinamico(access_token, nome_responsavel_bitrix):
    """
    FUNÇÃO PRINCIPAL: Resolve o vendedor do Bling buscando dinamicamente por NOME.
    
    FLUXO:
    1. Busca TODOS os vendedores do Bling via API
    2. Faz fuzzy matching do nome do responsável do Bitrix
    3. Retorna o vendedor encontrado com ID e NOME
    
    Args:
        access_token: Token de autenticação do Bling
        nome_responsavel_bitrix: Nome do responsável do Bitrix (ex: "Nayara Tavares")
    
    Retorna:
        Dict {id, nome} ou None se não encontrado
    
    Exemplo:
        resultado = resolver_vendedor_por_nome_dinamico(token, "Nayara")
        # {"id": 15596718349, "nome": "Nayara Tavares"}
    """
    
    print(f"\n{'='*70}")
    print(f"[VENDEDOR-RESOLVER] === RESOLVER DINÂMICO DE VENDEDOR ===")
    print(f"{'='*70}")
    print(f"[VENDEDOR-RESOLVER] 👤 Nome do responsável (Bitrix): '{nome_responsavel_bitrix}'")
    
    # PASSO 1: Buscar todos os vendedores do Bling
    vendedores_bling = buscar_todos_vendedores_bling(access_token)
    
    if not vendedores_bling:
        print(f"[VENDEDOR-RESOLVER] ❌ Não foi possível buscar vendedores do Bling")
        return None
    
    # PASSO 2: Fazer fuzzy matching
    vendedor_encontrado = encontrar_vendedor_por_nome(nome_responsavel_bitrix, vendedores_bling)
    
    if vendedor_encontrado:
        print(f"\n[VENDEDOR-RESOLVER] ✅ SUCESSO! Vendedor encontrado:")
        print(f"[VENDEDOR-RESOLVER]    ID: {vendedor_encontrado.get('id')}")
        print(f"[VENDEDOR-RESOLVER]    Nome: '{vendedor_encontrado.get('nome')}'")
        return vendedor_encontrado
    else:
        print(f"\n[VENDEDOR-RESOLVER] ❌ Vendedor NÃO encontrado após fuzzy matching")
        return None
def criar_contato_bling(access_token, empresa_data, vendedor_nome=None, vendedor_id=None, forcar_juridico=False, deal_title=None):
    """
    Cria novo contato no Bling baseado nos dados da empresa do Bitrix
    Segue a estrutura da API v3 do Bling: POST /contatos
    
    Args:
        deal_title: Título da deal do Bitrix (será usado como fantasia no Bling)
    """
    print(f"\n[BLING CONTATO] === INICIANDO CRIAÇÃO DE CONTATO ===")
    print(f"[BLING CONTATO] 🔍 vendedor_nome recebido: {repr(vendedor_nome)}")
    print(f"[BLING CONTATO] 🔍 vendedor_id recebido: {repr(vendedor_id)}")
    print(f"[BLING CONTATO] 🔍 deal_title recebido: {repr(deal_title)}")
    
    responsavel_rep = empresa_data.get('responsavel_representante', None)
    print(f"[BLING CONTATO] 👤 responsavel_representante em empresa_data: {repr(responsavel_rep)}")
    
    if responsavel_rep:
        print(f"[BLING CONTATO] ✅ MODO CONCLUÍDO - Responsável ENCONTRADO: '{responsavel_rep}'")
    else:
        print(f"[BLING CONTATO] ⚠️  Responsável NÃO foi encontrado na deal")
    
    print(f"[BLING CONTATO]")
    
    # 🔥 DEBUG MASSIVO: Dump de todas as chaves em empresa_data
    print(f"\n[BLING CONTATO] 📋 === DUMP COMPLETO DE empresa_data ===")
    print(f"[BLING CONTATO] Total de chaves: {len(empresa_data)}")
    if empresa_data:
        for key in sorted(empresa_data.keys()):
            valor = empresa_data[key]
            # Limitar exibição de valores muito longos
            if isinstance(valor, str) and len(str(valor)) > 100:
                valor_display = str(valor)[:100] + "..."
            else:
                valor_display = repr(valor)
            print(f"[BLING CONTATO]   {key}: {valor_display}")
    print(f"[BLING CONTATO] === FIM DO DUMP ===\n")
    
    try:
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        # Extrair dados da empresa do Bitrix
        nome = empresa_data.get('TITLE', 'Cliente sem nome')
        
        # === EXTRAÇÃO DO CNPJ COM MÚLTIPLOS FALLBACKS ===
        # Tenta múltiplos campos do Bitrix para CNPJ
        cnpj = None
        campos_cnpj = [
            'UF_CRM_1713291425',  # Campo correto identificado no debug ✅
            'UF_CRM_1713291425',  # Campo correto do CNPJ
            'LATIN_NAME',  # Às vezes o CNPJ fica aqui
            'INN',  # Código fiscal alternativo
            'REG_NUMBER'  # Registro alternativo
        ]
        
        for campo in campos_cnpj:
            valor = empresa_data.get(campo)
            if valor and valor != 'None':  # Verifica se tem valor real
                cnpj = str(valor).strip()
                if cnpj:
                    print(f"[BLING] 🔍 CNPJ encontrado em '{campo}': {repr(cnpj)}")
                    break
        
        # Garantir que cnpj é sempre string (não None)
        if cnpj is None or cnpj == 'None':
            cnpj = ''
        
        # === LOG DETALHADO DA EXTRAÇÃO DO CNPJ ===
        print(f"\n[BLING] === EXTRAÇÃO DO CNPJ ===")
        print(f"[BLING] CNPJ bruto: {repr(cnpj)}")
        print(f"[BLING] Tipo: {type(cnpj).__name__}")
        print(f"[BLING] Comprimento antes da limpeza: {len(str(cnpj))}")
        
        # === EXTRAÇÃO E LIMPEZA DE TELEFONE ===
        # ✅ NOVO: Remover '+55', parênteses, hífen, espaço, etc.
        telefone_bruto = empresa_data.get('PHONE', [{}])[0].get('VALUE', '') if isinstance(empresa_data.get('PHONE'), list) else ''
        def limpar_telefone(telefone_str):
            """Remove formatação: +55, (), -, espaços, etc. Retorna apenas dígitos"""
            if not telefone_str:
                return ''
            # Remove +55 (código país)
            telefone_str = str(telefone_str).replace('+55', '').replace('+', '')
            # Remove caracteres especiais: ( ) - espaço
            import re
            telefone_limpo = re.sub(r'[\s\-\(\)\.]+', '', telefone_str)
            # Mantém apenas dígitos
            telefone_limpo = ''.join(c for c in telefone_limpo if c.isdigit())
            return telefone_limpo
        
        telefone = limpar_telefone(telefone_bruto)
        print(f"[BLING] 📞 Telefone: bruto='{telefone_bruto}' → limpo='{telefone}'")
        
        celular_bruto = empresa_data.get('UF_CRM_CELULAR', '') or ''
        celular = limpar_telefone(celular_bruto)
        print(f"[BLING] 📱 Celular: bruto='{celular_bruto}' → limpo='{celular}'")
        
        email = empresa_data.get('EMAIL', [{}])[0].get('VALUE', '') if isinstance(empresa_data.get('EMAIL'), list) else ''
        
        # === BUSCA DA RAZÃO SOCIAL (para swapear com TITLE) ===
        # Prioridade: UF_CRM_1724855055 > UF_CRM_FANTASIA > LATIN_NAME > Nenhum
        razao_social = ''
        campos_razao = [
            'UF_CRM_1724855055',  # Campo correto identificado no debug
            'UF_CRM_FANTASIA',    # Campo alternativo (fantasia/razão social)
            'LATIN_NAME',         # Fallback - às vezes fica aqui
        ]
        
        print(f"\n[BLING] 🔍 === BUSCA DE RAZÃO SOCIAL ===")
        print(f"[BLING] Nome da empresa (TITLE): '{nome}'")
        
        for campo in campos_razao:
            valor = empresa_data.get(campo, '').strip() if empresa_data.get(campo) else ''
            print(f"[BLING]   Tentando {campo}: '{valor}'")
            if valor and valor.lower() not in ['none', '']:
                razao_social = valor
                print(f"[BLING] ✅ Razão social encontrada em '{campo}': '{razao_social}'")
                break
        
        if not razao_social:
            print(f"[BLING] ⚠️ Razão social não encontrada - será usado TITLE como fallback")
        
        # No contato Bling:
        # - "nome" deve ter a razão social (ou TITLE se não tiver razão social)
        # - "fantasia" deve ter o nome da deal (title da transação) ou TITLE se não tiver
        nome_para_bling = razao_social or nome  # Razão social ou TITLE
        fantasia_para_bling = (deal_title or nome)  # Nome da deal ou TITLE da empresa
        
        # Variáveis de compatibilidade (já usadas em outras funções)
        fantasia = razao_social or nome  # Por compatibilidade com código existente
        nome_fantasia = razao_social or nome  # Para usar em "nome" do contato
        
        print(f"[BLING] 📋 MAPEAMENTO FINAL:")
        print(f"[BLING]   payload['nome'] = '{nome_para_bling}'")
        print(f"[BLING]   payload['fantasia'] = '{fantasia_para_bling}' (source: deal_title='{deal_title}', nome='{nome}')")
        
        # === MAPEAMENTO CORRETO DE ENDEREÇO BASEADO NOS UF_CRM REAIS ===
        # Campos identificados pelo usuário no HTML do Bitrix:
        # NOTA: Campos podem vir como None do Bitrix, usar 'or ""' para garantir string
        endereco_rua = (empresa_data.get('UF_CRM_1721160042326') or '')      # Logradouro (Físico)
        endereco_numero = (empresa_data.get('UF_CRM_1721160053841') or '')    # Número (Físico)
        endereco_bairro = (empresa_data.get('UF_CRM_1721160072753') or '')    # Bairro (Físico)  
        endereco_cidade = (empresa_data.get('UF_CRM_1721160090521') or '')    # Cidade (Físico)
        
        # === ESTADO (UF) COM MÚLTIPLOS CAMPOS DE FALLBACK ===
        # Tenta múltiplos campos para Estado/UF com nomes descritivos
        campos_uf = [
            ('UF_CRM_1721160100959', 'UF (ID Real Bitrix - Físico)'),  # Campo correto do UF (Físico)
            ('UF_CRM_ESTADO', 'Estado (alt1)'),                        # Campo alternativo 1 (mais confiável)
            ('ESTADO', 'Estado (genérico)'),                          # Campo genérico
            ('UF', 'UF (direto)'),                                    # Campo direto UF
        ]
        
        endereco_uf = ''  # Começa vazio
        campo_uf_usado = None
        campo_uf_descricao = None
        
        # Itera pelos campos até achar um preenchido
        for campo_id, campo_descricao in campos_uf:
            valor = empresa_data.get(campo_id)
            if valor:  # Se encontrar um valor preenchido
                endereco_uf = valor
                campo_uf_usado = campo_id
                campo_uf_descricao = campo_descricao
                print(f"[DEBUG] 🗺️ Estado/UF encontrado em '{campo_descricao}' [{campo_id}]: {repr(valor)}")
                break
        
        if not endereco_uf:
            campos_consultados = [f"{desc} [{id}]" for id, desc in campos_uf]
            print(f"[DEBUG] ⚠️ Estado/UF não encontrado em campos diretos. Tentando estratégia inteligente...")
            # ✅ ESTRATÉGIA INTELIGENTE: Usar função que tenta traduzir ID ou extrair do CEP
            endereco_uf = _extrair_uf_do_bitrix(empresa_data)
            campo_uf_usado = 'ESTRATÉGIA_INTELIGENTE'
            campo_uf_descricao = 'ID traduzido ou extraído de CEP'
            if endereco_uf:
                print(f"[DEBUG] 🗺️ UF obtido pela estratégia inteligente: {repr(endereco_uf)}")
            else:
                print(f"[DEBUG] ⚠️ Estratégia inteligente também falhou, UF permanece vazio")

        
        # === CEP COM MÚLTIPLOS CAMPOS DE FALLBACK ===
        # 🔧 ALTERAÇÃO: Campo correto do Bitrix PRIMEIRO!
        # Tenta múltiplos campos de CEP para cobrir diferentes configurações de Bitrix
        # Usa tuplas (id_campo, nome_descritivo) para mostrar qual campo consultou
        campos_cep = [
            ('UF_CRM_1721160082099', 'CEP (ID Real Bitrix)'),  # ← CAMPO CORRETO DO BITRIX!
            ('CEP_FISICO', 'CEP Físico'),                      # Campo 1
            ('UF_CRM_1725646763', 'CEP (Físico ID)'),          # Campo 2 - ID bruto
            ('UF_CRM_CEP', 'CEP (Bitrix)'),                    # Campo 3 - alternativa
            ('CEP', 'CEP (Genérico)'),                         # Campo 4 - nome genérico
        ]
        
        endereco_cep = ''  # Começa vazio
        campo_cep_usado = None
        campo_cep_descricao = None
        
        # Itera pelos campos até achar um preenchido
        for campo_id, campo_descricao in campos_cep:
            valor = empresa_data.get(campo_id)
            # ✅ Validar se tem conteúdo REAL (não apenas espaços em branco)
            valor_limpo = str(valor).strip() if valor else ''
            if valor_limpo:  # Se encontrar um valor preenchido
                endereco_cep = valor
                campo_cep_usado = campo_id
                campo_cep_descricao = campo_descricao
                print(f"[DEBUG] 📬 CEP encontrado em '{campo_descricao}' [{campo_id}]: {repr(valor)}")
                print(f"[DEBUG]    Valor após .strip(): '{valor_limpo}' (comprimento: {len(valor_limpo)})")
                break
        
        if not endereco_cep:
            campos_consultados = [f"{desc} [{id}]" for id, desc in campos_cep]
            print(f"[DEBUG] ⚠️ CEP não encontrado. Campos consultados: {', '.join(campos_consultados)}")
        
        endereco_complemento = (empresa_data.get('UF_CRM_COMPLEMENTO') or '')  # Complemento (genérico)
        
        # === LOGS DETALHADOS - VALORES BRUTOS DO BITRIX ===
        print(f"\n[DEBUG] === VALORES BRUTOS (ANTES DE PROCESSAR) ===")
        print(f"[DEBUG] empresa_data.get('UF_CRM_1721160042326'): {repr(empresa_data.get('UF_CRM_1721160042326'))} (tipo: {type(empresa_data.get('UF_CRM_1721160042326')).__name__})")
        print(f"[DEBUG] empresa_data.get('UF_CRM_1721160053841'): {repr(empresa_data.get('UF_CRM_1721160053841'))} (tipo: {type(empresa_data.get('UF_CRM_1721160053841')).__name__})")
        print(f"[DEBUG] empresa_data.get('UF_CRM_1721160072753'): {repr(empresa_data.get('UF_CRM_1721160072753'))} (tipo: {type(empresa_data.get('UF_CRM_1721160072753')).__name__})")
        print(f"[DEBUG] empresa_data.get('UF_CRM_1721160090521'): {repr(empresa_data.get('UF_CRM_1721160090521'))} (tipo: {type(empresa_data.get('UF_CRM_1721160090521')).__name__})")
        if campo_uf_usado:
            print(f"[DEBUG] Estado/UF encontrado em '{campo_uf_descricao}' [{campo_uf_usado}]: {repr(empresa_data.get(campo_uf_usado))} (tipo: {type(empresa_data.get(campo_uf_usado)).__name__})")
        else:
            print(f"[DEBUG] Estado/UF: não encontrado em nenhum campo")
        if campo_cep_usado:
            print(f"[DEBUG] CEP encontrado em '{campo_cep_descricao}' [{campo_cep_usado}]: {repr(empresa_data.get(campo_cep_usado))} (tipo: {type(empresa_data.get(campo_cep_usado)).__name__})")
        else:
            print(f"[DEBUG] CEP: não encontrado em nenhum campo")
        print(f"[DEBUG] empresa_data.get('UF_CRM_COMPLEMENTO'): {repr(empresa_data.get('UF_CRM_COMPLEMENTO'))} (tipo: {type(empresa_data.get('UF_CRM_COMPLEMENTO')).__name__})")
        
        # === LOGS DETALHADOS - APÓS CONVERSÃO PRA STRING ===
        print(f"\n[DEBUG] === VALORES APÓS GARANTIR STRING (ANTES DE .strip()) ===")
        print(f"[DEBUG] endereco_rua: '{endereco_rua}' (tipo: {type(endereco_rua).__name__})")
        print(f"[DEBUG] endereco_numero: '{endereco_numero}' (tipo: {type(endereco_numero).__name__})")
        print(f"[DEBUG] endereco_bairro: '{endereco_bairro}' (tipo: {type(endereco_bairro).__name__})")
        print(f"[DEBUG] endereco_cidade: '{endereco_cidade}' (tipo: {type(endereco_cidade).__name__})")
        print(f"[DEBUG] endereco_uf: '{endereco_uf}' (tipo: {type(endereco_uf).__name__})")
        print(f"[DEBUG] endereco_cep: '{endereco_cep}' (tipo: {type(endereco_cep).__name__})")
        print(f"[DEBUG] endereco_complemento: '{endereco_complemento}' (tipo: {type(endereco_complemento).__name__})")
        
        # === LOGS DETALHADOS DOS VALORES BRUTOS DO BITRIX ===
        print(f"\n[DEBUG] === MAPEAMENTO DE ENDEREÇO BITRIX → BLING (CAMPOS CORRETOS) ===")
        print(f"[DEBUG] Endereço Bitrix - rua (UF_CRM_1721160042326): '{endereco_rua}'")
        print(f"[DEBUG] Endereço Bitrix - numero (UF_CRM_1721160053841): '{endereco_numero}'")
        print(f"[DEBUG] Endereço Bitrix - bairro (UF_CRM_1721160072753): '{endereco_bairro}'")
        print(f"[DEBUG] Endereço Bitrix - cidade (UF_CRM_1721160090521): '{endereco_cidade}'")
        if campo_uf_descricao:
            print(f"[DEBUG] Endereço Bitrix - Estado/UF ('{campo_uf_descricao}' [{campo_uf_usado}]): '{endereco_uf}'")
        else:
            print(f"[DEBUG] Endereço Bitrix - Estado/UF: não encontrado")
        if campo_cep_descricao:
            print(f"[DEBUG] Endereço Bitrix - CEP ('{campo_cep_descricao}' [{campo_cep_usado}]): '{endereco_cep}'")
        else:
            print(f"[DEBUG] Endereço Bitrix - CEP: não encontrado")
        print(f"[DEBUG] Endereço Bitrix - complemento (UF_CRM_COMPLEMENTO): '{endereco_complemento}'")

        # === NORMALIZAÇÃO E LIMPEZA DOS DADOS ===
        
        # Limpar e normalizar rua
        endereco_rua_limpo = (endereco_rua or '').strip()
        
        # Limpar e normalizar número
        endereco_numero_limpo = (endereco_numero or '').strip() or 'S/N'
        
        # Limpar e normalizar bairro
        endereco_bairro_limpo = (endereco_bairro or '').strip() or 'Centro'
        
        # Limpar e normalizar cidade
        endereco_cidade_limpo = (endereco_cidade or '').strip()
        
        # Limpar e normalizar complemento
        endereco_complemento_limpo = (endereco_complemento or '').strip()
        
        # Limpar e normalizar UF (2 letras maiúsculas)
        endereco_uf_limpo = (endereco_uf or '').strip().upper()
        print(f"[DEBUG] ⚠️ UF após .strip().upper(): '{endereco_uf_limpo}' (comprimento: {len(endereco_uf_limpo)} caracteres)")
        
        # ✅ Inicializar endereco_cep_limpo para evitar NameError em lógica abaixo
        endereco_cep_limpo = ''.join(c for c in str(endereco_cep or '') if c.isdigit()) if endereco_cep else ''
        print(f"[DEBUG] 📬 CEP pré-processado: '{endereco_cep_limpo}' (comprimento: {len(endereco_cep_limpo)})")
        
        # === LÓGICA DE VALIDAÇÃO E FALLBACK PARA UF ===
        
        # Mapeamento de nomes de estado para sigla
        mapeamento_estados = {
            'MINAS GERAIS': 'MG',
            'SÃO PAULO': 'SP',
            'RIO DE JANEIRO': 'RJ',
            'BAHIA': 'BA',
            'PARANÁ': 'PR',
            'SANTA CATARINA': 'SC',
            'RIO GRANDE DO SUL': 'RS',
            'GOIÁS': 'GO',
            'MATO GROSSO': 'MT',
            'MATO GROSSO DO SUL': 'MS',
            'BRASÍLIA': 'DF',
            'DISTRITO FEDERAL': 'DF',
            'ACRE': 'AC',
            'ALAGOAS': 'AL',
            'AMAPÁ': 'AP',
            'AMAZONAS': 'AM',
            'CEARÁ': 'CE',
            'ESPÍRITO SANTO': 'ES',
            'MARANHÃO': 'MA',
            'PARÁ': 'PA',
            'PARAÍBA': 'PB',
            'PERNAMBUCO': 'PE',
            'PIAUÍ': 'PI',
            'RIO GRANDE DO NORTE': 'RN',
            'RONDÔNIA': 'RO',
            'RORAIMA': 'RR',
            'SERGIPE': 'SE',
            'TOCANTINS': 'TO',
        }
        
        # PASSO 1: Se é sigla 2 caracteres, aceitar como está
        if len(endereco_uf_limpo) == 2 and endereco_uf_limpo.isalpha():
            print(f"[DEBUG] ✅ UF é sigla válida de 2 caracteres: '{endereco_uf_limpo}'")
            uf_final = endereco_uf_limpo
        
        # PASSO 2: Se é nome extenso de estado, converter para sigla
        elif endereco_uf_limpo in mapeamento_estados:
            uf_final = mapeamento_estados[endereco_uf_limpo]
            print(f"[DEBUG] ✅ Convertendo nome de estado: '{endereco_uf_limpo}' → '{uf_final}'")
        
        # PASSO 3: Se é número (ex: '988' ou CEP '30670565'), tentar extrair via viacep
        elif endereco_uf_limpo.isdigit():
            print(f"[DEBUG] 🔍 Valor é número ('{endereco_uf_limpo}'), trata-se de CEP ou valor inválido")
            print(f"[DEBUG] 📞 Consultando viacep.com.br para extrair UF...")
            
            # Tentar usar este valor como CEP
            uf_final = extrair_uf_do_cep(endereco_uf_limpo)
            
            # Se falhar, tentar com o CEP armazenado
            if uf_final == 'MG' and endereco_cep_limpo:
                print(f"[DEBUG] ⚠️ Fallback: tentando com CEP alternativo: {endereco_cep_limpo}")
                uf_final = extrair_uf_do_cep(endereco_cep_limpo)
        
        # PASSO 4: Se vazio, tenta extrair do CEP
        elif not endereco_uf_limpo or endereco_uf_limpo == '':
            print(f"[DEBUG] 🔍 UF vazio, tentando extrair do CEP: {endereco_cep_limpo}")
            
            if endereco_cep_limpo:
                print(f"[DEBUG] 📞 Consultando viacep.com.br para extrair UF...")
                uf_final = extrair_uf_do_cep(endereco_cep_limpo)
            else:
                print(f"[DEBUG] ❌ Nem UF nem CEP disponível, usando padrão MG")
                uf_final = 'MG'
        
        # PASSO 5: Valor inválido ou desconhecido
        else:
            print(f"[DEBUG] ❌ UF não reconhecido: '{endereco_uf_limpo}' - tentando extrair do CEP")
            
            if endereco_cep_limpo:
                print(f"[DEBUG] 📞 Consultando viacep.com.br para extrair UF...")
                uf_final = extrair_uf_do_cep(endereco_cep_limpo)
            else:
                print(f"[DEBUG] 🔧 Usando padrão MG (Belo Horizonte)")
                uf_final = 'MG'
        
        # Garantir que UF_final tem 2 caracteres
        if not uf_final or len(uf_final) != 2:
            print(f"[DEBUG] ⚠️ UF final inválido: '{uf_final}', usando MG")
            uf_final = 'MG'
        
        endereco_uf_limpo = uf_final
        
        print(f"[DEBUG] ✓ UF final processado: '{endereco_uf_limpo}'")
        
        # Limpar CEP (apenas números, 8 dígitos obrigatórios)
        print(f"[DEBUG] � === PROCESSANDO CEP === ")
        print(f"[DEBUG] Valor bruto enviado pelo frontend: {repr(endereco_cep)}")
        print(f"[DEBUG] Tipo: {type(endereco_cep).__name__}")
        
        endereco_cep_limpo = ''.join(c for c in str(endereco_cep or '') if c.isdigit())
        print(f"[DEBUG] 📬 CEP após remover não-dígitos: '{endereco_cep_limpo}' (comprimento: {len(endereco_cep_limpo)})")
        if len(endereco_cep_limpo) != 8:
            print(f"[ALERTA] CEP INVÁLIDO: '{endereco_cep}' resultou em '{endereco_cep_limpo}' ({len(endereco_cep_limpo)} dígitos, esperado 8)")
            print(f"[ALERTA] CEP será enviado VAZIO (nunca com zeros)")
            endereco_cep_limpo = ''  # 🔧 CEP VAZIO, nunca "00000000"
        else:
            print(f"[DEBUG] ✅ CEP Válido: '{endereco_cep_limpo}' - pronto para envio como '{endereco_cep_limpo[:5]}-{endereco_cep_limpo[5:]}'")
        
        # === LOGS DOS VALORES FINAIS LIMPOS ===
        print(f"\n[DEBUG] === VALORES LIMPOS PARA BLING ===")
        print(f"[DEBUG] Rua limpa: '{endereco_rua_limpo}'")
        print(f"[DEBUG] Número limpo: '{endereco_numero_limpo}'")
        print(f"[DEBUG] Bairro limpo: '{endereco_bairro_limpo}'")
        print(f"[DEBUG] Cidade limpa: '{endereco_cidade_limpo}'")
        print(f"[DEBUG] CEP limpo: '{endereco_cep_limpo}'")
        print(f"[DEBUG] Complemento limpo: '{endereco_complemento_limpo}'")
        print(f"[DEBUG] UF limpo: '{endereco_uf_limpo}'")
        
        # Normalizações mínimas para evitar erro de validação do Bling
        cidade = endereco_cidade_limpo
        uf = endereco_uf_limpo
        cep = endereco_cep_limpo
        
        # Limpar CNPJ (somente números)  
        cnpj_limpo = ''.join(c for c in str(cnpj) if c.isdigit()) if cnpj else ''
        
        # === VALIDAÇÃO RIGOROSA DO CNPJ ===
        def validar_cnpj(cnpj_str):
            """Valida CNPJ: 14 dígitos, não todos iguais, verifica dígitos verificadores"""
            if not cnpj_str or len(cnpj_str) != 14:
                return False
            
            # Não aceita CNPJ com todos os dígitos iguais (ex: 00000000000000)
            if len(set(cnpj_str)) == 1:
                print(f"[BLING] ℹ️ CNPJ com todos dígitos iguais (inválido): {cnpj_str}")
                return False
            
            # Verifica dígitos verificadores (algoritmo CNPJ)
            try:
                numeros = [int(d) for d in cnpj_str]
                
                # Primeiro dígito verificador
                soma = 0
                multiplicadores = [5,4,3,2,9,8,7,6,5,4,3,2]
                for i, mult in enumerate(multiplicadores):
                    soma += numeros[i] * mult
                resto = soma % 11
                digito1 = 0 if resto < 2 else 11 - resto
                if digito1 != numeros[12]:
                    print(f"[BLING] ℹ️ CNPJ falhou validação de checksum (1º dígito): {cnpj_str}")
                    return False
                
                # Segundo dígito verificador
                soma = 0
                multiplicadores = [6,5,4,3,2,9,8,7,6,5,4,3,2]
                for i, mult in enumerate(multiplicadores):
                    soma += numeros[i] * mult
                resto = soma % 11
                digito2 = 0 if resto < 2 else 11 - resto
                if digito2 != numeros[13]:
                    print(f"[BLING] ℹ️ CNPJ falhou validação de checksum (2º dígito): {cnpj_str}")
                    return False
                
                return True
            except Exception as e:
                print(f"[BLING] ⚠️ Erro ao validar checksum CNPJ: {e}")
                return False
        
        cnpj_para_enviar = cnpj_limpo  # Variável separada para controlar se envia ou não
        cnpj_valido = False
        
        if cnpj_limpo:
            print(f"[BLING] 🔍 Validando CNPJ: {cnpj_limpo}")
            cnpj_valido = validar_cnpj(cnpj_limpo)
            
            if cnpj_valido:
                print(f"[BLING] ✅ CNPJ VÁLIDO: {cnpj_limpo}")
            else:
                print(f"[BLING] ❌ CNPJ INVÁLIDO: {cnpj_limpo}")
                print(f"[BLING] 🔧 OMITINDO CNPJ do payload para evitar erro Bling")
                cnpj_para_enviar = ""  # Não enviar CNPJ inválido
        else:
            print(f"[BLING] ⚠️ CNPJ vazio ou não informado")
        
        print(f"[BLING] === CNPJ FINAL PARA ENVIAR ===")
        print(f"[BLING] Valor: {repr(cnpj_para_enviar)}")
        print(f"[BLING] Será incluído no payload: {bool(cnpj_para_enviar)}")
        
        # Escolas públicas são SEMPRE Pessoa Jurídica
        tipo_pessoa = "J"
        
        # === VALIDAR SE DEVE INCLUIR ENDEREÇO ===
        # 🔧 CEP NUNCA é "00000000", então verificar apenas se tem conteúdo válido
        cep_valido = bool(endereco_cep_limpo)  # Sim ou não
        incluir_endereco = bool(
            endereco_rua_limpo or 
            cep_valido or 
            endereco_cidade_limpo
        )
        
        print(f"\n[DEBUG] === DECISÃO DE INCLUIR ENDEREÇO ===")
        print(f"[DEBUG] Tem rua: {bool(endereco_rua_limpo)}")
        print(f"[DEBUG] Tem CEP válido: {cep_valido} ('{endereco_cep_limpo}')")
        print(f"[DEBUG] Tem cidade: {bool(endereco_cidade_limpo)}")
        print(f"[DEBUG] Incluir endereço no payload: {incluir_endereco}")
        
        # Montar payload conforme documentação da API Bling v3
        payload = {
            "nome": nome_para_bling,  # 🔄 RAZÃO SOCIAL (ou TITLE se não tiver)
            "codigo": f"BITRIX_{empresa_data.get('ID', '')}",
            "situacao": "A",  # A = Ativo
            "telefone": telefone,
            "celular": celular,
            "fantasia": fantasia_para_bling,  # 🔄 NOME DA EMPRESA (TITLE)
            "tipo": tipo_pessoa,
            "indicadorIe": 9,  # 9 = Não contribuinte (escolas públicas não têm IE)
            "email": email,
            "tiposContato": [
                {
                    "descricao": "Cliente"
                }
            ]
        }
        
        # === ADICIONAR CNPJ AO PAYLOAD DE FORMA CONDICIONAL ===
        # Somente incluir se validado
        if cnpj_para_enviar:
            payload["numeroDocumento"] = cnpj_para_enviar
            print(f"[BLING] ✅ CNPJ incluído no payload: {cnpj_para_enviar}")
        else:
            print(f"[BLING] ⚠️ CNPJ NÃO será incluído no payload (vazio ou inválido)")
            print(f"[BLING] 📝 Criando contato sem numeroDocumento (aceitável para escolas públicas)")
        
        # Incluir endereço apenas se houver dados mínimos
        if incluir_endereco:
            # === VALIDAÇÃO FINAL DO CEP - Remover formatação e validar ===
            print(f"[DEBUG] 📬 VALIDAÇÃO FINAL CEP: '{endereco_cep_limpo}'")
            # Se tem formatação tipo "12345-678", limpar
            cep_final = ''.join(c for c in str(endereco_cep_limpo or '') if c.isdigit())
            if len(cep_final) == 8:
                # ✅ Formatar CEP com hífen (conforme API Bling: XXXXX-XXX)
                cep_para_enviar = f"{cep_final[:5]}-{cep_final[5:]}"
                print(f"[DEBUG] ✅ CEP final para enviar: '{cep_para_enviar}' (formatado com hífen)")
            else:
                print(f"[DEBUG] ⚠️ CEP inválido após validação: '{endereco_cep_limpo}' → '{cep_final}' ({len(cep_final)} dígitos)")
                # Se não tem 8 dígitos, marcar como vazio
                cep_para_enviar = ''
                print(f"[DEBUG] 📍 CEP marcado como vazio para o Bling")
            
            # === VALIDAÇÃO FINAL DO UF - Garantir 2 letras ===
            print(f"[DEBUG] 🗺️ VALIDAÇÃO FINAL UF: '{endereco_uf_limpo}'")
            if len(endereco_uf_limpo) == 2:
                uf_para_enviar = endereco_uf_limpo
                print(f"[DEBUG] ✅ UF final para enviar: '{uf_para_enviar}'")
            else:
                print(f"[DEBUG] ⚠️ UF inválido (não é sigla 2 letras): '{endereco_uf_limpo}' ({len(endereco_uf_limpo)} caracteres)")
                uf_para_enviar = ''
                print(f"[DEBUG] 📍 UF marcado como vazio para o Bling")
            
            payload["endereco"] = {
                "geral": {
                    "endereco": endereco_rua_limpo,
                    "numero": endereco_numero_limpo,
                    "complemento": endereco_complemento_limpo,
                    "bairro": endereco_bairro_limpo,
                    "cep": cep_para_enviar,  # CEP validado
                    "municipio": endereco_cidade_limpo,
                    "uf": uf_para_enviar  # UF validado
                }
            }
            print(f"[DEBUG] ✅ Endereço incluído no payload")
        else:
            print(f"[DEBUG] ⚠️ Endereço omitido - dados insuficientes")
        
        # === LOGS FINAIS DO PAYLOAD ===
        print(f"\n[DEBUG] === PAYLOAD FINAL PARA BLING ===")
        if 'endereco' in payload:
            endereco_payload = payload['endereco']['geral']
            print(f"[DEBUG] Payload Bling endereco.geral:")
            print(f"[DEBUG]   endereco: '{endereco_payload.get('endereco', '')}'")
            print(f"[DEBUG]   numero: '{endereco_payload.get('numero', '')}'")
            print(f"[DEBUG]   complemento: '{endereco_payload.get('complemento', '')}'")
            print(f"[DEBUG]   bairro: '{endereco_payload.get('bairro', '')}'")
            print(f"[DEBUG]   cep: '{endereco_payload.get('cep', '')}'")
            print(f"[DEBUG]   municipio: '{endereco_payload.get('municipio', '')}'")
            print(f"[DEBUG]   uf: '{endereco_payload.get('uf', '')}'")
        else:
            print(f"[DEBUG] ⚠️ ENDEREÇO OMITIDO do payload")
        
        
        # Adicionar vendedor se fornecido
        # 🆕 Usar NOME do representante (não ID fixo!)
        # Se tem nome, usar como vendedor (melhor que ID padrão Nayara)
        
        # Prioridade:
        # 1. vendedor_nome (passado como parâmetro - do cache!)
        # 2. responsavel_representante (armazenado em empresa_data)
        # 3. vendedor_id (se encontrou match no Bling)
        
        nome_vendedor_final = vendedor_nome
        if not nome_vendedor_final:
            nome_vendedor_final = empresa_data.get('responsavel_representante', '')
            if nome_vendedor_final:
                print(f"[BLING] 🔍 Usando responsavel_representante de empresa_data: '{nome_vendedor_final}'")
        
        # Tentar resolver vendedor por nome se não houver ID
        vendedor_id_resolvido = vendedor_id
        
        if nome_vendedor_final and not vendedor_id_resolvido:
            print(f"[BLING] 🔍 Buscando vendedor por nome: '{nome_vendedor_final}'")
            vendedor_id_resolvido = buscar_vendedor_por_nome_flexivel(access_token, nome_vendedor_final)
            if vendedor_id_resolvido:
                print(f"[BLING] ✅ Vendedor resolvido: '{nome_vendedor_final}' -> ID {vendedor_id_resolvido}")
            else:
                print(f"[BLING] ⚠️ Vendedor '{nome_vendedor_final}' não encontrado no Bling - campo vendedor será OMITIDO")
        
        # INJETAR VENDEDOR NO PAYLOAD
        # Bling API v3: SOMENTE aceita ID numérico. Nunca enviar nome sem ID.
        if vendedor_id_resolvido:
            payload["vendedor"] = {"id": vendedor_id_resolvido}
            print(f"[BLING] 📝 Vendedor do contato: {nome_vendedor_final} (ID: {vendedor_id_resolvido})")
        else:
            print(f"[BLING] ℹ️ Vendedor omitido do payload (sem ID válido)")
        
        # Adicionar responsável/representante se fornecido
        print(f"\n[BLING CONTATO] {'='*60}")
        print(f"[BLING CONTATO] 👤 CAMPO: aosCuidadosDe (Responsável/Representante)")
        print(f"[BLING CONTATO] {'='*60}")
        
        responsavel_representante = empresa_data.get('responsavel_representante', '')
        
        if responsavel_representante:
            payload["aosCuidadosDe"] = responsavel_representante
            print(f"[BLING CONTATO] ✅ RESPONSÁVEL ADICIONADO!")
            print(f"[BLING CONTATO]    • Campo: aosCuidadosDe")
            print(f"[BLING CONTATO]    • Valor: '{responsavel_representante}'")
            print(f"[BLING CONTATO]    • Este valor aparecerá no contato do Bling")
        else:
            print(f"[BLING CONTATO] ⚠️  SEM RESPONSÁVEL")
            print(f"[BLING CONTATO]    • Campo responsavel_representante está vazio")
            print(f"[BLING CONTATO]    • Contato será criado SEM representante")
        
        print(f"[BLING CONTATO] {'='*60}\n")
        
        print(f"[BLING] 📝 Criando contato: {nome}")
        print(f"[BLING] 📝 Tipo de pessoa: J (Jurídica) - FORÇADO")
        print(f"[BLING] 📝 CNPJ original: '{cnpj}'")
        print(f"[BLING] 📝 CNPJ limpo: '{cnpj_limpo}' (comprimento: {len(cnpj_limpo)})")
        print(f"[BLING] 📝 CNPJ será enviado: {bool(cnpj_para_enviar)}")
        print(f"[BLING] 📝 Telefone: {telefone}")
        print(f"[BLING] 📝 Email: {email}")
        print(f"[BLING] 📝 Vendedor ID: {vendedor_id if vendedor_id else 'Não informado'}")
        
        # VALIDAÇÃO FINAL DO PAYLOAD
        if cnpj_limpo:
            print(f"[BLING] ✅ ENVIANDO COM CNPJ: {cnpj_limpo}")
        else:
            print(f"[BLING] ⚠️ ENVIANDO SEM CNPJ (campo vazio)")
        
        print(f"\n[BLING] 📦 === PAYLOAD COMPLETO ENVIADO AO BLING ===")
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        
        response = requests.post(
            f"{BLING_API_BASE}/contatos",
            headers=headers,
            json=payload,
            timeout=30
        )
        
        # ===== TRATAMENTO DE 401: LAZY REFRESH COM RETRY =====
        if response.status_code == 401:
            print(f"[BLING] ⚠️ HTTP 401 - Token expirado! Tentando renovar...")
            
            if TOKEN_MANAGER_AVAILABLE:
                try:
                    # Renovar token usando a função correta do token manager
                    success, new_tokens = refresh_bling_token()
                    
                    if success:
                        print(f"[BLING] ✅ Token renovado com sucesso!")
                        print(f"[BLING] 🔄 Retry da criação de contato com novo token...")
                        
                        # Obter o novo token
                        new_token = get_valid_bling_token()
                        
                        if new_token:
                            # Atualizar headers com novo token
                            headers["Authorization"] = f"Bearer {new_token}"
                            
                            # RETRY: Fazer requisição POST novamente
                            response = requests.post(
                                f"{BLING_API_BASE}/contatos",
                                headers=headers,
                                json=payload,
                                timeout=30
                            )
                            
                            print(f"[BLING] ✅ Retry status: {response.status_code}")
                        else:
                            print(f"[BLING] ❌ Não conseguiu obter novo token após renovação")
                    else:
                        print(f"[BLING] ❌ Falha ao renovar token")
                        
                except Exception as e:
                    print(f"[BLING] ❌ Erro ao renovar token: {e}")
                    import traceback
                    traceback.print_exc()
            else:
                print(f"[BLING] ⚠️ Token manager não disponível, não conseguindo renovar")
        
        if response.status_code in [200, 201]:
            result = response.json()
            contato_id = result.get('data', {}).get('id')
            print(f"[BLING] ✅ Contato criado com sucesso! ID: {contato_id}")
            print(f"[BLING] 📦 RESPOSTA BLING: {json.dumps(result, indent=2, ensure_ascii=False)}")
            
            # VERIFICAÇÃO: Buscar o contato recém-criado para confirmar que CNPJ foi salvo
            if contato_id:
                try:
                    import time as _time
                    _time.sleep(1)  # Aguardar Bling processar
                    verify_resp = requests.get(
                        f"{BLING_API_BASE}/contatos/{contato_id}",
                        headers=headers,
                        timeout=15
                    )
                    print(f"[BLING] 🔍 VERIFICAÇÃO PÓS-CRIAÇÃO (GET /contatos/{contato_id}):")
                    print(f"[BLING] 🔍 Status GET: {verify_resp.status_code}")
                    if verify_resp.status_code == 200:
                        contato_salvo = verify_resp.json().get('data', {})
                        cnpj_salvo = contato_salvo.get('numeroDocumento', '')
                        tipo_salvo = contato_salvo.get('tipo', '')
                        endereco_salvo = contato_salvo.get('enderecos', [])
                        
                        print(f"[BLING] 🔍 DADOS COMPLETOS DO CONTATO SALVO NO BLING:")
                        print(json.dumps(contato_salvo, indent=2, ensure_ascii=False))
                        
                        # Verificar CNPJ
                        print(f"\n[BLING] 🔍 === VERIFICAÇÃO CNPJ ===")
                        print(f"[BLING] 🔍 CNPJ que enviamos: '{cnpj_limpo}'")
                        print(f"[BLING] 🔍 CNPJ salvo no Bling: '{cnpj_salvo}'")
                        print(f"[BLING] 🔍 Tipo salvo: '{tipo_salvo}'")
                        
                        # Verificar ENDEREÇO 
                        print(f"\n[BLING] 🔍 === VERIFICAÇÃO ENDEREÇO ===")
                        if incluir_endereco:
                            print(f"[BLING] 🔍 Endereço foi ENVIADO no payload")
                            if endereco_salvo and len(endereco_salvo) > 0:
                                endereco_bling = endereco_salvo[0]  # Primeiro endereço
                                print(f"[BLING] ✅ Endereço SALVO no Bling:")
                                print(f"[BLING]   Rua: '{endereco_bling.get('endereco', '')}'")
                                print(f"[BLING]   Número: '{endereco_bling.get('numero', '')}'")
                                print(f"[BLING]   Bairro: '{endereco_bling.get('bairro', '')}'")
                                print(f"[BLING]   CEP: '{endereco_bling.get('cep', '')}'")
                                print(f"[BLING]   Cidade: '{endereco_bling.get('municipio', '')}'")
                                print(f"[BLING]   UF: '{endereco_bling.get('uf', '')}'")
                                print(f"[BLING]   Complemento: '{endereco_bling.get('complemento', '')}'")
                                
                                # Comparar se os dados coincidem
                                # 🔧 BING retorna CEP formatado "XXXXX-XXX", remover formatação para comparar
                                cep_bling_limpo = ''.join(c for c in endereco_bling.get('cep', '') if c.isdigit())
                                if (
                                    endereco_bling.get('endereco', '') == endereco_rua_limpo and
                                    endereco_bling.get('municipio', '') == endereco_cidade_limpo and
                                    cep_bling_limpo == endereco_cep_limpo
                                ):
                                    print(f"[BLING] ✅ Endereço salvo corretamente!")
                                else:
                                    print(f"[BLING] ⚠️ Endereço salvo DIFERENTE do enviado!")
                            else:
                                print(f"[BLING] ❌ ENDEREÇO NÃO FOI SALVO no Bling!")
                                print(f"[BLING] 💡 Possível causa: formato incorreto ou validação falhou")
                        else:
                            print(f"[BLING] ℹ️ Endereço não foi enviado (dados insuficientes)")
                        
                        # Verificar e corrigir CNPJ se necessário
                        if not cnpj_salvo and cnpj_limpo:
                            print(f"[BLING] ⚠️⚠️⚠️ CNPJ NÃO FOI SALVO PELO POST! Forçando PUT...")
                            put_ok = _atualizar_cnpj_contato(access_token, contato_id, cnpj_limpo, nome)
                            if put_ok:
                                print(f"[BLING] ✅ CNPJ adicionado via PUT com sucesso!")
                            else:
                                print(f"[BLING] ❌ PUT CNPJ também falhou!")
                        
                        # Verificar e corrigir ENDEREÇO se necessário
                        if incluir_endereco and (not endereco_salvo or len(endereco_salvo) == 0):
                            print(f"[BLING] ⚠️⚠️⚠️ ENDEREÇO NÃO FOI SALVO PELO POST! Forçando PUT...")
                            put_endereco_ok = _atualizar_endereco_contato(
                                access_token, contato_id, 
                                endereco_rua_limpo, endereco_numero_limpo, 
                                endereco_bairro_limpo, endereco_cidade_limpo, 
                                endereco_uf_limpo, endereco_cep_limpo, 
                                endereco_complemento_limpo, nome
                            )
                            if put_endereco_ok:
                                print(f"[BLING] ✅ ENDEREÇO adicionado via PUT com sucesso!")
                            else:
                                print(f"[BLING] ❌ PUT ENDEREÇO também falhou!")
                    else:
                        print(f"[BLING] ⚠️ GET verificação falhou: {verify_resp.status_code} - {verify_resp.text[:300]}")
                except Exception as e_verify:
                    print(f"[BLING] ⚠️ Erro na verificação pós-criação: {e_verify}")
                    import traceback
                    traceback.print_exc()
            
            return {
                "id": contato_id,
                "nome": nome,
                "numeroDocumento": cnpj_limpo,
                "criado": True
            }, None
        else:
            error_msg = response.text
            print(f"[BLING] ❌ Erro ao criar contato: {response.status_code}")
            print(f"[BLING] ❌ Detalhes: {error_msg}")
            
            # ========== TRATAMENTO ESPECIAL: CNPJ DUPLICADO ==========
            # Se CNPJ já existe, retentar criando sem o CNPJ (não bloquear)
            if 'cadastrado' in error_msg.lower() and ('cnpj' in error_msg.lower() or 'documento' in error_msg.lower()):
                print(f"\n[BLING] 🔄 CNPJ DUPLICADO - retentando criação SEM o CNPJ...")
                payload_sem_cnpj = {k: v for k, v in payload.items() if k != 'numeroDocumento'}
                response_retry = requests.post(
                    f"{BLING_API_BASE}/contatos",
                    headers=headers,
                    json=payload_sem_cnpj,
                    timeout=30
                )
                print(f"[BLING] 🔄 Retry sem CNPJ: status {response_retry.status_code}")
                if response_retry.status_code in [200, 201]:
                    result_retry = response_retry.json()
                    contato_id_retry = result_retry.get('data', {}).get('id')
                    print(f"[BLING] ✅ Contato criado sem CNPJ! ID: {contato_id_retry}")
                    return {
                        "id": contato_id_retry,
                        "nome": nome,
                        "numeroDocumento": '',
                        "criado": True
                    }, None
                else:
                    print(f"[BLING] ❌ Retry sem CNPJ também falhou: {response_retry.text[:300]}")
                    error_msg = response_retry.text
            
            # ========== TRATAMENTO: ERRO DE CIDADE INVÁLIDA ==========
            # Se o erro for de cidade inválida (e não CNPJ duplicado), tentar sem endereço
            elif 'cidade' in error_msg.lower() and 'endereco' in payload:
                print(f"[BLING] 🔄 Erro de cidade - tentando criar sem endereço...")
                payload.pop('endereco', None)
                response2 = requests.post(
                    f"{BLING_API_BASE}/contatos",
                    headers=headers,
                    json=payload,
                    timeout=30
                )
                if response2.status_code in [200, 201]:
                    result2 = response2.json()
                    contato_id2 = result2.get('data', {}).get('id')
                    print(f"[BLING] ✅ Contato criado sem endereço! ID: {contato_id2}")
                    return {
                        "id": contato_id2,
                        "nome": nome,
                        "numeroDocumento": cnpj_limpo,
                        "criado": True
                    }, None
                else:
                    error_msg = response2.text
                    print(f"[BLING] ❌ Também falhou sem endereço: {error_msg}")
            
            return None, error_msg
            
    except Exception as e:
        print(f"[BLING] 💥 Exceção ao criar contato: {e}")
        import traceback
        traceback.print_exc()
        return None, str(e)


def _reativar_contato_bling(access_token, contato_id, nome_contato=''):
    """Reativa contato com situacao 'E' (Excluido) para 'A' (Ativo) via PUT"""
    try:
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        if not nome_contato:
            try:
                r_get = requests.get(f"{BLING_API_BASE}/contatos/{contato_id}", headers=headers, timeout=15)
                if r_get.status_code == 200:
                    nome_contato = r_get.json().get('data', {}).get('nome', 'Contato')
            except Exception:
                nome_contato = 'Contato'

        put_payload = {
            "nome": nome_contato,
            "situacao": "A",
            "tipo": "J",
            "indicadorIe": 9
        }
        print(f"[BLING] REATIVANDO contato {contato_id} (situacao E -> A)")
        r = requests.put(
            f"{BLING_API_BASE}/contatos/{contato_id}",
            headers=headers,
            json=put_payload,
            timeout=15
        )
        if r.status_code in [200, 201, 204]:
            print(f"[BLING] Contato {contato_id} reativado com sucesso!")
            return True
        else:
            print(f"[BLING] Falha ao reativar contato {contato_id}: {r.status_code} - {r.text[:300]}")
            return False
    except Exception as e:
        print(f"[BLING] Erro ao reativar contato: {e}")
        return False


def validar_contato_completo_para_pedido(access_token, contato_id, empresa_data=None):
    """
    Valida se um contato tem dados COMPLETOS para criar pedido de venda.
    Pedidos de venda NO BLING exigem:
    - CNPJ (numeroDocumento) preenchido
    - Endereço completo (rua, número, bairro, cep, municipio, uf)
    
    Se faltar dados, tenta atualizar o contato.
    
    Retorna: (está_completo, mensagem_erro)
    """
    try:
        headers = {"Authorization": f"Bearer {access_token}"}
        
        # 1️⃣ BUSCAR DADOS ATUAIS DO CONTATO
        print(f"\n[VALIDACAO-CONTATO] === VALIDANDO CONTATO PARA PEDIDO ===")
        print(f"[VALIDACAO-CONTATO] Contato ID: {contato_id}")
        
        response = requests.get(
            f"{BLING_API_BASE}/contatos/{contato_id}",
            headers=headers,
            timeout=15
        )
        
        if response.status_code != 200:
            print(f"[VALIDACAO-CONTATO] ❌ Contato não encontrado! Status: {response.status_code}")
            return (False, f"Contato ID {contato_id} não encontrado no Bling")
        
        contato = response.json().get('data', {})
        print(f"[VALIDACAO-CONTATO] 📋 Contato obtido: {contato.get('nome', 'N/A')}")
        
        # 2️⃣ VALIDAR CNPJ
        cnpj_atual = contato.get('numeroDocumento', '')
        print(f"[VALIDACAO-CONTATO] 🔍 CNPJ ATUAL NO BLING: '{cnpj_atual}'")
        
        if not cnpj_atual or cnpj_atual.strip() == '':
            print(f"[VALIDACAO-CONTATO] ❌ CNPJ FALTANDO NO BLING - tentando atualizar...")
            
            if empresa_data:
                cnpj_novo = empresa_data.get('UF_CRM_1713291425', '')
                print(f"[VALIDACAO-CONTATO] 📥 CNPJ vindo do BITRIX: '{cnpj_novo}'")
                
                cnpj_novo_limpo = ''.join(c for c in str(cnpj_novo) if c.isdigit()) if cnpj_novo else ''
                print(f"[VALIDACAO-CONTATO] 🧹 CNPJ limpo: '{cnpj_novo_limpo}' (comprimento: {len(cnpj_novo_limpo)})")
                
                if cnpj_novo_limpo and len(cnpj_novo_limpo) == 14:
                    print(f"[VALIDACAO-CONTATO] ✅ CNPJ válido! Atualizando no Bling...")
                    nome_contato = contato.get('nome', '')
                    if _atualizar_cnpj_contato(access_token, contato_id, cnpj_novo_limpo, nome_contato):
                        print(f"[VALIDACAO-CONTATO] ✅ CNPJ atualizado com sucesso!")
                        cnpj_atual = cnpj_novo_limpo
                    else:
                        print(f"[VALIDACAO-CONTATO] ⚠️ Falha ao atualizar CNPJ via PUT")
                else:
                    print(f"[VALIDACAO-CONTATO] ❌ CNPJ DO BITRIX INVÁLIDO ou vazio!")
                    print(f"[VALIDACAO-CONTATO]    Valor bruto: '{cnpj_novo}'")
                    print(f"[VALIDACAO-CONTATO]    Esperado: 14 dígitos, recebido: {len(cnpj_novo_limpo)}")
        else:
            print(f"[VALIDACAO-CONTATO] ✅ CNPJ presente: '{cnpj_atual}'")
        
        # 3️⃣ VALIDAR ENDEREÇO
        enderecos = contato.get('enderecos', [])
        print(f"[VALIDACAO-CONTATO] 🔍 Endereços cadastrados no BLING: {len(enderecos)}")
        
        endereco_valido = False
        if enderecos and len(enderecos) > 0:
            endereco = enderecos[0]  # Usar primeiro endereço
            rua = endereco.get('endereco', '').strip()
            numero = endereco.get('numero', '').strip()
            cep = endereco.get('cep', '').strip()
            cidade = endereco.get('municipio', '').strip()
            uf = endereco.get('uf', '').strip()
            
            print(f"[VALIDACAO-CONTATO] 📍 Endereço ATUAL NO BLING:")
            print(f"[VALIDACAO-CONTATO]   Rua: '{rua}'")
            print(f"[VALIDACAO-CONTATO]   Número: '{numero}'")
            print(f"[VALIDACAO-CONTATO]   CEP: '{cep}'")
            print(f"[VALIDACAO-CONTATO]   Cidade: '{cidade}'")
            print(f"[VALIDACAO-CONTATO]   UF: '{uf}'")
            
            # Endereço é válido se tem rua, número, CEP e cidade
            endereco_valido = bool(rua and numero and cep and cidade)
            print(f"[VALIDACAO-CONTATO] ✓ Validação: rua={bool(rua)}, nro={bool(numero)}, cep={bool(cep)}, cidade={bool(cidade)}")
        
        if not endereco_valido:
            print(f"[VALIDACAO-CONTATO] ❌ ENDEREÇO FALTANDO/INCOMPLETO NO BLING - tentando atualizar...")
            
            if empresa_data:
                endereco_rua = empresa_data.get('UF_CRM_1721160042326', '').strip()
                endereco_numero = empresa_data.get('UF_CRM_1721160053841', '').strip() or 'S/N'
                endereco_bairro = empresa_data.get('UF_CRM_1721160072753', '').strip() or 'Centro'
                endereco_cidade = empresa_data.get('UF_CRM_1721160090521', '').strip()
                
                # === EXTRAIR UF CORRETAMENTE ===
                endereco_uf = _extrair_uf_do_bitrix(empresa_data)
                
                # === CEP COM MÚLTIPLOS CAMPOS DE FALLBACK ===
                # 🔧 Campo correto do Bitrix PRIMEIRO!
                campos_cep = [
                    'UF_CRM_1721160082099',  # ← CAMPO CORRETO DO BITRIX!
                    'CEP_FISICO',
                    'UF_CRM_1725646763',
                    'UF_CRM_CEP',
                    'CEP'
                ]
                endereco_cep = ''
                campo_cep_usado = None
                for campo in campos_cep:
                    valor = empresa_data.get(campo)
                    # ✅ Validar se tem conteúdo REAL (não apenas espaços)
                    valor_limpo = str(valor).strip() if valor else ''
                    if valor_limpo:
                        endereco_cep = valor
                        campo_cep_usado = campo
                        print(f"[DEBUG] 📬 CEP encontrado em '{campo}': '{valor_limpo}'")
                        break
                endereco_cep = ''.join(c for c in (endereco_cep or '') if c.isdigit())
                
                endereco_complemento = empresa_data.get('UF_CRM_COMPLEMENTO', '').strip()
                
                print(f"[VALIDACAO-CONTATO] 📥 ENDEREÇO vindo do BITRIX:")
                print(f"[VALIDACAO-CONTATO]   Rua: '{endereco_rua}'")
                print(f"[VALIDACAO-CONTATO]   Número: '{endereco_numero}'")
                print(f"[VALIDACAO-CONTATO]   Bairro: '{endereco_bairro}'")
                print(f"[VALIDACAO-CONTATO]   Cidade: '{endereco_cidade}'")
                print(f"[VALIDACAO-CONTATO]   UF: '{endereco_uf}'")
                print(f"[VALIDACAO-CONTATO]   CEP (campo: '{campo_cep_usado}'): '{endereco_cep}' (comprimento: {len(endereco_cep)})")
                
                # Validar CEP
                if len(endereco_cep) != 8:
                    print(f"[VALIDACAO-CONTATO] ⚠️ CEP inválido: '{endereco_cep}' (esperado: 8 dígitos)")
                    endereco_cep = ''  # NUNCA enviar como "00000000"
                
                if endereco_rua or endereco_cidade or endereco_cep:  # 🔧 OR para incluir CEP mesmo sem rua/cidade
                    print(f"[VALIDACAO-CONTATO] ✅ ENDEREÇO VÁLIDO no Bitrix! Atualizando...")
                    print(f"[VALIDACAO-CONTATO]   {endereco_rua}, {endereco_numero} - {endereco_bairro}")
                    print(f"[VALIDACAO-CONTATO]   {endereco_cidade}, {endereco_uf}, CEP {endereco_cep}")
                    
                    if _atualizar_endereco_contato(access_token, contato_id, endereco_rua, endereco_numero, 
                                                   endereco_bairro, endereco_cidade, endereco_uf, endereco_cep, 
                                                   endereco_complemento, contato.get('nome', '')):
                        print(f"[VALIDACAO-CONTATO] ✅ Endereço atualizado com sucesso!")
                        endereco_valido = True
                    else:
                        print(f"[VALIDACAO-CONTATO] ⚠️ Falha ao atualizar endereço via PUT")
                else:
                    print(f"[VALIDACAO-CONTATO] ❌ ENDEREÇO DO BITRIX INCOMPLETO:")
                    print(f"[VALIDACAO-CONTATO]   Rua: {bool(endereco_rua)}")
                    print(f"[VALIDACAO-CONTATO]   Cidade: {bool(endereco_cidade)}")
        else:
            print(f"[VALIDACAO-CONTATO] ✅ Endereço presente e completo no Bling")
        
        # 4️⃣ VALIDAÇÃO FINAL
        cnpj_ok = bool(cnpj_atual and cnpj_atual.strip())
        endereco_ok = endereco_valido
        
        print(f"\n[VALIDACAO-CONTATO] === RESULTADO FINAL ===")
        print(f"[VALIDACAO-CONTATO] CNPJ: {'✅ OK' if cnpj_ok else '⚠️ FALTANDO'}")
        print(f"[VALIDACAO-CONTATO] Endereço: {'✅ OK' if endereco_ok else '⚠️ FALTANDO/INCOMPLETO'}")
        
        # === NOVA LÓGICA: PERMITIR CRIAR PEDIDO MESMO SEM DADOS COMPLETOS ===
        # O Bling permite criar pedidos sem CNPJ e endereço
        # Vamos apenas tentar preencher esses dados e deixar criar de qualquer forma
        
        if cnpj_ok and endereco_ok:
            print(f"[VALIDACAO-CONTATO] ✅✅✅ CONTATO VALIDADO COM SUCESSO - PRONTO PARA PEDIDO!")
            return (True, "Contato validado com sucesso - dados completos")
        elif cnpj_ok or endereco_ok:
            print(f"[VALIDACAO-CONTATO] ✅✅ CONTATO PARCIALMENTE COMPLETO - PERMITINDO CRIAR PEDIDO")
            print(f"[VALIDACAO-CONTATO]    (CNPJ: {'✅' if cnpj_ok else '⚠️'} | Endereço: {'✅' if endereco_ok else '⚠️'})")
            return (True, "Contato parcialmente completo - pedido será criado")
        else:
            print(f"[VALIDACAO-CONTATO] ⚠️ CONTATO INCOMPLETO - MAS PERMITINDO CRIAR PEDIDO (dados serão adicionados depois)")
            print(f"[VALIDACAO-CONTATO]    (CNPJ: ⚠️ | Endereço: ⚠️)")
            return (True, "Contato incompleto - pedido será criado e dados podem ser adicionados posteriormente")
        
    except Exception as e:
        erro_msg = f"Erro ao validar contato: {str(e)}"
        print(f"[VALIDACAO-CONTATO] ❌ {erro_msg}")
        import traceback
        print(traceback.format_exc())
        return (False, erro_msg)


def _extrair_uf_do_bitrix(empresa_data, campos_metadata=None):
    """
    Extrai UF (estado) corretamente do Bitrix.
    Tenta múltiplas estratégias em ordem de preferência.
    
    Retorna: string com sigla de 2 letras (ex: 'MG', 'SP') ou '' se não encontrado
    """
    print(f"[EXTRAIR-UF] 🔍 Tentando extrair UF do Bitrix...")
    
    # ========== ESTRATÉGIA 0: Campo UF_CRM_1721160082099 (Campo de UF direto) ==========
    # Este campo contém a UF diretamente (ex: 'MG', 'SP')
    uf_direto = empresa_data.get('UF_CRM_1721160082099', '').strip().upper()
    if uf_direto and len(uf_direto) == 2 and uf_direto.isalpha():
        print(f"[EXTRAIR-UF] 🅰️ Campo UF_CRM_1721160082099 tem valor: {repr(uf_direto)}")
        print(f"[EXTRAIR-UF]    ✅ UF encontrada: {uf_direto}")
        return uf_direto
    
    # ========== ESTRATÉGIA 1: Campo UF_CRM_1721160103801 (SELECT que retorna ID) ==========
    # Este campo é um SELECT que retorna um ID que precisa ser traduzido via metadata
    uf_id = empresa_data.get('UF_CRM_1721160103801', '').strip()
    if uf_id:
        print(f"[EXTRAIR-UF] 1️⃣ Campo UF_CRM_1721160103801 tem valor: {repr(uf_id)}")
        # Se temos metadata, tentar traduzir
        if campos_metadata and 'UF_CRM_1721160103801' in campos_metadata:
            items = campos_metadata.get('UF_CRM_1721160103801', {}).get('items', [])
            for item in items:
                if str(item.get('ID')) == str(uf_id):
                    uf_nome = item.get('VALUE', '')
                    print(f"[EXTRAIR-UF]    ✅ Traduzido ID {uf_id} → {repr(uf_nome)}")
                    # Agora converter nome extenso para sigla
                    uf_sigla = _converter_nome_uf_para_sigla(uf_nome)
                    if uf_sigla:
                        print(f"[EXTRAIR-UF]    ✅ Sigla: {uf_sigla}")
                        return uf_sigla
        else:
            print(f"[EXTRAIR-UF]    ⚠️ Metadata não disponível para traduzir ID")
    
    # ========== ESTRATÉGIA 2: Tentar CEP para determinar UF ==========
    # Se encontramos um CEP válido, usar ViaCEP para extrair UF
    cep_raw = ''
    # 🔧 Campo correto do Bitrix PRIMEIRO!
    campos_cep = [
        'UF_CRM_1721160082099',  # ← CAMPO CORRETO DO BITRIX!
        'CEP_FISICO',
        'UF_CRM_1725646763',
        'UF_CRM_CEP',
        'CEP'
    ]
    for campo in campos_cep:
        valor = empresa_data.get(campo, '')
        # ✅ Validar se tem conteúdo REAL (não apenas espaços)
        valor_limpo = str(valor).strip() if valor else ''
        if valor_limpo:
            cep_raw = valor
            print(f"[DEBUG] 📬 CEP encontrado em '{campo}': {repr(valor)}")
            break
    
    if cep_raw:
        cep_limpo = ''.join(c for c in cep_raw if c.isdigit())
        if len(cep_limpo) == 8:
            print(f"[EXTRAIR-UF] 2️⃣ CEP válido encontrado: {cep_limpo}")
            try:
                response = requests.get(
                    f"https://viacep.com.br/ws/{cep_limpo}/json/",
                    timeout=3
                )
                if response.status_code == 200:
                    dados_cep = response.json()
                    if not dados_cep.get('erro'):
                        uf = dados_cep.get('uf', '').upper()
                        if uf and len(uf) == 2:
                            print(f"[EXTRAIR-UF]    ✅ ViaCEP retornou UF: {uf}")
                            return uf
            except Exception as e:
                print(f"[EXTRAIR-UF]    ⚠️ Erro ao consultar ViaCEP: {e}")
    
    # ========== ESTRATÉGIA 3: Último recurso - vazio ==========
    print(f"[EXTRAIR-UF] ❌ Não conseguiu extrair UF")
    return ''


def _converter_nome_uf_para_sigla(nome_uf):
    """Converte nome extenso do estado para sigla (ex: 'MINAS GERAIS' → 'MG')"""
    nome_limpo = (nome_uf or '').strip().upper()
    
    mapeamento = {
        'MINAS GERAIS': 'MG',
        'SÃO PAULO': 'SP',
        'RIO DE JANEIRO': 'RJ',
        'BAHIA': 'BA',
        'PARANÁ': 'PR',
        'SANTA CATARINA': 'SC',
        'RIO GRANDE DO SUL': 'RS',
        'GOIÁS': 'GO',
        'MATO GROSSO': 'MT',
        'MATO GROSSO DO SUL': 'MS',
        'BRASÍLIA': 'DF',
        'DISTRITO FEDERAL': 'DF',
        'ACRE': 'AC',
        'ALAGOAS': 'AL',
        'AMAPÁ': 'AP',
        'AMAZONAS': 'AM',
        'CEARÁ': 'CE',
        'ESPÍRITO SANTO': 'ES',
        'MARANHÃO': 'MA',
        'PARÁ': 'PA',
        'PARAÍBA': 'PB',
        'PERNAMBUCO': 'PE',
        'PIAUÍ': 'PI',
        'RIO GRANDE DO NORTE': 'RN',
        'RONDÔNIA': 'RO',
        'RORAIMA': 'RR',
        'SERGIPE': 'SE',
        'TOCANTINS': 'TO',
    }
    
    return mapeamento.get(nome_limpo, '')


def _atualizar_endereco_contato(access_token, contato_id, rua, numero, bairro, cidade, uf, cep, complemento, nome_contato=''):
    """Atualiza endereço de um contato existente no Bling via PUT - PRESERVANDO CNPJ E DADOS"""
    try:
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        print(f"\n[BLING-PUT-ENDERECO] === ATUALIZANDO ENDEREÇO (COM PRESERVAÇÃO DE DADOS) ===")
        print(f"[BLING-PUT-ENDERECO] Contato ID: {contato_id}")
        
        # 🔧 NOVO: Buscar contato atual PRIMEIRO para preservar CNPJ e outros dados
        print(f"[BLING-PUT-ENDERECO] 🔍 BUSCANDO DADOS ATUAIS DO CONTATO...")
        try:
            r_get = requests.get(f"{BLING_API_BASE}/contatos/{contato_id}", headers=headers, timeout=15)
            if r_get.status_code == 200:
                contato_atual = r_get.json().get('data', {})
                cnpj_atual = contato_atual.get('numeroDocumento', '')
                tipo_atual = contato_atual.get('tipo', 'J')
                situacao_atual = contato_atual.get('situacao', 'A')
                nome_salvo = contato_atual.get('nome', nome_contato or 'Contato')
                
                print(f"[BLING-PUT-ENDERECO] ✅ Contato recuperado:")
                print(f"[BLING-PUT-ENDERECO]    Nome: {nome_salvo}")
                print(f"[BLING-PUT-ENDERECO]    CNPJ: {cnpj_atual}")
                print(f"[BLING-PUT-ENDERECO]    Tipo: {tipo_atual}")
                print(f"[BLING-PUT-ENDERECO]    Situação: {situacao_atual}")
            else:
                print(f"[BLING-PUT-ENDERECO] ⚠️ Falha ao buscar contato: HTTP {r_get.status_code}")
                cnpj_atual = ''
                tipo_atual = 'J'
                situacao_atual = 'A'
                nome_salvo = nome_contato or 'Contato'
        except Exception as e:
            print(f"[BLING-PUT-ENDERECO] ⚠️ Erro ao buscar contato: {e}")
            cnpj_atual = ''
            tipo_atual = 'J'
            situacao_atual = 'A'
            nome_salvo = nome_contato or 'Contato'
        
        # === VALIDAÇÃO E LIMPEZA DO CEP ===
        print(f"[BLING-PUT-ENDERECO] 📬 VALIDAÇÃO CEP: entrada='{cep}'")
        cep_limpo = ''.join(c for c in str(cep or '') if c.isdigit())
        if len(cep_limpo) == 8:
            cep_final = cep_limpo
            print(f"[BLING-PUT-ENDERECO] ✅ CEP válido: '{cep_final}'")
        else:
            cep_final = ''  # Deixar vazio se inválido
            print(f"[BLING-PUT-ENDERECO] ⚠️ CEP inválido ({len(cep_limpo)} dígitos), usando vazio")
        
        # === VALIDAÇÃO E LIMPEZA DO UF ===
        print(f"[BLING-PUT-ENDERECO] 🗺️ VALIDAÇÃO UF: entrada='{uf}'")
        uf_limpo = (uf or '').strip().upper()
        if len(uf_limpo) == 2:
            uf_final = uf_limpo
            print(f"[BLING-PUT-ENDERECO] ✅ UF válida (sigla): '{uf_final}'")
        else:
            # Tentar converter nome extenso
            mapeamento_estados = {
                'MINAS GERAIS': 'MG',
                'SÃO PAULO': 'SP',
                'RIO DE JANEIRO': 'RJ',
                'BAHIA': 'BA',
                'PARANÁ': 'PR',
                'SANTA CATARINA': 'SC',
                'RIO GRANDE DO SUL': 'RS',
                'GOIÁS': 'GO',
                'MATO GROSSO': 'MT',
                'MATO GROSSO DO SUL': 'MS',
                'BRASÍLIA': 'DF',
                'DISTRITO FEDERAL': 'DF',
                'ACRE': 'AC',
                'ALAGOAS': 'AL',
                'AMAPÁ': 'AP',
                'AMAZONAS': 'AM',
                'CEARÁ': 'CE',
                'ESPÍRITO SANTO': 'ES',
                'MARANHÃO': 'MA',
                'PARÁ': 'PA',
                'PARAÍBA': 'PB',
                'PERNAMBUCO': 'PE',
                'PIAUÍ': 'PI',
                'RIO GRANDE DO NORTE': 'RN',
                'RONDÔNIA': 'RO',
                'RORAIMA': 'RR',
                'SERGIPE': 'SE',
                'TOCANTINS': 'TO',
            }
            if uf_limpo in mapeamento_estados:
                uf_final = mapeamento_estados[uf_limpo]
                print(f"[BLING-PUT-ENDERECO] ✅ UF convertida: '{uf_limpo}' → '{uf_final}'")
            else:
                uf_final = ''  # Deixar vazio se inválido
                print(f"[BLING-PUT-ENDERECO] ⚠️ UF não reconhecida '{uf_limpo}', usando vazio")
        
        # 🔧 NOVO: Payload COMPLETO com CNPJ + Endereço (não apenas endereço)
        payload = {
            "nome": nome_salvo,
            "numeroDocumento": cnpj_atual,  # 🔧 PRESERVAR CNPJ
            "tipo": tipo_atual,
            "situacao": situacao_atual,
            "indicadorIe": 9,
            "endereco": {
                "geral": {
                    "endereco": rua,
                    "numero": numero,
                    "complemento": complemento,
                    "bairro": bairro,
                    "cep": cep_final,  # CEP validado
                    "municipio": cidade,
                    "uf": uf_final  # UF validado
                }
            }
        }
        
        print(f"\n[BLING-PUT-ENDERECO] === ATUALIZANDO ===")
        print(f"[BLING-PUT-ENDERECO] Endereço: {rua}, {numero} - {bairro}")
        print(f"[BLING-PUT-ENDERECO] Cidade/UF: {cidade}/{uf_final}, CEP: {cep_final}")
        print(f"[BLING-PUT-ENDERECO] CNPJ será preservado: {cnpj_atual}")
        print(f"[BLING-PUT-ENDERECO] Payload completo:")
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        
        response = requests.put(
            f"{BLING_API_BASE}/contatos/{contato_id}",
            headers=headers,
            json=payload,
            timeout=30
        )
        
        print(f"[BLING-PUT-ENDERECO] 🔄 Status da resposta: {response.status_code}")
        
        # ===== TRATAMENTO DE 401: LAZY REFRESH COM RETRY =====
        if response.status_code == 401:
            print(f"[BLING-PUT-ENDERECO] ⚠️ HTTP 401 - Token expirado! Tentando renovar...")
            
            if TOKEN_MANAGER_AVAILABLE:
                try:
                    # Renovar token usando a função correta do token manager
                    success, new_tokens = refresh_bling_token()
                    
                    if success:
                        print(f"[BLING-PUT-ENDERECO] ✅ Token renovado com sucesso!")
                        print(f"[BLING-PUT-ENDERECO] 🔄 Retry da atualização de endereço com novo token...")
                        
                        # Obter o novo token
                        new_token = get_valid_bling_token()
                        
                        if new_token:
                            # Atualizar headers com novo token
                            headers["Authorization"] = f"Bearer {new_token}"
                            
                            # RETRY: Fazer requisição PUT novamente
                            response = requests.put(
                                f"{BLING_API_BASE}/contatos/{contato_id}",
                                headers=headers,
                                json=payload,
                                timeout=30
                            )
                            
                            print(f"[BLING-PUT-ENDERECO] ✅ Retry status: {response.status_code}")
                        else:
                            print(f"[BLING-PUT-ENDERECO] ❌ Não conseguiu obter novo token após renovação")
                    else:
                        print(f"[BLING-PUT-ENDERECO] ❌ Falha ao renovar token")
                        
                except Exception as e:
                    print(f"[BLING-PUT-ENDERECO] ❌ Erro ao renovar token: {e}")
                    import traceback
                    traceback.print_exc()
            else:
                print(f"[BLING-PUT-ENDERECO] ⚠️ Token manager não disponível, não conseguindo renovar")
        
        if response.status_code in [200, 201, 204]:
            print(f"[BLING-PUT-ENDERECO] ✅ Endereço atualizado com sucesso!")
            print(f"[BLING-PUT-ENDERECO] ✅ CNPJ {cnpj_atual} foi preservado!")
            
            # 204 No Content não tem body JSON, então só fazer parse se houver conteúdo
            if response.text:
                try:
                    resultado = response.json()
                    print(f"[BLING-PUT-ENDERECO] 📋 Resposta Bling:")
                    print(json.dumps(resultado, indent=2, ensure_ascii=False, default=str))
                except:
                    print(f"[BLING-PUT-ENDERECO] (Sem conteúdo retornado - 204 No Content)")
            else:
                print(f"[BLING-PUT-ENDERECO] (Sem conteúdo retornado - 204 No Content)")
            
            return True
        else:
            print(f"[BLING-PUT-ENDERECO] ❌ FALHA ao atualizar endereço!")
            print(f"[BLING-PUT-ENDERECO] Status: {response.status_code}")
            print(f"[BLING-PUT-ENDERECO] Resposta texto:")
            print(response.text)
            try:
                erro_json = response.json()
                print(f"[BLING-PUT-ENDERECO] Erro JSON:")
                print(json.dumps(erro_json, indent=2, ensure_ascii=False))
            except:
                pass
            return False
            
    except Exception as e:
        print(f"[BLING-PUT-ENDERECO] ❌ Exceção ao atualizar endereço: {e}")
        import traceback
        print(traceback.format_exc())
        return False


def _atualizar_cnpj_contato(access_token, contato_id, cnpj_limpo, nome_contato=''):
    """Atualiza CNPJ de um contato existente no Bling via PUT - PRESERVANDO ENDEREÇO"""
    try:
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        # Bling API v3 exige 'nome' e 'tipo' no PUT
        print(f"\n[BLING-PUT-CNPJ] === ATUALIZANDO CNPJ ===")
        print(f"[BLING-PUT-CNPJ] Contato ID: {contato_id}")
        print(f"[BLING-PUT-CNPJ] CNPJ para adicionar: {cnpj_limpo}")
        
        print(f"[BLING-PUT-CNPJ] 🔍 BUSCANDO DADOS ATUAIS DO CONTATO (para preservar endereço)...")
        try:
            r_get = requests.get(f"{BLING_API_BASE}/contatos/{contato_id}", headers=headers, timeout=15)
            if r_get.status_code == 200:
                contato_atual = r_get.json().get('data', {})
                nome_contato = contato_atual.get('nome', nome_contato or 'Contato')
                endereco_atual = contato_atual.get('endereco', {})
                
                print(f"[BLING-PUT-CNPJ] ✅ Contato recuperado:")
                print(f"[BLING-PUT-CNPJ]    Nome: {nome_contato}")
                print(f"[BLING-PUT-CNPJ]    Endereço atual: {endereco_atual}")
            else:
                print(f"[BLING-PUT-CNPJ] ⚠️ Falha ao buscar: HTTP {r_get.status_code}")
                nome_contato = nome_contato or 'Contato'
                endereco_atual = {}
        except Exception as e:
            print(f"[BLING-PUT-CNPJ] ⚠️ Erro ao buscar: {e}")
            nome_contato = nome_contato or 'Contato'
            endereco_atual = {}
        
        # Montar payload com CNPJ + tudo que estava lá
        put_payload = {
            "nome": nome_contato,
            "numeroDocumento": cnpj_limpo,
            "tipo": "J",
            "situacao": "A",
            "indicadorIe": 9
        }
        
        # 🔧 IMPORTANTE: Incluir endereço se existir
        if endereco_atual:
            put_payload["endereco"] = endereco_atual
            print(f"[BLING-PUT-CNPJ] ✅ Endereço será preservado na atualização")
        else:
            print(f"[BLING-PUT-CNPJ] ⚠️ Nenhum endereço encontrado para preservar")
        
        print(f"[BLING-PUT-CNPJ] 📝 Payload completo para PUT:")
        print(json.dumps(put_payload, indent=2, ensure_ascii=False))
        
        print(f"[BLING-PUT-CNPJ] 🔄 Enviando PUT para {BLING_API_BASE}/contatos/{contato_id}...")
        
        r = requests.put(
            f"{BLING_API_BASE}/contatos/{contato_id}",
            headers=headers,
            json=put_payload,
            timeout=15
        )
        
        print(f"[BLING-PUT-CNPJ] 📥 Status da resposta: {r.status_code}")
        
        # ===== TRATAMENTO DE 401: LAZY REFRESH COM RETRY =====
        if r.status_code == 401:
            print(f"[BLING-PUT-CNPJ] ⚠️ HTTP 401 - Token expirado! Tentando renovar...")
            
            if TOKEN_MANAGER_AVAILABLE:
                try:
                    # Renovar token usando a função correta do token manager
                    success, new_tokens = refresh_bling_token()
                    
                    if success:
                        print(f"[BLING-PUT-CNPJ] ✅ Token renovado com sucesso!")
                        print(f"[BLING-PUT-CNPJ] 🔄 Retry da atualização de CNPJ com novo token...")
                        
                        # Obter o novo token
                        new_token = get_valid_bling_token()
                        
                        if new_token:
                            # Atualizar headers com novo token
                            headers["Authorization"] = f"Bearer {new_token}"
                            
                            # RETRY: Fazer requisição PUT novamente
                            r = requests.put(
                                f"{BLING_API_BASE}/contatos/{contato_id}",
                                headers=headers,
                                json=put_payload,
                                timeout=15
                            )
                            
                            print(f"[BLING-PUT-CNPJ] ✅ Retry status: {r.status_code}")
                        else:
                            print(f"[BLING-PUT-CNPJ] ❌ Não conseguiu obter novo token após renovação")
                    else:
                        print(f"[BLING-PUT-CNPJ] ❌ Falha ao renovar token")
                        
                except Exception as e:
                    print(f"[BLING-PUT-CNPJ] ❌ Erro ao renovar token: {e}")
                    import traceback
                    traceback.print_exc()
            else:
                print(f"[BLING-PUT-CNPJ] ⚠️ Token manager não disponível, não conseguindo renovar")
        print(f"[BLING-PUT-CNPJ] Resposta texto (primeiros 500 chars):")
        print(f"{r.text[:500]}")
        
        if r.status_code in [200, 201, 204]:
            print(f"[BLING-PUT-CNPJ] ✅ CNPJ {cnpj_limpo} atualizado com sucesso no contato {contato_id}!")
            print(f"[BLING-PUT-CNPJ] ✅ Endereço foi preservado na atualização!")
            try:
                resposta_json = r.json()
                print(f"[BLING-PUT-CNPJ] 📋 Resposta JSON:")
                print(json.dumps(resposta_json, indent=2, ensure_ascii=False, default=str))
            except:
                pass
            return True
        else:
            print(f"[BLING-PUT-CNPJ] ❌ FALHA ao atualizar CNPJ!")
            
            # Verificar se é problema de CNPJ duplicado
            if 'cadastrado' in r.text.lower() or 'duplicado' in r.text.lower():
                print(f"[BLING-PUT-CNPJ] 🚨 CNPJ {cnpj_limpo} BLOQUEADO!")
                print(f"[BLING-PUT-CNPJ] Motivo: Pode estar cadastrado em outro contato no Bling")
                print(f"[BLING-PUT-CNPJ] Solução: Você precisa excluir o contato duplicado MANUALMENTE no Bling")
            
            try:
                erro_json = r.json()
                print(f"[BLING-PUT-CNPJ] Erro JSON:")
                print(json.dumps(erro_json, indent=2, ensure_ascii=False))
            except:
                pass
            
            return False
            
    except Exception as e:
        print(f"[BLING-PUT-CNPJ] ❌ Exceção ao atualizar CNPJ: {e}")
        import traceback
        print(traceback.format_exc())
        return False


def buscar_ou_criar_contato_bling(access_token, empresa_data, vendedor_id=None):
    """
    NOVO FLUXO IDEMPOTENTE - SEMPRE BUSCA E ATUALIZA AO INVÉS DE CRIAR DUPLICATAS
    
    Fluxo:
    1. BUSCAR por CNPJ (se válido) → Se encontrar, verificar e atualizar dados
    2. BUSCAR por NOME → Se encontrar, verificar e atualizar CNPJ
    3. CRIAR novo contato com TODOS os dados corretos
    4. NUNCA criar contato sem CNPJ válido (evita duplicação)
    """
    # 🔥 DEBUG: Ver o que está sendo recebido
    print(f"\n[BUSCAR-CRIAR-CONTATO] === INICIANDO ===")
    print(f"[BUSCAR-CRIAR-CONTATO] vendedor_id recebido: {vendedor_id}")
    print(f"[BUSCAR-CRIAR-CONTATO] empresa_data tem {len(empresa_data)} chaves")
    print(f"[BUSCAR-CRIAR-CONTATO] 🔍 Primeira verificação - responsavel_representante: {empresa_data.get('responsavel_representante', 'NÃO ESTÁ AQUI')}")
    
    # MODO FALLBACK: criar contato fictício quando token não funciona
    if access_token == "FALLBACK_TOKEN":
        nome = empresa_data.get('TITLE', 'Cliente')
        cnpj = empresa_data.get('UF_CRM_1713291425', '')
        print(f"[BLING] 🔄 MODO FALLBACK ATIVO - criando contato fictício")
        print(f"[BLING] Nome: {nome}")
        print(f"[BLING] CNPJ: {cnpj}")
        
        # Extrair endereço mesmo em fallback
        print(f"[BLING] 📍 Extraindo endereço para fallback...")
        endereco_rua = (empresa_data.get('UF_CRM_1721160042326') or '').strip()
        endereco_numero = (empresa_data.get('UF_CRM_1721160053841') or '').strip() or 'S/N'
        endereco_bairro = (empresa_data.get('UF_CRM_1721160072753') or '').strip() or 'Centro'
        endereco_cidade = (empresa_data.get('UF_CRM_1721160090521') or '').strip()
        endereco_uf = _extrair_uf_do_bitrix(empresa_data)
        
        # CEP com fallback - 🔧 Campo correto do Bitrix PRIMEIRO!
        campos_cep = [
            'UF_CRM_1721160082099',  # ← CAMPO CORRETO DO BITRIX!
            'CEP_FISICO',
            'UF_CRM_1725646763',
            'UF_CRM_CEP',
            'CEP'
        ]
        endereco_cep = ''
        for campo in campos_cep:
            valor = empresa_data.get(campo)
            # ✅ Validar se tem conteúdo REAL (não apenas espaços)
            valor_limpo = str(valor).strip() if valor else ''
            if valor_limpo:
                endereco_cep = valor
                print(f"[DEBUG] 📬 CEP encontrado em '{campo}': {repr(valor)}")
                break
        
        endereco_cep = ''.join(c for c in (endereco_cep or '') if c.isdigit())
        endereco_complemento = (empresa_data.get('UF_CRM_COMPLEMENTO') or '').strip()
        
        # Validar UF
        if len(endereco_uf) == 2:
            uf_final = endereco_uf
        else:
            uf_final = 'MG'
        
        # Montar endereço para retorno
        enderecos_fallback = []
        if endereco_rua or endereco_cidade or endereco_cep:
            if len(endereco_cep) == 8:
                cep_fmt = f"{endereco_cep[:5]}-{endereco_cep[5:]}"
            else:
                cep_fmt = ''
            
            enderecos_fallback.append({
                "tipo": "principal",
                "logradouro": endereco_rua,
                "numero": endereco_numero,
                "complemento": endereco_complemento,
                "bairro": endereco_bairro,
                "cep": cep_fmt,
                "municipio": endereco_cidade,
                "uf": uf_final
            })
        
        # Retornar contato fictício que permitirá continuar o fluxo
        contato_fallback = {
            "id": f"FALLBACK_{hash(nome) % 1000000}",  # ID fictício baseado no nome
            "nome": nome,
            "numeroDocumento": cnpj,
            "situacao": "A",
            "enderecos": enderecos_fallback,  # Incluir endereços
            "fallback": True
        }
        
        print(f"[BLING] ✅ Contato FALLBACK criado: ID {contato_fallback['id']} - {nome}")
        if enderecos_fallback:
            print(f"[BLING] ✅ Endereço fallback incluído: {enderecos_fallback[0].get('municipio', 'N/A')}, {enderecos_fallback[0].get('uf', 'N/A')}")
        return contato_fallback
    
    # FLUXO NORMAL (token válido)
    cnpj = empresa_data.get('UF_CRM_1713291425', '')
    nome = empresa_data.get('TITLE', 'Cliente')
    cnpj_limpo = ''.join(c for c in str(cnpj) if c.isdigit()) if cnpj else ''
    
    # Validar CNPJ antes de tudo
    cnpj_valido = cnpj_limpo and len(cnpj_limpo) == 14
    if cnpj_limpo and not cnpj_valido:
        print(f"[BLING] ⚠️ CNPJ INVÁLIDO: {cnpj_limpo} (comprimento: {len(cnpj_limpo)}, esperado: 14)")
        cnpj_limpo = ""  # Limpar CNPJ inválido
    
    # PROTEÇÃO CONTRA CALLS MÚLTIPLOS
    cache_key = _get_contato_cache_key(nome, cnpj_limpo)
    cached_result = _is_recently_processed(cache_key, ttl_seconds=300)
    
    if cached_result:
        print(f"[BLING] 🔄 CACHE HIT: retornando contato recentemente processado")
        print(f"[BLING] 📋 Contato: ID {cached_result.get('id')} - {cached_result.get('nome', 'N/A')}")
        return cached_result
    
    print(f"\n[BLING] === BUSCAR/ATUALIZAR CONTATO (IDEMPOTENTE) ===")
    print(f"[BLING] Empresa: {nome}")
    print(f"[BLING] CNPJ: '{cnpj_limpo}' (válido: {cnpj_valido})")
    
    # ============================================================================
    # ETAPA 1: BUSCAR E ATUALIZAR CONTATO EXISTENTE (por CNPJ se válido)
    # ============================================================================
    if cnpj_limpo and cnpj_valido:
        print(f"\n[BLING] 🔍 ETAPA 1: Buscando contato existente por CNPJ {cnpj_limpo}")
        contato = buscar_contato_bling_por_cnpj(access_token, cnpj_limpo)
        
        if contato:
            print(f"[BLING] ✅ ENCONTRADO contato por CNPJ: ID {contato['id']} - {contato.get('nome', 'SEM NOME')}")
            contato_enderecos = contato.get('enderecos', [])
            
            print(f"[BLING] 📋 Contato encontrado:")
            print(f"[BLING]   CNPJ: '{contato.get('numeroDocumento', '')}'")
            print(f"[BLING]   Endereços: {len(contato_enderecos)}")
            
            # Verificar se está completo
            if len(contato_enderecos) > 0:
                print(f"[BLING] ✅ Contato TEM CNPJ E ENDEREÇO - retornando")
                _cache_contato_result(cache_key, contato)
                return contato
            else:
                print(f"[BLING] ⚠️ Contato TEM CNPJ mas FALTA endereço - tentando atualizar...")
                # Tentar atualizar endereço
                endereco_rua = empresa_data.get('UF_CRM_1721160042326', '').strip()
                endereco_numero = empresa_data.get('UF_CRM_1721160053841', '').strip() or 'S/N'
                endereco_bairro = empresa_data.get('UF_CRM_1721160072753', '').strip() or 'Centro'
                endereco_cidade = empresa_data.get('UF_CRM_1721160090521', '').strip()
                
                # === EXTRAIR UF CORRETAMENTE ===
                endereco_uf = _extrair_uf_do_bitrix(empresa_data)
                
                # === CEP COM MÚLTIPLOS CAMPOS DE FALLBACK ===
                # 🔧 Campo correto do Bitrix PRIMEIRO!
                campos_cep = [
                    'UF_CRM_1721160082099',  # ← CAMPO CORRETO DO BITRIX!
                    'CEP_FISICO',
                    'UF_CRM_1725646763',
                    'UF_CRM_CEP',
                    'CEP'
                ]
                endereco_cep = ''
                campo_cep_usado = None
                for campo in campos_cep:
                    valor = empresa_data.get(campo)
                    # ✅ Validar se tem conteúdo REAL (não apenas espaços)
                    valor_limpo = str(valor).strip() if valor else ''
                    if valor_limpo:
                        endereco_cep = valor
                        campo_cep_usado = campo
                        print(f"[DEBUG] 📬 CEP encontrado em '{campo}': {repr(valor)}")
                        break
                endereco_cep = ''.join(c for c in (endereco_cep or '') if c.isdigit())
                
                endereco_complemento = empresa_data.get('UF_CRM_COMPLEMENTO', '').strip()
                
                if len(endereco_cep) != 8:
                    endereco_cep = ''  # NUNCA enviar como "00000000"
                
                if endereco_rua or endereco_cidade or endereco_cep:  # 🔧 OR para incluir CEP mesmo sem rua/cidade
                    if _atualizar_endereco_contato(access_token, contato['id'], endereco_rua, endereco_numero,
                                                   endereco_bairro, endereco_cidade, endereco_uf, endereco_cep,
                                                   endereco_complemento, contato.get('nome', '')):
                        print(f"[BLING] ✅ Endereço atualizado! Retornando contato")
                        _cache_contato_result(cache_key, contato)
                        return contato
                    else:
                        print(f"[BLING] ⚠️ Falha ao atualizar endereço - retornando contato mesmo assim")
                        # Retornar o contato mesmo com falha - pelo menos tem CNPJ
                        _cache_contato_result(cache_key, contato)
                        return contato
                else:
                    print(f"[BLING] ⚠️ Dados de endereço insuficientes para atualizar")
                    # Retornar o contato mesmo assim - pelo menos tem CNPJ
                    _cache_contato_result(cache_key, contato)
                    return contato
    
    # ============================================================================
    # ETAPA 2: BUSCAR E ATUALIZAR CONTATO POR NOME
    # ============================================================================
    print(f"\n[BLING] 🔍 ETAPA 2: Buscando contato existente por NOME '{nome}'")
    if nome and len(nome.strip()) > 3:
        contato = buscar_contato_bling_por_nome(access_token, nome)
        
        if contato:
            print(f"[BLING] ✅ ENCONTRADO contato por NOME: ID {contato['id']}")
            contato_cnpj_atual = contato.get('numeroDocumento', '')
            contato_enderecos = contato.get('enderecos', [])
            
            # 🎯 LÓGICA SIMPLES: Se contato está vazio (fallback antigo), IGNORAR e CRIAR novo
            # Não tentar atualizar um contato vazio - isso é complexo e pode falhar
            
            if not contato_cnpj_atual and len(contato_enderecos) == 0:
                # Contato completamente vazio - é um fallback antigo ruim
                print(f"[BLING] 🚨 CONTATO VAZIO DETECTADO - É um OLD FALLBACK")
                print(f"[BLING] 🔌 IGNORANDO este contato")
                print(f"[BLING] 📝 Criando novo contato com dados do Bitrix (ETAPA 3)")
                # Não cacheamos resultado ruim - continua para ETAPA 3 abaixo
            else:
                # Contato tem dados - usar como está
                print(f"[BLING] ✅ Contato tem dados suficientes")
                _cache_contato_result(cache_key, contato)
                return contato

    
    # ============================================================================
    # ETAPA 3: CRIAR NOVO CONTATO (com CNPJ ou sem - conforme disponibilidade)
    # ============================================================================
    print(f"\n[BLING] 🆕 ETAPA 3: Nenhum contato similar encontrado - CRIANDO NOVO")
    
    # Nota: Se não temos CNPJ válido, ainda tentaremos criar
    # A API do Bling deixará o campo vazio ou a validação falhará
    # Isso é preferível a criar duplicatas
    
    print(f"[BLING] 📝 Criando contato com dados:")
    print(f"[BLING]   Nome: {nome}")
    if cnpj_limpo and cnpj_valido:
        print(f"[BLING]   CNPJ: {cnpj_limpo}")
    else:
        print(f"[BLING]   CNPJ: (vazio - não será enviado)")
    
    contato_novo, erro = criar_contato_bling(access_token, empresa_data, vendedor_nome=None, vendedor_id=vendedor_id)
    
    if contato_novo:
        print(f"[BLING] ✅ NOVO CONTATO CRIADO: ID {contato_novo.get('id')}")
        print(f"[BLING] 📋 Nome: {contato_novo.get('nome', 'N/A')}")
        print(f"[BLING] 📋 CNPJ: {contato_novo.get('numeroDocumento', 'VAZIO')}")
        _cache_contato_result(cache_key, contato_novo)
        return contato_novo
    
    # ============================================================================
    # ETAPA 4: FALHA NA CRIAÇÃO - TENTAR CONTATO MÍNIMO DE EMERGÊNCIA
    # ============================================================================
    print(f"\n[BLING] ❌ FALHA AO CRIAR CONTATO NORMAL")
    print(f"[BLING] ❌ Erro: {erro}")
    print(f"[BLING] ❌ TODAS as tentativas falharam!")
    print(f"[BLING] 🚨 CRIANDO CONTATO MÍNIMO DE EMERGÊNCIA...")
    
    # === EXTRAIR ENDEREÇO PARA MODO EMERGÊNCIA ===
    # Tentar recuperar dados de endereço mesmo em emergência
    print(f"[BLING] 🔄 Tentando extrair dados de endereço para modo emergência...")
    
    endereco_rua = (empresa_data.get('UF_CRM_1721160042326') or '').strip()
    endereco_numero = (empresa_data.get('UF_CRM_1721160053841') or '').strip() or 'S/N'
    endereco_bairro = (empresa_data.get('UF_CRM_1721160072753') or '').strip() or 'Centro'
    endereco_cidade = (empresa_data.get('UF_CRM_1721160090521') or '').strip()
    endereco_uf = _extrair_uf_do_bitrix(empresa_data)
    
    # CEP com fallback - 🔧 Campo correto do Bitrix PRIMEIRO!
    campos_cep = [
        'UF_CRM_1721160082099',  # ← CAMPO CORRETO DO BITRIX!
        'CEP_FISICO',
        'UF_CRM_1725646763',
        'UF_CRM_CEP',
        'CEP'
    ]
    endereco_cep = ''
    for campo in campos_cep:
        valor = empresa_data.get(campo)
        # ✅ Validar se tem conteúdo REAL (não apenas espaços)
        valor_limpo = str(valor).strip() if valor else ''
        if valor_limpo:
            endereco_cep = valor
            print(f"[DEBUG] 📬 CEP encontrado em '{campo}': {repr(valor)}")
            break
    
    endereco_cep = ''.join(c for c in (endereco_cep or '') if c.isdigit())
    if len(endereco_cep) != 8:
        endereco_cep = ''  # NUNCA enviar como "00000000"
    
    endereco_complemento = (empresa_data.get('UF_CRM_COMPLEMENTO') or '').strip()
    
    # Validar UF (2 caracteres)
    if len(endereco_uf) == 2:
        uf_para_emergencia = endereco_uf
    else:
        uf_para_emergencia = 'MG'  # Padrão
    
    # Montar payload mínimo COM ENDEREÇO
    payload_minimo = {
        "nome": (nome[:50] if nome else "Cliente Bitrix Emergência"),
        "tipo": "J",
        "situacao": "A",
        "indicadorIe": 9,
        "numeroDocumento": ""  # Sempre sem CNPJ no fallback extremo
    }
    
    # Incluir endereço se houver dados mínimos
    if endereco_rua or endereco_cidade or endereco_cep:  # NUNCA "00000000", checar apenas se tem valor
        cep_formatado = ''
        if len(endereco_cep) == 8:
            cep_formatado = f"{endereco_cep[:5]}-{endereco_cep[5:]}"
        
        payload_minimo["endereco"] = {
            "geral": {
                "endereco": endereco_rua,
                "numero": endereco_numero,
                "complemento": endereco_complemento,
                "bairro": endereco_bairro,
                "cep": cep_formatado,
                "municipio": endereco_cidade,
                "uf": uf_para_emergencia
            }
        }
        print(f"[BLING] ✅ Endereço incluído no payload de emergência")
    else:
        print(f"[BLING] ⚠️ Dados de endereço insuficientes no payload de emergência")
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    try:
        print(f"\n[BLING] 📦 === PAYLOAD ENVIADO PARA EMERGÊNCIA ===")
        print(json.dumps(payload_minimo, indent=2, ensure_ascii=False))
        resp = requests.post(f"{BLING_API_BASE}/contatos", headers=headers, json=payload_minimo, timeout=30)
        if resp.status_code in [200, 201]:
            data = resp.json().get('data', {})
            contato_id = data.get('id')
            print(f"[BLING] ✅ CONTATO MÍNIMO CRIADO: ID {contato_id}")
            
            # Construir endereço que foi enviado para retornar
            enderecos_retorno = []
            if "endereco" in payload_minimo:
                endereco_geral = payload_minimo["endereco"].get("geral", {})
                if endereco_geral:
                    enderecos_retorno.append(endereco_geral)
            
            contato_emergencia = {
                "id": contato_id, 
                "nome": payload_minimo["nome"], 
                "numeroDocumento": "", 
                "situacao": "A",
                "enderecos": enderecos_retorno,  # Incluir endereços
                "criado_emergencia": True
            }
            
            # Tentar adicionar CNPJ se válido
            if cnpj_limpo and cnpj_valido:
                print(f"[BLING] 🔄 Tentando adicionar CNPJ {cnpj_limpo} ao contato de emergência...")
                if _atualizar_cnpj_contato(access_token, contato_id, cnpj_limpo, nome):
                    contato_emergencia['numeroDocumento'] = cnpj_limpo
                    print(f"[BLING] ✅ CNPJ adicionado ao contato de emergência!")
                    
            _cache_contato_result(cache_key, contato_emergencia)  # Cache do resultado
            return contato_emergencia
        else:
            print(f"[BLING] ❌ ERRO contato mínimo: {resp.status_code} - {resp.text[:200]}")
    except Exception as e:
        print(f"[BLING] 💥 EXCEÇÃO contato mínimo: {e}")
    
    # Se chegou aqui, algo está MUITO errado com a API Bling
    print(f"[BLING] 🚨🚨🚨 FALHA TOTAL CRÍTICA 🚨🚨🚨")
    print(f"[BLING] ❌ Impossível criar qualquer contato no Bling para: {nome}")
    
    # Verificar se a API Bling está offline
    api_viva, motivo = _verificar_saude_api_bling(access_token)
    if not api_viva:
        mensagem_erro = f"API Bling indisponível: {motivo}. Tente novamente em alguns minutos."
        print(f"[BLING] 🚨 {mensagem_erro}")
        raise Exception(f"FALHA_API_BLING: {mensagem_erro}")
    
    # Se API está viva mas ainda teve erro, algo mais crítico aconteceu
    mensagem_erro = f"Falha crítica ao criar contato para '{nome}'. Verifique dados e tente novamente."
    raise Exception(f"FALHA_CRITICA: {mensagem_erro}")

# FUNÇÕES REMOVIDAS - SUBSTITUÍDAS POR MAPEAMENTO DIRETO
# 
# criar_vendedor_bling() - Criação automática removida, usar mapeamento fixo
# escolher_vendedor_bitrix_para_bling() - Lógica simplificada em resolver_vendedor_bling()
#
# RAZÃO: Mapeamento direto Bitrix ID -> Bling ID é mais confiável que busca/criação automática

def buscar_vendedor_por_nome_flexivel(access_token, nome_bitrix):
    """
    Busca vendedor no Bling por nome com FUZZY MATCHING avançado.
    
    ESTRATÉGIAS DE MATCH (em ordem de confiabilidade):
    1️⃣ EXATO: nome_bitrix == nome_bling
    2️⃣ CONTIDO: bitrix contido em bling (Sibele Calixto em Sibele Martins Calixto)
    3️⃣ SUBSTRING: bling contido em bitrix (raro, mas possível)
    4️⃣ PALAVRAS CHAVE: todas palavras bitrix têm correspodência em bling
    5️⃣ DISTÂNCIA: Levenshtein distance < 20% (permite "Carlim" → "Santina")
    6️⃣ INTERSEÇÃO: 80%+ palavras coincidem
    
    RESULTADO: Retorna o ID do vendedor mais parecido, ou None se não encontrar.
    """
    try:
        headers = {"Authorization": f"Bearer {access_token}"}
        url = f"{BLING_API_BASE}/vendedores"
        
        print(f"\n[FUZZY-MATCH] {'='*70}")
        print(f"[FUZZY-MATCH] 🔍 INICIANDO BUSCA COM FUZZY MATCHING")
        print(f"[FUZZY-MATCH] {'='*70}")
        print(f"[FUZZY-MATCH] Nome a buscar (Bitrix): '{nome_bitrix}'")
        
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            vendedores = result.get('data', [])
            
            print(f"[FUZZY-MATCH] 📊 Total de vendedores no Bling: {len(vendedores)}")
            
            # Normalizar nome do Bitrix
            nome_bitrix_norm = nome_bitrix.lower().strip()
            palavras_bitrix = set(nome_bitrix_norm.split())
            
            print(f"[FUZZY-MATCH] 📐 Nome normalizado: '{nome_bitrix_norm}'")
            print(f"[FUZZY-MATCH] 📝 Palavras-chave: {', '.join(sorted(palavras_bitrix))}")
            print(f"[FUZZY-MATCH]")
            
            # 📊 RASTREAMENTO: Guardar todos os candidatos com scores para debug
            candidatos = []
            
            for idx, vendedor in enumerate(vendedores):
                contato = vendedor.get('contato', {})
                nome_bling = contato.get('nome', '').strip()
                vendedor_id = vendedor.get('id')
                
                if not nome_bling or nome_bling.lower() in ['sem nome', '', 'null']:
                    continue
                
                # Normalizar nome do Bling
                nome_bling_norm = nome_bling.lower().strip()
                palavras_bling = set(nome_bling_norm.split())
                
                print(f"[FUZZY-MATCH] Vendedor #{idx+1}: {vendedor_id} - '{nome_bling}'")
                
                # ═════════════════════════════════════════════════════════════════════
                # ESTRATÉGIA 1: Match exato
                # ═════════════════════════════════════════════════════════════════════
                if nome_bitrix_norm == nome_bling_norm:
                    print(f"[FUZZY-MATCH] ✅ ESTRATÉGIA 1 - MATCH EXATO!")
                    print(f"[FUZZY-MATCH]    '{nome_bitrix}' == '{nome_bling}'")
                    print(f"[FUZZY-MATCH] 🎯 RESULTADO: ID {vendedor_id}")
                    return vendedor_id
                
                # ═════════════════════════════════════════════════════════════════════
                # ESTRATÉGIA 2: Nome Bitrix está contido no Bling
                # ═════════════════════════════════════════════════════════════════════
                # Caso: "Sibele Calixto" em "Sibele Martins Calixto"
                if nome_bitrix_norm in nome_bling_norm:
                    print(f"[FUZZY-MATCH] ✅ ESTRATÉGIA 2 - BITRIX CONTIDO EM BLING!")
                    print(f"[FUZZY-MATCH]    '{nome_bitrix}' ⊂ '{nome_bling}'")
                    candidatos.append((vendedor_id, nome_bling, 95, "contido"))
                    # NÃO RETORNAR JÁ, DEIXAR PROCURAR MATCH EXATO
                    continue
                
                # ═════════════════════════════════════════════════════════════════════
                # ESTRATÉGIA 3: Nome Bling está contido no Bitrix (raro)
                # ═════════════════════════════════════════════════════════════════════
                if nome_bling_norm in nome_bitrix_norm:
                    print(f"[FUZZY-MATCH] ✅ ESTRATÉGIA 3 - BLING CONTIDO EM BITRIX!")
                    print(f"[FUZZY-MATCH]    '{nome_bling}' ⊂ '{nome_bitrix}'")
                    candidatos.append((vendedor_id, nome_bling, 90, "bling_contido"))
                    continue
                
                # ═════════════════════════════════════════════════════════════════════
                # ESTRATÉGIA 4: Todas as palavras do Bitrix estão no Bling
                # ═════════════════════════════════════════════════════════════════════
                if palavras_bitrix <= palavras_bling:  # Subconjunto
                    print(f"[FUZZY-MATCH] ✅ ESTRATÉGIA 4 - TODAS PALAVRAS BITRIX ⊆ BLING!")
                    print(f"[FUZZY-MATCH]    {palavras_bitrix} ⊆ {palavras_bling}")
                    candidatos.append((vendedor_id, nome_bling, 85, "palavras_subset"))
                    continue
                
                # ═════════════════════════════════════════════════════════════════════
                # ESTRATÉGIA 5: Distância de Levenshtein (permitir 20% diferença)
                # ═════════════════════════════════════════════════════════════════════
                # Usa difflib.SequenceMatcher para similarity
                from difflib import SequenceMatcher
                
                similarity = SequenceMatcher(None, nome_bitrix_norm, nome_bling_norm).ratio()
                # Converter para percentual de similaridade
                similaridade_pct = int(similarity * 100)
                
                if similaridade_pct >= 80:
                    print(f"[FUZZY-MATCH] ✅ ESTRATÉGIA 5 - DISTÂNCIA/SIMILARITY!")
                    print(f"[FUZZY-MATCH]    Similarity: {similaridade_pct}%")
                    print(f"[FUZZY-MATCH]    '{nome_bitrix}' ~ '{nome_bling}'")
                    candidatos.append((vendedor_id, nome_bling, similaridade_pct, "similarity"))
                    continue
                
                # ═════════════════════════════════════════════════════════════════════
                # ESTRATÉGIA 6: Interseção significativa de palavras (60%+)
                # ═════════════════════════════════════════════════════════════════════
                if len(palavras_bitrix) > 0 and len(palavras_bling) > 0:
                    intersecao = len(palavras_bitrix & palavras_bling)
                    total_palavras = len(palavras_bitrix | palavras_bling)  # União
                    
                    if total_palavras > 0:
                        intersecao_pct = int((intersecao / total_palavras) * 100)
                    else:
                        intersecao_pct = 0
                    
                    if intersecao_pct >= 60:
                        print(f"[FUZZY-MATCH] ✅ ESTRATÉGIA 6 - INTERSEÇÃO DE PALAVRAS!")
                        print(f"[FUZZY-MATCH]    Interseção: {intersecao_pct}% ({intersecao} palavras)")
                        candidatos.append((vendedor_id, nome_bling, intersecao_pct, "intersecao"))
                        continue
                
                print(f"[FUZZY-MATCH]    ❌ Nenhuma estratégia conseguiu match")
            
            # ═════════════════════════════════════════════════════════════════════
            # SELEÇÃO FINAL: Pegar o melhor candidato se existir
            # ═════════════════════════════════════════════════════════════════════
            print(f"[FUZZY-MATCH]")
            if candidatos:
                # Ordenar por score (maior primeiro)
                candidatos_ordenados = sorted(candidatos, key=lambda x: x[2], reverse=True)
                
                print(f"[FUZZY-MATCH] 📋 CANDIDATOS ENCONTRADOS (ordenados por score):")
                for i, (vid, vnome, score, estrategia) in enumerate(candidatos_ordenados, 1):
                    print(f"[FUZZY-MATCH]    {i}. ID {vid} - '{vnome}' ({score}% - {estrategia})")
                
                # PEGAR O MELHOR
                melhor_id, melhor_nome, melhor_score, estrategia_usada = candidatos_ordenados[0]
                
                print(f"[FUZZY-MATCH]")
                print(f"[FUZZY-MATCH] 🎯 ESCOLHIDO: ID {melhor_id}")
                print(f"[FUZZY-MATCH]    Nome: '{melhor_nome}'")
                print(f"[FUZZY-MATCH]    Score: {melhor_score}%")
                print(f"[FUZZY-MATCH]    Estratégia: {estrategia_usada}")
                print(f"[FUZZY-MATCH] {'='*70}\n")
                
                return melhor_id
            else:
                print(f"[FUZZY-MATCH] ❌ Nenhum candidato encontrado!")
                print(f"[FUZZY-MATCH] 📋 Vendedores disponíveis no Bling:")
                
                for idx, vendedor in enumerate(vendedores, 1):
                    contato = vendedor.get('contato', {})
                    nome_bling = contato.get('nome', 'SEM NOME').strip()
                    vendedor_id = vendedor.get('id')
                    print(f"[FUZZY-MATCH]    {idx}. ID {vendedor_id}: '{nome_bling}'")
                
                print(f"[FUZZY-MATCH] {'='*70}\n")
                return None
        
        else:
            print(f"[FUZZY-MATCH] ⚠️ Erro ao listar vendedores: HTTP {response.status_code}")
            print(f"[FUZZY-MATCH] Resposta: {response.text[:200]}")
            return None
        
    except Exception as e:
        print(f"[FUZZY-MATCH] ❌ Erro na busca fuzzy: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return None

def resolver_vendedor_bling(access_token: str, deal: dict, vendedor_info: dict) -> dict:
    """Resolve o ID E NOME do vendedor no Bling usando mapeamento direto + FUZZY MATCHING.
    
    ESTRATÉGIA (3 FASES):
    1️⃣ CACHE: Bitrix User ID -> Nome completo do usuário (cache local)
    2️⃣ MAPEAMENTO DIRETO: Bitrix User ID -> Bling Vendor ID
    3️⃣ FUZZY MATCHING: Buscar por nome no Bling
    
    NÃO USA Nayara como padrão. Se não encontrar, retorna nome do cache.
    """
    
    global VENDEDOR_MAP
    global MAPA_NOMES_REPRESENTANTES
    
    # Obter ID do responsável do deal Bitrix 
    assigned_id = None
    if isinstance(deal, dict):
        assigned_id = deal.get('ASSIGNED_BY_ID') or deal.get('assigned_by_id')
    
    bitrix_user_id = str(assigned_id) if assigned_id is not None else None
    
    print(f"\n[BLING-RESOLVER] === RESOLVENDO VENDEDOR ===")
    print(f"[BLING-RESOLVER] 🎯 Bitrix User ID = {bitrix_user_id}")
    
    # ═══════════════════════════════════════════════════════════════════════════
    # PASSO 1: Resolver nome do usuário via CACHE
    # ═══════════════════════════════════════════════════════════════════════════
    nome_do_cache = None
    if CACHE_AVAILABLE and CACHE_MANAGER and bitrix_user_id:
        nome_do_cache = CACHE_MANAGER.get_user_name(bitrix_user_id)
        if nome_do_cache:
            print(f"[BLING-RESOLVER] ✅ Nome do cache: '{nome_do_cache}' (ID: {bitrix_user_id})")
        else:
            print(f"[BLING-RESOLVER] ⚠️ ID {bitrix_user_id} não encontrado no cache")
    
    # ═══════════════════════════════════════════════════════════════════════════
    # PASSO 2: MAPEAMENTO DIRETO (Bitrix ID -> Bling ID)
    # ═══════════════════════════════════════════════════════════════════════════
    if bitrix_user_id and bitrix_user_id in VENDEDOR_MAP:
        bling_id = VENDEDOR_MAP[bitrix_user_id]
        nome_vendedor = nome_do_cache or MAPA_NOMES_REPRESENTANTES.get(bitrix_user_id, "")
        print(f"[BLING-RESOLVER] ✅ MAPEAMENTO DIRETO: Bitrix {bitrix_user_id} → Bling ID {bling_id} ('{nome_vendedor}')")
        return {"id": bling_id, "nome": nome_vendedor}
    
    print(f"[BLING-RESOLVER] ℹ️ Não encontrado em mapeamento direto, tentando fuzzy match...")
    
    # ═══════════════════════════════════════════════════════════════════════════
    # PASSO 3: FUZZY MATCHING por nome (do cache ou vendedor_info)
    # ═══════════════════════════════════════════════════════════════════════════
    # Nome para busca: prioriza cache, depois vendedor_info
    nome_para_busca = nome_do_cache
    if not nome_para_busca and vendedor_info and isinstance(vendedor_info, dict):
        nome_para_busca = vendedor_info.get('nome') or vendedor_info.get('NOME')
    
    if nome_para_busca:
        print(f"[BLING-RESOLVER] 🔍 Fuzzy matching por nome: '{nome_para_busca}'")
        
        resultado_fuzzy = resolver_vendedor_por_nome_dinamico(access_token, nome_para_busca)
        
        if resultado_fuzzy and resultado_fuzzy.get('id'):
            print(f"[BLING-RESOLVER] ✅ FUZZY MATCH: ID {resultado_fuzzy['id']} ('{resultado_fuzzy.get('nome')}')")
            return resultado_fuzzy
        else:
            # Não encontrou ID, mas tem nome - enviar só o nome
            print(f"[BLING-RESOLVER] ℹ️ Sem match de ID, retornando nome: '{nome_para_busca}'")
            return {"id": None, "nome": nome_para_busca}
    
    print(f"[BLING-RESOLVER] ⚠️ Sem nome disponível para resolução - vendedor será omitido")
    return {"id": None, "nome": None}

def refresh_token():
    """Renova o token de acesso"""
    global _cached_tokens
    
    with _token_lock:
        tokens = _cached_tokens or load_tokens()
        
        if not tokens or "refresh_token" not in tokens:
            print(f"❌ Não há refresh_token disponível")
            return None

        print(f"🔄 Renovando token automaticamente...")
        
        try:
            r = requests.post(
                TOKEN_URL,
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": tokens["refresh_token"],
                },
                auth=(CLIENT_ID, CLIENT_SECRET),
                headers={"Accept": "application/json"},
                timeout=30
            )

            if r.status_code != 200:
                print(f"❌ Erro ao renovar token: {r.status_code} - {r.text}")
                return None

            new_tokens = r.json()
            save_tokens(new_tokens)
            print(f"✅ Token renovado com sucesso!")
            print(f"⏰ Próxima renovação em: {new_tokens.get('expires_in', 0) // 60} minutos")
            return new_tokens
            
        except Exception as e:
            print(f"❌ Exceção ao renovar token: {e}")
            return None


def auto_refresh_worker():
    """Thread que renova o token automaticamente"""
    global _auto_refresh_running
    
    print(f"🤖 Iniciando renovação automática de tokens...")
    
    while _auto_refresh_running:
        try:
            tokens = _cached_tokens
            
            if tokens:
                expires_in = tokens.get("expires_in", 21600)
                saved_at = tokens.get("saved_at", 0)
                
                if saved_at:
                    # Calcular quanto tempo falta para expirar
                    tempo_decorrido = time.time() - saved_at
                    tempo_restante = expires_in - tempo_decorrido
                    
                    # Renovar quando faltar 10% do tempo (ou 10 minutos, o que for menor)
                    margem_seguranca = min(expires_in * 0.1, 600)  # 10% ou 10min
                    
                    if tempo_restante <= margem_seguranca:
                        print(f"⏰ Token próximo da expiração, renovando...")
                        refresh_token()
                        # Aguardar 5 minutos após renovar
                        time.sleep(300)
                    else:
                        # Aguardar até 80% do tempo de expiração
                        tempo_espera = min(tempo_restante * 0.8, 3600)  # Max 1 hora
                        print(f"😴 Próxima verificação em {int(tempo_espera / 60)} minutos")
                        time.sleep(tempo_espera)
                else:
                    # Sem timestamp, aguardar 5 minutos
                    time.sleep(300)
            else:
                # Sem tokens, aguardar 1 minuto
                time.sleep(60)
                
        except Exception as e:
            print(f"❌ Erro no auto-refresh: {e}")
            time.sleep(60)


def start_auto_refresh():
    """Inicia a thread de auto-renovação"""
    global _auto_refresh_running
    
    if _auto_refresh_running:
        return
    
    _auto_refresh_running = True
    
    thread = threading.Thread(target=auto_refresh_worker, daemon=True)
    thread.start()
    
    print(f"✅ Sistema de renovação automática iniciado")


def stop_auto_refresh():
    """Para a thread de auto-renovação"""
    global _auto_refresh_running
    _auto_refresh_running = False
    print(f"🛑 Sistema de renovação automática parado")


def get_valid_token():
    """Obtém um token válido (renova se necessário)"""
    tokens = load_tokens()
    
    if not tokens:
        return None
    
    if is_token_expired(tokens):
        print(f"🔄 Token expirado, renovando antes de usar...")
        tokens = refresh_token()
    
    return tokens.get("access_token") if tokens else None



@app.route("/", methods=["GET"])
def health():
    tokens = load_tokens()
    token_status = "✅ Autenticado" if tokens else "❌ Não autenticado"
    
    if tokens:
        if is_token_expired(tokens):
            token_status = "⚠️ Token expirando em breve (será renovado automaticamente)"
        
        expires_in = tokens.get("expires_in", 0)
        saved_at = tokens.get("saved_at", 0)
        tempo_restante = (saved_at + expires_in) - time.time() if saved_at else 0
        
        return jsonify({
            "status": "ok", 
            "message": "Backend Bling API rodando",
            "autenticacao": token_status,
            "tokens_file": TOKENS_FILE,
            "auto_renovacao": "✅ Ativa" if _auto_refresh_running else "❌ Inativa",
            "tempo_restante_token": f"{int(tempo_restante / 60)} minutos" if tempo_restante > 0 else "Token expirado"
        }), 200
    
    return jsonify({
        "status": "ok", 
        "message": "Backend Bling API rodando",
        "autenticacao": token_status,
        "tokens_file": TOKENS_FILE
    }), 200


@app.route("/auth/status", methods=["GET"])
def auth_status():
    """Verifica status da autenticação"""
    tokens = load_tokens()
    
    if not tokens:
        return jsonify({
            "autenticado": False,
            "mensagem": "Nenhum token encontrado. Faça a autenticação.",
            "auth_url": "/auth/url"
        }), 401
    
    expires_in = tokens.get("expires_in", 0)
    saved_at = tokens.get("saved_at", 0)
    tempo_restante = (saved_at + expires_in) - time.time() if saved_at else 0
    
    return jsonify({
        "autenticado": True,
        "mensagem": "Autenticação válida",
        "auto_renovacao": "✅ Ativa" if _auto_refresh_running else "❌ Inativa",
        "tempo_restante": f"{int(tempo_restante / 60)} minutos" if tempo_restante > 0 else "Token expirado",
        "expires_in": expires_in,
        "saved_at": saved_at
    })

# DEBUG: Adicionar log em TODOS os endpoints POST
@app.before_request
def log_requests():
    """Log todas as requisições POST para identificar qual endpoint está sendo usado"""
    if request.method == 'POST' and request.path in ['/bling/proposta', '/callback', '/bitrix', '/bling/test', '/bling/criar-proposta', '/propostas-comerciais']:
        print(f"\n" + "="*80)
        print(f"🔍 [RASTREAMENTO] ENDPOINT CHAMADO: {request.method} {request.path}")
        print(f"🕐 [RASTREAMENTO] HORÁRIO: {datetime.now().strftime('%H:%M:%S')}")
        
        if request.is_json:
            data = request.get_json()
            if data:
                print(f"\n📊 [DADOS RECEBIDOS DO FRONTEND]:")
                
                # DEAL
                deal = data.get('deal', {})
                if deal:
                    print(f"   🎯 DEAL:")
                    print(f"      ID: {deal.get('ID', 'N/A')}")
                    print(f"      TITLE: {deal.get('TITLE', 'N/A')}")
                    print(f"      ASSIGNED_BY_ID (Vendedor): {deal.get('ASSIGNED_BY_ID', 'N/A')}")
                
                # EMPRESA
                empresa = data.get('empresa', {})
                if empresa:
                    print(f"   🏢 EMPRESA:")
                    print(f"      ID: {empresa.get('ID', 'N/A')}")
                    print(f"      TITLE: {empresa.get('TITLE', 'N/A')}")
                    print(f"      CNPJ (UF_CRM_1713291425): {empresa.get('UF_CRM_1713291425', 'N/A')}")
                    print(f"      Responsável (UF_CRM_1724855055): {empresa.get('UF_CRM_1724855055', 'N/A')}")
                
                # VENDEDOR
                vendedor = data.get('vendedor', {})
                if vendedor:
                    print(f"   👤 VENDEDOR:")
                    print(f"      ID: {vendedor.get('id', 'N/A')}")
                    print(f"      Nome: {vendedor.get('nome', 'N/A')}")
                    print(f"      Email: {vendedor.get('email', 'N/A')}")
                
                # PRODUTOS
                produtos = data.get('produtos', [])
                if produtos:
                    print(f"   📦 PRODUTOS ({len(produtos)} itens TOTAIS):")
                    # Mostrar todos os produtos (não limitado a 5)
                    for i, p in enumerate(produtos, 1):
                        nome = p.get('PRODUCT_NAME', 'N/A')
                        qty = p.get('QUANTITY', 0)
                        price = p.get('PRICE', 0)
                        print(f"      {i}. {nome} - Qtd: {qty} - Preço: R$ {price}")
        
        print(f"=" * 80 + "\n")
        print(f"🔄 [RASTREAMENTO] Iniciando processamento...")

@app.route("/auth/url", methods=["GET"])
def auth_url():
    import uuid
    redirect_uri = get_redirect_uri(request)
    state = str(uuid.uuid4())
    url = (
        f"{AUTH_URL}?response_type=code"
        f"&client_id={CLIENT_ID}"
        f"&redirect_uri={requests.utils.quote(redirect_uri, safe='')}"
        f"&state={state}"
    )
    print(f"[OAuth] auth_url(): REDIRECT_URI detectada={redirect_uri}", flush=True)
    return {"url": url, "state": state, "redirect_uri": redirect_uri}

@app.route("/bling/auth", methods=["GET"])
def bling_auth():
    """Inicia o fluxo OAuth com Bling"""
    auth = auth_url()
    print(f"[OAuth] /bling/auth: Redirecionando para {auth['redirect_uri']}", flush=True)
    return redirect(auth["url"])

@app.route("/callback", methods=["GET"])
def callback():
    code = request.args.get("code")
    state = request.args.get("state")
    error = request.args.get("error")
    error_desc = request.args.get("error_description")
    
    print(f"[OAuth-Callback] code={code}, state={state}, error={error}, desc={error_desc}", flush=True)
    print(f"[OAuth-Callback] Full URL: {request.url}", flush=True)
    print(f"[OAuth-Callback] Full query params: {request.args}", flush=True)
    
    if error:
        return {
            "error": f"Erro do Bling: {error}",
            "description": error_desc,
            "url_received": request.url
        }, 400
    
    if not code:
        return {
            "error": "Sem code na URL",
            "url_received": request.url,
            "args": dict(request.args),
            "help": "Volte ao /bling/auth e autorize novamente"
        }, 400
    
    redirect_uri = get_redirect_uri(request)
    print(f"[OAuth-Callback] Trocando code por token com REDIRECT_URI={redirect_uri}", flush=True)

    r = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
        },
        auth=(CLIENT_ID, CLIENT_SECRET),
        headers={"Accept": "application/json"},
        timeout=30
    )

    print(f"[OAuth-Callback] Token request status={r.status_code}, response={r.text[:300]}", flush=True)

    if r.status_code != 200:
        return {"status": r.status_code, "body": r.text, "error": "Falha ao trocar code por token"}, r.status_code

    data = r.json()
    save_tokens(data)
    print(f"[OAuth-Callback] ✅ Tokens salvos com sucesso!", flush=True)
    return {
        "ok": True,
        "message": "✅ Tokens salvos com sucesso!",
        "access_token_preview": data.get("access_token", "")[:10] + "...",
        "refresh_token_preview": data.get("refresh_token", "")[:10] + "...",
        "expires_in": data.get("expires_in")
    }

@app.route("/tokens", methods=["GET"])
def get_tokens():
    """Endpoint para obter tokens atuais (para debug/desenvolvimento)"""
    tokens = load_tokens()
    if not tokens:
        return {"error": "Tokens não encontrados"}, 404
    return tokens

@app.route("/callback", methods=["POST"]) 
def callback_bitrix_post():
    """
    Endpoint para criar proposta do Bitrix via POST no /callback  
    FORMATO CORRETO API v3 DO BLING
    """
    try:
        print(f"[BLING-CALLBACK] === INICIANDO CRIAÇÃO DE PROPOSTA ===\n")
        
        # Carregar e validar tokens
        tokens = load_tokens()
        if not tokens:
            return {"error": "Tokens não encontrados. Faça a autenticação primeiro."}, 400

        if is_token_expired(tokens):
            tokens = refresh_token()  
            if not tokens:
                return {"error": "Não foi possível renovar o token. Refaça a autenticação."}, 401

        access_token = tokens.get("access_token")
        if not access_token:
            return {"error": "Access token não encontrado nos tokens."}, 400

        # Obter dados da requisição
        data = request.get_json()
        if not data:
            return {"error": "Dados não fornecidos"}, 400

        deal = data.get("deal", {})
        empresa = data.get("empresa", {})
        produtos = data.get("produtos", [])
        vendedor_info = data.get("vendedor", {})

        # 🔐 VALIDAR STAGE: APENAS CONCLUÍDO PERMITE CRIAR
        stage = deal.get("STAGE_ID") or ""
        stage_valido, msg = validar_stage_para_pedido(stage)
        if not stage_valido:
            print(f"[BLING-CALLBACK] ❌ {msg}")
            return {"erro": "Stage inválido - callback bloqueado", "mensagem": msg}, 400

        print(f"[BLING-CALLBACK] Deal ID: {deal.get('ID')} - Empresa: {empresa.get('TITLE')}")
        print(f"[BLING-CALLBACK] Total de produtos: {len(produtos)}")
        print(f"[BLING-CALLBACK] 🔍 DEBUG DADOS RECEBIDOS:")
        print(f"[BLING-CALLBACK]   📧 Email empresa: {empresa.get('EMAIL', 'N/A')}")
        print(f"[BLING-CALLBACK]   👤 Vendedor: {vendedor_info.get('nome', 'N/A')}")
        print(f"[BLING-CALLBACK]   📧 Email vendedor: {vendedor_info.get('email', 'N/A')}")
        
        # VALIDACAO: Verificar se email da empresa não está sendo contaminado
        if empresa.get('EMAIL'):
            email_empresa = empresa.get('EMAIL', [])
            if isinstance(email_empresa, list) and len(email_empresa) > 0:
                primeiro_email = email_empresa[0].get('VALUE', '')
                print(f"[BLING-CALLBACK] ✅ Email correto da empresa será usado: {primeiro_email}")
            else:
                print(f"[BLING-CALLBACK] ⚠️ Problema na estrutura do email da empresa: {email_empresa}")

        # Validar dados obrigatórios
        if not deal or not empresa or not produtos:
            return {"error": "Deal, empresa e produtos são obrigatórios"}, 400

        # 1. DETERMINAR VENDEDOR NO BLING - MAPEAMENTO DIRETO 
        vendedor_dados = resolver_vendedor_bling(access_token, deal, vendedor_info)
        vendedor_id = vendedor_dados.get('id') if vendedor_dados else None
        vendedor_nome = vendedor_dados.get('nome') if vendedor_dados else None
        
        # 🎯 BUSCAR NOME DO REPRESENTANTE: CACHE PRIMEIRO, depois MAPA
        assigned_by_id = deal.get('ASSIGNED_BY_ID')
        assigned_by_id_str = str(assigned_by_id) if assigned_by_id else ''
        nome_responsavel = None

        # Prioridade 1: Cache
        if CACHE_AVAILABLE and CACHE_MANAGER and assigned_by_id:
            nome_responsavel = CACHE_MANAGER.get_user_name(assigned_by_id)
            if nome_responsavel:
                print(f"[BLING] ✅ Representante do cache: '{nome_responsavel}' (ID: {assigned_by_id_str})")

        # Prioridade 2: Mapa estático
        if not nome_responsavel:
            nome_responsavel = MAPA_NOMES_REPRESENTANTES.get(assigned_by_id_str)
            if nome_responsavel:
                print(f"[BLING] ✅ Representante do mapa: '{nome_responsavel}' (ID: {assigned_by_id_str})")

        if not nome_responsavel:
            print(f"[BLING] ⚠️ ID {assigned_by_id_str} não encontrado - deal sem representante definido")
            nome_responsavel = 'N/A'
        
        if vendedor_id:
            print(f"[BLING] ✅ Vendedor mapeado: Bitrix {deal.get('ASSIGNED_BY_ID')} -> Bling {vendedor_id} ('{vendedor_nome}')")
        else:
            print(f"[BLING] ⚠️ Vendedor não mapeado - campo será OMITIDO da proposta")
            print(f"[BLING] ⚠️ Vendedor não encontrado - campo será deixado VAZIO no Bling")

        # Preparar headers para chamadas à API Bling
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        # Buscar ou criar contato no Bling
        contato_bling = None
        cnpj = empresa.get('UF_CRM_1713291425', '')
        cnpj_limpo = ''.join(c for c in str(cnpj) if c.isdigit()) if cnpj else ''
        
        if cnpj_limpo:
            # Buscar contato existente
            buscar_url = f"{BLING_API_URL}?filtros[numeroDocumento]={cnpj_limpo}"
            response = requests.get(buscar_url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                if result.get('data') and len(result['data']) > 0:
                    contato_bling = result['data'][0]

        # Se não encontrou contato, usar dados da empresa
        if not contato_bling:
            contato_bling = {
                "id": int(empresa.get('ID', 999999)),  
                "nome": empresa.get('TITLE', 'Cliente do Bitrix'),
                "numeroDocumento": cnpj_limpo if cnpj else ''
            }

        # 3. PREPARAR ITENS DA PROPOSTA - ESTRUTURA EXATA
        itens = []
        total = 0
        
        for idx, produto in enumerate(produtos):
            try:
                quantidade = float(produto.get('QUANTITY', 0))
                preco = float(produto.get('PRICE', 0))
                nome_produto = produto.get('PRODUCT_NAME', 'Produto sem nome')
                
                # MAPEAR PRODUTO PARA CÓDIGO BLING
                print(f"[BLING-CALLBACK] Mapeando produto: '{nome_produto}'")
                produto_mapeado = mapear_produto_para_codigo_bling(nome_produto)
                
                if not produto_mapeado:
                    print(f"[BLING-CALLBACK] ❌ Produto '{nome_produto}' não foi mapeado!")
                    # Usar nome como fallback
                    codigo_bling = nome_produto
                    nome_bling = nome_produto
                else:
                    codigo_bling = produto_mapeado.get('codigo', nome_produto)
                    nome_bling = produto_mapeado.get('nome', nome_produto)
                    print(f"[BLING-CALLBACK] ✅ Mapeado: '{nome_produto}' → {codigo_bling}")
                
                # Filtrar produtos com quantidade zero (Bling não aceita)
                if quantidade <= 0:
                    print(f"[BLING-CALLBACK] Produto {nome_produto} ignorado (quantidade: {quantidade})")
                    continue
                
                # ESTRUTURA CORRETA PARA API v3 DO BLING (CALLBACK)
                item = {
                    "codigo": f"BITRIX_{produto.get('PRODUCT_ID', idx+1)}",  # Código obrigatório 
                    "descricao": nome_produto.strip(),                       # Nome do produto
                    "quantidade": quantidade,                                # Quantidade
                    "valor": preco,                                         # Preço unitário
                    "aliquotaIPI": 0,                                       # IPI (obrigatório)
                    "desconto": 0,                                          # Desconto (obrigatório)
                    "unidade": "UN"                                         # Unidade (obrigatório)
                }
                
                itens.append(item)
                total += (quantidade * preco)
                
                print(f"[BLING-CALLBACK] Item {idx+1}: {nome_produto} - Qtd: {quantidade} - Valor: R$ {preco}")
                
            except (ValueError, TypeError) as e:
                print(f"[BLING-CALLBACK] Erro ao processar produto {idx+1}: {e}")
                continue

        if not itens:
            return {"error": "Nenhum produto válido encontrado"}, 400

        # Preparar parcelas (FORMATO CORRETO API v3)
        parcelas = [
            {
                "numeroDias": 30,
                "dataVencimento": (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d"),
                "valor": round(total, 2),
                "observacoes": "Pagamento via Bitrix",
                "formaPagamento": {
                    "id": 2094030  # ID real da forma de pagamento no Bling
                }
            }
        ]

        # Preparar payload da proposta (FORMATO CORRETO API v3)
        payload = {
            "data": datetime.now().strftime("%Y-%m-%d"),
            "situacao": "Em elaboração",
            "numero": int(deal.get('ID', 1)),
            "contato": {
                "id": int(contato_bling.get('id', 12345678))
            },
            "loja": {
                "id": 0,  # ID 0 funciona no Bling
                "unidadeNegocio": {
                    "id": 0
                }
            },
            "desconto": 0,
            "outrasDespesas": 0,
            "garantia": 12,
            "dataProximoContato": (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d"),
            "observacoes": f"Proposta gerada automaticamente do Bitrix CRM\\nDeal ID: {deal.get('ID')}\\nCliente: {empresa.get('TITLE', 'N/A')}",
            # 🧪 COMENTADO: Não preencher observação interna da proposta
            # "observacaoInterna": f"Integração Bitrix x Bling - Deal {deal.get('ID')}",
            "totalOutrosItens": 0,
            "aosCuidadosDe": vendedor_nome if vendedor_nome else nome_responsavel,  # Usar o nome do vendedor resolvido
            "introducao": f"Proposta comercial para {empresa.get('TITLE', 'o cliente')}",
            "prazoEntrega": "30 dias úteis",
            "itens": itens,
            "parcelas": parcelas,
            "vendedor": {
                "id": vendedor_id,  # Usar o vendedor resolvido dinamicamente
                "nome": vendedor_nome  # Incluir o nome do vendedor também
            },
            "transporte": {
                "freteModalidade": 0,
                "frete": 0.0,
                "quantidadeVolumes": 1.0,
                "prazoEntrega": 30,
                "pesoBruto": 1.0,
                "contato": {
                    "id": int(contato_bling.get('id', 12345678)),
                    "nome": empresa.get('TITLE', 'Cliente')
                },
                "volumes": {}
            }
        }

        print(f"[BLING-CALLBACK] Payload preparado - Total: R$ {total:.2f} - {len(itens)} itens")

        # 6. ENVIAR PARA API BLING v3
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        url = f"{BLING_API_BASE}/propostas-comerciais"
        print(f"[BLING-CALLBACK] Enviando POST para: {url}")
        
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        
        print(f"[BLING-CALLBACK] Resposta: Status {response.status_code}")
        
        if response.status_code not in [200, 201]:
            error_details = response.text
            print(f"[BLING-CALLBACK] ERRO: {error_details}")
            return {
                "error": f"Erro {response.status_code} ao criar proposta no Bling",
                "details": error_details,
                "payload_enviado": payload
            }, response.status_code

        # SUCESSO
        proposta_data = response.json()
        proposta_id = proposta_data.get('data', {}).get('id')
        
        print(f"[BLING-CALLBACK] SUCESSO! Proposta criada com ID: {proposta_id}")
        
        # Variáveis para resposta
        nome_cliente = empresa.get('TITLE', 'Cliente')
        aos_cuidados_de = empresa.get('UF_CRM_1724855055', '')
        id_contato = contato_bling.get('id')
        
        return {
            "sucesso": True,
            "id": proposta_id,
            "data": proposta_data.get('data'),
            "cliente": nome_cliente,
            "aos_cuidados": aos_cuidados_de,
            "total": round(total, 2),
            "itens_count": len(itens),
            "detalhes": {
                "deal_id": deal.get('ID'),
                "contato_bling_id": id_contato,
                "vendedor_bling_id": vendedor_id
            }
        }

    except Exception as e:
        import traceback
        return {"error": f"Erro interno: {str(e)}", "traceback": traceback.format_exc()}, 500

@app.route("/bitrix", methods=["POST"])
def criar_proposta_bitrix():
    """
    Endpoint específico para criar proposta do Bitrix
    """
    try:
        # Carregar e validar tokens
        tokens = load_tokens()
        if not tokens:
            return {"error": "Tokens não encontrados. Faça a autenticação primeiro."}, 400

        if is_token_expired(tokens):
            tokens = refresh_token()
            if not tokens:
                return {"error": "Não foi possível renovar o token. Refaça a autenticação."}, 401

        access_token = tokens.get("access_token")
        if not access_token:
            return {"error": "Access token não encontrado nos tokens."}, 400

        # Obter dados da requisição
        data = request.get_json()
        if not data:
            return {"error": "Dados não fornecidos"}, 400

        deal = data.get("deal", {})
        empresa = data.get("empresa", {})
        contatos = data.get("contatos", [])
        produtos = data.get("produtos", [])

        # 🔐 VALIDAR STAGE: APENAS CONCLUÍDO PERMITE CRIAR
        stage = deal.get("STAGE_ID") or ""
        stage_valido, msg = validar_stage_para_pedido(stage)
        if not stage_valido:
            print(f"[BITRIX] ❌ {msg}")
            return {"erro": "Stage inválido - operação bloqueada", "mensagem": msg}, 400

        # Validar dados obrigatórios
        if not deal or not empresa or not produtos:
            return {"error": "Deal, empresa e produtos são obrigatórios"}, 400

        # Preparar cabeçalhos
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

        # Buscar ou criar contato no Bling
        contato_bling = None
        cnpj = empresa.get('UF_CRM_1713291425', '')
        
        if cnpj:
            # Limpar CNPJ (somente números)
            cnpj_limpo = ''.join(c for c in str(cnpj) if c.isdigit())
            
            # Buscar contato existente
            buscar_url = f"{BLING_API_URL}?filtros[numeroDocumento]={cnpj_limpo}"
            response = requests.get(buscar_url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                if result.get('data') and len(result['data']) > 0:
                    contato_bling = result['data'][0]

        # Se não encontrou contato, usar dados da empresa
        if not contato_bling:
            contato_bling = {
                "id": int(empresa.get('ID', 999999)),  
                "nome": empresa.get('TITLE', 'Cliente do Bitrix'),
                "numeroDocumento": cnpj_limpo if cnpj else ''
            }

        # Preparar itens da proposta
        itens = []
        total = 0
        
        print(f"\n[PROPOSTA-DEBUG] === PROCESSANDO {len(produtos)} PRODUTOS DO BITRIX ===")
        
        for produto in produtos:
            try:
                # Extrair dados básicos do produto
                quantidade = float(produto.get('QUANTITY', 0))
                preco = float(produto.get('PRICE', 0))
                subtotal = quantidade * preco
                nome_bitrix = produto.get('PRODUCT_NAME', 'Produto sem nome').strip()
                
                print(f"\n[PRODUTO-DEBUG] === PROCESSANDO PRODUTO {len(itens)+1} ===")
                print(f"[PRODUTO-DEBUG] Nome original: '{nome_bitrix}'")
                print(f"[PRODUTO-DEBUG] Quantidade: {quantidade}")
                print(f"[PRODUTO-DEBUG] Preço: {preco}")
                
                # ETAPA 1: MAPEAR PRODUTO BITRIX PARA CÓDIGO BLING
                produto_mapeado = mapear_produto_para_codigo_bling(nome_bitrix)
                
                if not produto_mapeado:
                    # Produto não encontrado no mapeamento - ERRO CRÍTICO
                    print(f"[ERRO] ❌ Produto do Bitrix sem mapeamento para o Bling: '{nome_bitrix}'")
                    print(f"[ERRO] ⚠️ Proposta não pode ser criada com itens inconsistentes")
                    return {"error": f"Produto não mapeado: '{nome_bitrix}'. Configure o mapeamento primeiro."}, 400
                
                # ETAPA 2: BUSCAR PRODUTO OFICIAL NO BLING 
                codigo_bling = produto_mapeado["codigo"]
                nome_bling_oficial = produto_mapeado["nome"]
                
                print(f"[PRODUTO-DEBUG] ✅ Mapeamento: {codigo_bling} → '{nome_bling_oficial}'")
                
                # Buscar produto completo no Bling
                produto_bling = buscar_produto_bling_por_codigo(access_token, codigo_bling)
                
                # ETAPA 3: MONTAR ITEM - SEMPRE GARANTIR QUE APAREÇA NO BLING
                if produto_bling and "id" in produto_bling and not produto_bling.get("fallback", False):
                    # Produto encontrado na API - usar ID real
                    item = {
                        "produto": {"id": produto_bling["id"]},
                        "codigo": codigo_bling,
                        "descricao": nome_bling_oficial,
                        "quantidade": quantidade,
                        "valor": preco,
                        "aliquotaIPI": 0,
                        "desconto": 0,
                        "unidade": "UN"
                    }
                    print(f"[PRODUTO-DEBUG] ✅ Item COM ID do produto (vinculado): {produto_bling['id']}")
                else:
                    # Produto não encontrado na API - usar estrutura que GARANTE aparição no Bling
                    item = {
                        "codigo": codigo_bling,
                        "descricao": nome_bling_oficial,
                        "quantidade": quantidade,
                        "valor": preco,
                        "aliquotaIPI": 0,
                        "desconto": 0,
                        "unidade": "UN"
                    }
                    print(f"[PRODUTO-DEBUG] ✅ Item SEM ID (não vinculado mas visível): {codigo_bling}")
                
                # Adicionar campos obrigatórios que podem estar faltando
                if "desconto" not in item:
                    item["desconto"] = 0
                if "aliquotaIPI" not in item:
                    item["aliquotaIPI"] = 0
                if "unidade" not in item:
                    item["unidade"] = "UN"
                
                print(f"[PRODUTO-DEBUG] 📦 Estrutura final completa:")
                for campo, valor in item.items():
                    print(f"[PRODUTO-DEBUG]    {campo}: {valor}")
                
                itens.append(item)
                total += subtotal
                
                print(f"[PRODUTO-DEBUG] ✅ Item adicionado! Array agora tem {len(itens)} item(s)")
                
            except (ValueError, TypeError) as e:
                print(f"[ERRO] Erro ao processar produto: {produto}, erro: {e}")
                continue

        if not itens:
            return {"error": "Nenhum produto válido encontrado"}, 400

        print(f"\n[PROPOSTA-DEBUG] === PAYLOAD FINAL ===")
        # Preparar payload da proposta
        payload = {
            "numero": f"BITRIX_{deal.get('ID')}",
            "descricao": deal.get('TITLE', 'Proposta do Bitrix'),
            "contato": {
                "id": contato_bling['id']
            },
            "dataPropostaComoFinal": datetime.now().strftime("%Y-%m-%d"),
            "itens": itens,
            "total": total,
            "observacoes": f"Proposta gerada automaticamente do Bitrix - Deal ID: {deal.get('ID')}"
        }
        
        print(f"[PROPOSTA-DEBUG] Payload completo:")
        print(f"[PROPOSTA-DEBUG] {payload}")

        # Criar proposta no Bling
        response = requests.post(
            BLING_PROPOSTAS_URL,
            headers=headers,
            json=payload,
            timeout=30
        )

        if response.status_code in [200, 201]:
            proposta_data = response.json()
            print(f"[PROPOSTA-DEBUG] ✅ Proposta criada com sucesso no Bling!")
            print(f"[PROPOSTA-DEBUG] ID da proposta: {proposta_data.get('data', {}).get('id')}")
            return {
                "sucesso": True,
                "data": proposta_data.get('data'),
                "id": proposta_data.get('data', {}).get('id'),
                "numero": payload['numero'],
                "total": total,
                "itens_count": len(itens)
            }
        else:
            print(f"[PROPOSTA-DEBUG] ❌ Erro ao criar proposta no Bling: {response.status_code}")
            print(f"[PROPOSTA-DEBUG] Detalhes: {response.text}")
            return {
                "error": f"Erro ao criar proposta no Bling: {response.status_code}",
                "details": response.text
            }, response.status_code

    except Exception as e:
        return {"error": f"Erro interno: {str(e)}"}, 500

        if not itens:
            return {"error": "Nenhum produto válido encontrado"}, 400

        # Preparar payload da proposta
        payload = {
            "numero": f"BITRIX_{deal.get('ID')}",
            "descricao": deal.get('TITLE', 'Proposta do Bitrix'),
            "contato": {
                "id": contato_bling['id']
            },
            "dataPropostaComoFinal": datetime.now().strftime("%Y-%m-%d"),
            "itens": itens,
            "total": total,
            "observacoes": f"Proposta gerada automaticamente do Bitrix - Deal ID: {deal.get('ID')}"
        }

        # Criar proposta no Bling
        response = requests.post(
            BLING_PROPOSTAS_URL,
            headers=headers,
            json=payload,
            timeout=30
        )

        if response.status_code in [200, 201]:
            proposta_data = response.json()
            return {
                "sucesso": True,
                "data": proposta_data.get('data'),
                "id": proposta_data.get('data', {}).get('id'),
                "numero": payload['numero'],
                "total": total,
                "itens_count": len(itens)
            }
        else:
            return {
                "error": f"Erro ao criar proposta no Bling: {response.status_code}",
                "details": response.text
            }, response.status_code

    except Exception as e:
        return {"error": f"Erro interno: {str(e)}"}, 500

@app.route("/bling/test", methods=["GET"])
def bling_test():
    tokens = load_tokens()
    if not tokens:
        return {"error": "Tokens não encontrados. Faça a autenticação primeiro."}, 400

    if is_token_expired(tokens):
        tokens = refresh_token()
        if not tokens:
            return {"error": "Não foi possível renovar o token. Refaça a autenticação."}, 401

    access_token = tokens.get("access_token")
    if not access_token:
        return {"error": "Access token não encontrado nos tokens."}, 400

    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(BLING_API_URL, headers=headers, timeout=30)

    if response.status_code != 200:
        return {"status": response.status_code, "body": response.text}, response.status_code

    return response.json()

@app.route("/bling/test", methods=["POST"])
def criar_proposta_bitrix_via_test():
    """
    Cria proposta do Bitrix via endpoint /bling/test POST
    """
    try:
        # Carregar e validar tokens
        tokens = load_tokens()
        if not tokens:
            return {"error": "Tokens não encontrados. Faça a autenticação primeiro."}, 400

        if is_token_expired(tokens):
            tokens = refresh_token()
            if not tokens:
                return {"error": "Não foi possível renovar o token. Refaça a autenticação."}, 401

        access_token = tokens.get("access_token")
        if not access_token:
            return {"error": "Access token não encontrado nos tokens."}, 400

        # Obter dados da requisição
        data = request.get_json()
        if not data:
            return {"error": "Dados não fornecidos"}, 400

        deal = data.get("deal", {})
        empresa = data.get("empresa", {})
        contatos = data.get("contatos", [])
        produtos = data.get("produtos", [])

        # Validar dados obrigatórios
        if not deal or not empresa or not produtos:
            return {"error": "Deal, empresa e produtos são obrigatórios"}, 400

        # Preparar cabeçalhos
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

        # Buscar ou criar contato no Bling
        contato_bling = None
        cnpj = empresa.get('UF_CRM_1713291425', '')
        
        if cnpj:
            # Limpar CNPJ (somente números)
            cnpj_limpo = ''.join(c for c in str(cnpj) if c.isdigit())
            
            # Buscar contato existente
            buscar_url = f"{BLING_API_URL}?filtros[numeroDocumento]={cnpj_limpo}"
            response = requests.get(buscar_url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                if result.get('data') and len(result['data']) > 0:
                    contato_bling = result['data'][0]

        # Se não encontrou contato, usar dados da empresa
        if not contato_bling:
            contato_bling = {
                "id": int(empresa.get('ID', 999999)),  
                "nome": empresa.get('TITLE', 'Cliente do Bitrix'),
                "numeroDocumento": cnpj_limpo if cnpj else ''
            }

        # Preparar itens da proposta
        itens = []
        total = 0
        
        print(f"\n[PROPOSTA-TEST-DEBUG] === PROCESSANDO {len(produtos)} PRODUTOS DO BITRIX ===")
        
        for produto in produtos:
            try:
                # Extrair dados básicos do produto
                quantidade = float(produto.get('QUANTITY', 0))
                preco = float(produto.get('PRICE', 0))
                subtotal = quantidade * preco
                nome_bitrix = produto.get('PRODUCT_NAME', 'Produto sem nome').strip()
                
                print(f"\n[PRODUTO-TEST-DEBUG] === PROCESSANDO PRODUTO {len(itens)+1} ===")
                print(f"[PRODUTO-TEST-DEBUG] Nome original: '{nome_bitrix}'")
                print(f"[PRODUTO-TEST-DEBUG] Quantidade: {quantidade}")
                print(f"[PRODUTO-TEST-DEBUG] Preço: {preco}")
                
                # ETAPA 1: MAPEAR PRODUTO BITRIX PARA CÓDIGO BLING
                produto_mapeado = mapear_produto_para_codigo_bling(nome_bitrix)
                
                if not produto_mapeado:
                    # Produto não encontrado no mapeamento - ERRO CRÍTICO
                    print(f"[ERRO] ❌ Produto do Bitrix sem mapeamento para o Bling: '{nome_bitrix}'")
                    print(f"[ERRO] ⚠️ Proposta não pode ser criada com itens inconsistentes")
                    return {"error": f"Produto não mapeado: '{nome_bitrix}'. Configure o mapeamento primeiro."}, 400
                
                # ETAPA 2: BUSCAR PRODUTO OFICIAL NO BLING 
                codigo_bling = produto_mapeado["codigo"]
                nome_bling_oficial = produto_mapeado["nome"]
                
                print(f"[PRODUTO-TEST-DEBUG] ✅ Mapeamento: {codigo_bling} → '{nome_bling_oficial}'")
                
                # Buscar produto completo no Bling
                produto_bling = buscar_produto_bling_por_codigo(access_token, codigo_bling)
                
                # ETAPA 3: MONTAR ITEM - SEMPRE GARANTIR QUE APAREÇA NO BLING
                if produto_bling and "id" in produto_bling and not produto_bling.get("fallback", False):
                    # Produto encontrado na API - usar ID real
                    item = {
                        "produto": {"id": produto_bling["id"]},
                        "codigo": codigo_bling,
                        "descricao": nome_bling_oficial,
                        "quantidade": quantidade,
                        "valor": preco,
                        "aliquotaIPI": 0,
                        "desconto": 0,
                        "unidade": "UN"
                    }
                    print(f"[PRODUTO-TEST-DEBUG] ✅ Item COM ID do produto (vinculado): {produto_bling['id']}")
                else:
                    # Produto não encontrado na API - usar estrutura que GARANTE aparição no Bling
                    item = {
                        "codigo": codigo_bling,
                        "descricao": nome_bling_oficial,
                        "quantidade": quantidade,
                        "valor": preco,
                        "aliquotaIPI": 0,
                        "desconto": 0,
                        "unidade": "UN"
                    }
                    print(f"[PRODUTO-TEST-DEBUG] ✅ Item SEM ID (não vinculado mas visível): {codigo_bling}")
                
                # Adicionar campos obrigatórios que podem estar faltando
                if "desconto" not in item:
                    item["desconto"] = 0
                if "aliquotaIPI" not in item:
                    item["aliquotaIPI"] = 0
                if "unidade" not in item:
                    item["unidade"] = "UN"
                
                print(f"[PRODUTO-TEST-DEBUG] 📦 Estrutura final completa:")
                for campo, valor in item.items():
                    print(f"[PRODUTO-TEST-DEBUG]    {campo}: {valor}")
                
                itens.append(item)
                total += subtotal
                
                print(f"[PRODUTO-TEST-DEBUG] ✅ Item adicionado! Array agora tem {len(itens)} item(s)")
                
            except (ValueError, TypeError) as e:
                print(f"[ERRO] Erro ao processar produto: {produto}, erro: {e}")
                continue

        if not itens:
            return {"error": "Nenhum produto válido encontrado"}, 400

        print(f"\n[PROPOSTA-TEST-DEBUG] === PAYLOAD FINAL ===")
        # Preparar payload da proposta
        payload = {
            "numero": f"BITRIX_{deal.get('ID')}",
            "descricao": deal.get('TITLE', 'Proposta do Bitrix'),
            "contato": {
                "id": contato_bling['id']
            },
            "dataPropostaComoFinal": datetime.now().strftime("%Y-%m-%d"),
            "itens": itens,
            "total": total,
            "observacoes": f"Proposta gerada automaticamente do Bitrix - Deal ID: {deal.get('ID')}"
        }
        
        print(f"[PROPOSTA-TEST-DEBUG] Payload completo:")
        print(f"[PROPOSTA-TEST-DEBUG] {payload}")

        # Criar proposta no Bling
        response = requests.post(
            BLING_PROPOSTAS_URL,
            headers=headers,
            json=payload,
            timeout=30
        )

        if response.status_code in [200, 201]:
            proposta_data = response.json()
            print(f"[PROPOSTA-TEST-DEBUG] ✅ Proposta criada com sucesso no Bling!")
            print(f"[PROPOSTA-TEST-DEBUG] ID da proposta: {proposta_data.get('data', {}).get('id')}")
            return {
                "sucesso": True,
                "data": proposta_data.get('data'),
                "id": proposta_data.get('data', {}).get('id'),
                "numero": payload['numero'],
                "total": total,
                "itens_count": len(itens)
            }
        else:
            print(f"[PROPOSTA-TEST-DEBUG] ❌ Erro ao criar proposta no Bling: {response.status_code}")
            print(f"[PROPOSTA-TEST-DEBUG] Detalhes: {response.text}")
            return {
                "error": f"Erro ao criar proposta no Bling: {response.status_code}",
                "details": response.text
            }, response.status_code

    except Exception as e:
        return {"error": f"Erro interno: {str(e)}"}, 500

@app.route("/bling/propostas", methods=["GET"])
def bling_propostas():
    tokens = load_tokens()
    if not tokens:
        return {"error": "Tokens não encontrados. Faça a autenticação primeiro."}, 400

    access_token = tokens.get("access_token")
    if not access_token:
        return {"error": "Access token não encontrado nos tokens."}, 400

    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(BLING_PROPOSTAS_URL, headers=headers, timeout=30)

    if response.status_code != 200:
        return {"status": response.status_code, "body": response.text}, response.status_code

    return response.json()

@app.route("/bling/propostas/criar", methods=["POST"])
def bling_criar_proposta():
    """Cria uma proposta comercial no Bling - seguindo padrão simples"""
    tokens = load_tokens()
    if not tokens:
        return {"error": "Tokens não encontrados. Faça a autenticação primeiro."}, 400

    if is_token_expired(tokens):
        tokens = refresh_token()
        if not tokens:
            return {"error": "Não foi possível renovar o token. Refaça a autenticação."}, 401

    access_token = tokens.get("access_token")
    if not access_token:
        return {"error": "Access token não encontrado nos tokens."}, 400

    # Obter dados da requisição
    dados = request.get_json()
    if not dados:
        return {"error": "Dados não fornecidos"}, 400

    # Preparar headers
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    # Enviar para API Bling
    response = requests.post(BLING_PROPOSTAS_URL, headers=headers, json=dados, timeout=30)

    if response.status_code not in [200, 201]:
        return {"status": response.status_code, "body": response.text}, response.status_code

    return response.json()

@app.route("/propostas-comerciais", methods=["GET"])
def listar_propostas_comerciais():
    tokens = load_tokens()
    if not tokens:
        return {"error": "Tokens não encontrados. Faça a autenticação primeiro."}, 400

    if is_token_expired(tokens):
        tokens = refresh_token()
        if not tokens:
            return {"error": "Não foi possível renovar o token. Refaça a autenticação."}, 401

    access_token = tokens.get("access_token")
    if not access_token:
        return {"error": "Access token não encontrado nos tokens."}, 400

    headers = {"Authorization": f"Bearer {access_token}"}
    
    # Obter lista resumida de propostas com limite de 10 por página
    params = {"limite": 10, "pagina": 1}
    response = requests.get(BLING_PROPOSTAS_URL, headers=headers, params=params, timeout=60)

    if response.status_code != 200:
        return {"status": response.status_code, "body": response.text}, response.status_code

    data = response.json()
    propostas_resumidas = data.get("data", [])
    
    # Obter dados completos de cada proposta (máximo 10 requisições)
    propostas_completas = []
    for proposta in propostas_resumidas[:10]:
        proposta_id = proposta.get("id")
        if proposta_id:
            url_detalhes = f"{BLING_PROPOSTAS_URL}/{proposta_id}"
            try:
                response_detalhes = requests.get(url_detalhes, headers=headers, timeout=30)
                if response_detalhes.status_code == 200:
                    propostas_completas.append(response_detalhes.json())
                else:
                    propostas_completas.append(proposta)
            except Exception as e:
                propostas_completas.append(proposta)
    
    return {"data": propostas_completas}

@app.route("/propostas-comerciais", methods=["POST"])  
def criar_proposta_via_post():
    """
    Cria uma proposta comercial no Bling a partir de dados do Bitrix
    FLUXO CORRETO:
    1. GET /contatos?numeroDocumento=... para buscar contato existente
    2. POST /contatos para criar contato se não existir
    3. POST /propostas-comerciais para criar a proposta
    """
    try:
        print(f"\n{'='*60}")
        print(f"[PROPOSTA] === CRIANDO PROPOSTA COMERCIAL ===")
        print(f"{'='*60}")
        
        # Carregar e validar tokens
        tokens = load_tokens()
        if not tokens:
            return {"error": "Tokens não encontrados. Faça a autenticação primeiro."}, 400

        if is_token_expired(tokens):
            tokens = refresh_token()
            if not tokens:
                return {"error": "Não foi possível renovar o token. Refaça a autenticação."}, 401

        access_token = tokens.get("access_token")
        if not access_token:
            return {"error": "Access token não encontrado nos tokens."}, 400

        # Obter dados da requisição
        data = request.get_json()
        if not data:
            return {"error": "Dados não fornecidos"}, 400

        deal = data.get("deal", {})
        empresa = data.get("empresa", {})
        produtos = data.get("produtos", [])
        vendedor_info = data.get("vendedor", {})

        print(f"[PROPOSTA] Deal ID: {deal.get('ID')} - {deal.get('TITLE')}")
        print(f"[PROPOSTA] Empresa: {empresa.get('TITLE')}")
        print(f"[PROPOSTA] CNPJ: {empresa.get('UF_CRM_1713291425', 'N/A')}")
        print(f"[PROPOSTA] Produtos: {len(produtos)}")
        print(f"[PROPOSTA] Vendedor: {vendedor_info.get('nome', 'N/A')}")

        # Validar dados obrigatórios
        if not deal or not empresa or not produtos:
            return {"error": "Deal, empresa e produtos são obrigatórios"}, 400

        # PASSO 1: Determinar vendedor no Bling - SEMPRE usar o responsável do deal
        print(f"\n[PROPOSTA] === PASSO 1: DETERMINAR VENDEDOR ===")
        
        # Buscar o responsável correto do deal no Bitrix
        deal_id = deal.get('ID')
        assigned_by_id = deal.get('ASSIGNED_BY_ID')
        
        print(f"[PROPOSTA] 🔍 Buscando responsável correto do deal {deal_id}...")
        print(f"[PROPOSTA] 📋 ASSIGNED_BY_ID: {assigned_by_id}")
        
        # 🎯 BUSCAR NOME DO REPRESENTANTE: CACHE PRIMEIRO, depois MAPA, depois N/A
        assigned_by_id_str = str(assigned_by_id) if assigned_by_id else ''
        nome_responsavel = None

        # Prioridade 1: Cache de usuários Bitrix (mais completo)
        if CACHE_AVAILABLE and CACHE_MANAGER and assigned_by_id:
            nome_responsavel = CACHE_MANAGER.get_user_name(assigned_by_id)
            if nome_responsavel:
                print(f"[PROPOSTA] ✅ Representante encontrado no CACHE: '{nome_responsavel}' (ID: {assigned_by_id_str})")

        # Prioridade 2: MAPA estático (fallback)
        if not nome_responsavel:
            nome_responsavel = MAPA_NOMES_REPRESENTANTES.get(assigned_by_id_str)
            if nome_responsavel:
                print(f"[PROPOSTA] ✅ Representante encontrado no MAPA: '{nome_responsavel}' (ID: {assigned_by_id_str})")

        if not nome_responsavel:
            print(f"[PROPOSTA] ⚠️ ID {assigned_by_id_str} não encontrado no cache nem no mapa")
            nome_responsavel = 'N/A'
        vendedor_dados = resolver_vendedor_bling(access_token, deal, {"nome": nome_responsavel})
        vendedor_id = vendedor_dados.get('id') if vendedor_dados else None
        vendedor_nome = vendedor_dados.get('nome') if vendedor_dados else None
        print(f"[PROPOSTA] 🎯 Vendedor final escolhido: ID {vendedor_id} ('{vendedor_nome}')")

        # 🔥 ADICIONAR responsavel_representante ao dicionário empresa ANTES de passar
        empresa['responsavel_representante'] = nome_responsavel
        print(f"[PROPOSTA] ✅ Adicionado responsavel_representante à empresa: '{nome_responsavel}'")

        # PASSO 2: Buscar ou criar contato no Bling

        # PASSO 2: Buscar ou criar contato no Bling
        print(f"\n[PROPOSTA] === PASSO 2: BUSCAR/CRIAR CONTATO ===")
        contato_bling = buscar_ou_criar_contato_bling(access_token, empresa, vendedor_id)
        
        if not contato_bling:
            print(f"[PROPOSTA] ❌ Falha ao obter contato no Bling")
            return {"error": "Não foi possível criar ou encontrar contato no Bling"}, 400

        contato_id = contato_bling.get('id')
        contato_nome = contato_bling.get('nome', 'N/A')
        contato_criado = contato_bling.get('criado', False)
        
        print(f"[PROPOSTA] ✅ Contato obtido: ID {contato_id} - {contato_nome}")
        if contato_criado:
            print(f"[PROPOSTA] 📝 Contato foi CRIADO nesta operação")
        else:
            print(f"[PROPOSTA] 🔍 Contato já existia no Bling")

        # PASSO 3: Preparar itens da proposta
        print(f"\n[PROPOSTA] === PASSO 3: PROCESSAR PRODUTOS ===")
        itens = []
        total = 0
        
        for idx, produto in enumerate(produtos):
            try:
                quantidade = float(produto.get('QUANTITY', 0))
                preco = float(produto.get('PRICE', 0))
                nome_produto = produto.get('PRODUCT_NAME', 'Produto sem nome')
                
                # ⚠️  IMPORTANTE: NÃO filtrar por preço! Produtos com preço 0 são válidos
                # Validar APENAS quantidade
                if quantidade <= 0:
                    print(f"[PROPOSTA] ⚠️ Produto ignorado (qtd=0): {nome_produto}")
                    continue
                
                # ESTRUTURA CORRETA PARA API v3 DO BLING (PROPOSTA)
                item = {
                    "codigo": f"BITRIX_{produto.get('PRODUCT_ID', idx+1)}",  # Código obrigatório 
                    "descricao": nome_produto.strip(),                       # Nome do produto
                    "quantidade": quantidade,                                # Quantidade
                    "valor": preco,                                         # Preço unitário (pode ser 0)
                    "aliquotaIPI": 0,                                       # IPI (obrigatório)
                    "desconto": 0,                                          # Desconto (obrigatório)
                    "unidade": "UN"                                         # Unidade (obrigatório)
                }
                itens.append(item)
                subtotal = quantidade * preco
                total += subtotal
                
                print(f"[PROPOSTA] ✅ Item {idx+1}: {nome_produto} ({quantidade}x R${preco:.2f} = R${subtotal:.2f})")
                
            except (ValueError, TypeError) as e:
                print(f"[PROPOSTA] ❌ Erro ao processar produto {idx+1}: {e}")
                continue

        if not itens:
            return {"error": "Nenhum produto válido encontrado"}, 400

        print(f"[PROPOSTA] 📊 Total de itens: {len(itens)} - Valor: R$ {total:.2f}")

        # PASSO 4: Montar payload da proposta
        print(f"\n[PROPOSTA] === PASSO 4: CRIAR PROPOSTA ===")
        nome_empresa = empresa.get('TITLE', 'Cliente')
        
        # 🎯 USAR O NOME DO VENDEDOR JÁ RESOLVIDO (vendedor_nome)
        aos_cuidados = vendedor_nome if vendedor_nome else empresa.get('UF_CRM_1721072755', 'Representante')
        
        print(f"[PROPOSTA] 👤 Responsável/Aos Cuidados: '{aos_cuidados}' (Vendedor: {vendedor_nome})")
        
        payload = {
            "data": datetime.now().strftime("%Y-%m-%d"),
            "contato": {"id": contato_id},
            "aosCuidadosDe": aos_cuidados,
            "itens": itens,
            "desconto": {"valor": 0, "unidade": "REAL"},
            "outrasDespesas": 0,
            "observacoes": "",
            "descricaoPadrao": {
                "condicoes": "",
                "introducao": f"Proposta comercial para {nome_empresa}",
                "saudacao": "Atenciosamente,",
                "departamento": "Departamento de vendas"
            }
        }
        
        # INCLUIR VENDEDOR APENAS SE ENCONTRADO - Bling API v3 só aceita ID numérico
        if vendedor_id:
            payload["vendedor"] = {"id": vendedor_id}
            print(f"[PROPOSTA] ✅ Vendedor incluído: ID {vendedor_id} ('{vendedor_nome}')")
        else:
            print(f"[PROPOSTA] ⚠️ Vendedor não encontrado - campo omitido do payload")
        
        print(f"[PROPOSTA] Payload montado:")
        print(f"   - Contato ID: {contato_id}")
        print(f"   - Vendedor ID: {vendedor_id if vendedor_id else 'OMITIDO'}")
        print(f"   - Aos cuidados: {aos_cuidados}")
        print(f"   - Itens: {len(itens)}")

        # PASSO 5: Enviar para API Bling
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        url = f"{BLING_API_BASE}/propostas-comerciais"
        print(f"[PROPOSTA] 🌐 Enviando POST para: {url}")
        
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        
        print(f"[PROPOSTA] 📥 Resposta: Status {response.status_code}")

        if response.status_code not in [200, 201]:
            error_text = response.text
            print(f"[PROPOSTA] ❌ Erro: {error_text}")
            return {
                "error": f"Erro {response.status_code} ao criar proposta no Bling",
                "details": error_text,
                "debug": {
                    "contato_id": contato_id,
                    "vendedor_id": vendedor_id,
                    "itens_count": len(itens)
                }
            }, response.status_code

        # SUCESSO
        proposta_data = response.json()
        proposta_id = proposta_data.get('data', {}).get('id')
        
        print(f"\n{'='*60}")
        print(f"[PROPOSTA] ✅ SUCESSO! Proposta criada com ID: {proposta_id}")
        print(f"[PROPOSTA] Cliente: {contato_nome}")
        print(f"[PROPOSTA] Valor total: R$ {total:.2f}")
        print(f"{'='*60}")
        
        return {
            "sucesso": True,
            "id": proposta_id,
            "data": proposta_data.get('data'),
            "cliente": {
                "id": contato_id,
                "nome": contato_nome,
                "criado": contato_criado
            },
            "vendedor_id": vendedor_id,
            "total": round(total, 2),
            "itens_count": len(itens),
            "deal_id": deal.get('ID')
        }

    except Exception as e:
        import traceback
        print(f"[PROPOSTA] 💥 Exceção: {e}")
        traceback.print_exc()
        return {"error": f"Erro interno: {str(e)}"}, 500

@app.route("/propostas-comerciais/<int:idPropostaComercial>", methods=["GET"])
def obter_proposta_comercial(idPropostaComercial):
    tokens = load_tokens()
    if not tokens:
        return {"error": "Tokens não encontrados. Faça a autenticação primeiro."}, 400

    if is_token_expired(tokens):
        tokens = refresh_token()
        if not tokens:
            return {"error": "Não foi possível renovar o token. Refaça a autenticação."}, 401

    access_token = tokens.get("access_token")
    if not access_token:
        return {"error": "Access token não encontrado nos tokens."}, 400

    url = f"{BLING_PROPOSTAS_URL}/{idPropostaComercial}"
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(url, headers=headers, timeout=30)

    if response.status_code != 200:
        return {"status": response.status_code, "body": response.text}, response.status_code

    return response.json()

@app.route("/propostas-comerciais/numero/<int:numeroProposta>", methods=["GET"])
def obter_proposta_por_numero(numeroProposta):
    tokens = load_tokens()
    if not tokens:
        return {"error": "Tokens não encontrados. Faça a autenticação primeiro."}, 400

    if is_token_expired(tokens):
        tokens = refresh_token()
        if not tokens:
            return {"error": "Não foi possível renovar o token. Refaça a autenticação."}, 401

    access_token = tokens.get("access_token")
    if not access_token:
        return {"error": "Access token não encontrado nos tokens."}, 400

    headers = {"Authorization": f"Bearer {access_token}"}
    
    # Buscar propostas e filtrar pelo número
    params = {"limite": 100, "pagina": 1}
    response = requests.get(BLING_PROPOSTAS_URL, headers=headers, params=params, timeout=30)

    if response.status_code != 200:
        return {"status": response.status_code, "body": response.text}, response.status_code

    data = response.json()
    propostas = data.get("data", [])
    
    # Encontrar proposta com o número especificado
    for proposta in propostas:
        if proposta.get("numero") == numeroProposta:
            proposta_id = proposta.get("id")
            url_detalhes = f"{BLING_PROPOSTAS_URL}/{proposta_id}"
            try:
                response_detalhes = requests.get(url_detalhes, headers=headers, timeout=30)
                if response_detalhes.status_code == 200:
                    return response_detalhes.json()
            except:
                pass
    
    return {"error": f"Proposta com número {numeroProposta} não encontrada"}, 404

@app.route("/propostas-comerciais/numero/<int:numeroProposta>/dados-completos", methods=["GET"])
def obter_proposta_dados_completos(numeroProposta):
    tokens = load_tokens()
    if not tokens:
        return {"error": "Tokens não encontrados. Faça a autenticação primeiro."}, 400

    if is_token_expired(tokens):
        tokens = refresh_token()
        if not tokens:
            return {"error": "Não foi possível renovar o token. Refaça a autenticação."}, 401

    access_token = tokens.get("access_token")
    if not access_token:
        return {"error": "Access token não encontrado nos tokens."}, 400

    headers = {"Authorization": f"Bearer {access_token}"}
    
    # PASSO 1: Buscar proposta pelo número (lista resumida)
    params = {"limite": 100, "pagina": 1}
    response = requests.get(BLING_PROPOSTAS_URL, headers=headers, params=params, timeout=30)

    if response.status_code != 200:
        return {"error": "Erro ao buscar propostas", "status": response.status_code}, response.status_code

    data = response.json()
    propostas = data.get("data", [])
    
    proposta_resumida = None
    for proposta in propostas:
        if proposta.get("numero") == numeroProposta:
            proposta_resumida = proposta
            break
    
    if not proposta_resumida:
        return {"error": f"Proposta com número {numeroProposta} não encontrada"}, 404
    
    # PASSO 2: Buscar detalhes completos da proposta
    proposta_id = proposta_resumida.get("id")
    if not proposta_id:
        return {"error": "ID da proposta não encontrado"}, 400
    
    url_detalhes = f"{BLING_PROPOSTAS_URL}/{proposta_id}"
    response_detalhes = requests.get(url_detalhes, headers=headers, timeout=30)
    
    if response_detalhes.status_code != 200:
        return {"error": "Erro ao buscar detalhes da proposta", "status": response_detalhes.status_code}, response_detalhes.status_code
    
    proposta_completa = response_detalhes.json().get("data", {})
    
    # PASSO 3: Extrair ID do contato da proposta
    contato_info = proposta_completa.get("contato", {})
    contato_id = None
    
    if isinstance(contato_info, dict):
        contato_id = contato_info.get("id")
    
    if not contato_id:
        return {"error": "ID do contato não encontrado na proposta"}, 400
    
    # PASSO 4: Buscar dados completos do cliente usando o ID do contato
    contato_url = f"{BLING_API_URL}/{contato_id}"
    response_contato = requests.get(contato_url, headers=headers, timeout=30)
    
    if response_contato.status_code != 200:
        return {"error": "Erro ao buscar contato", "status": response_contato.status_code}, response_contato.status_code
    
    cliente = response_contato.json().get("data", {})
    
    # PASSO 5: Extrair e formatar dados
    endereco_info = cliente.get("endereco", {})
    endereco_geral = endereco_info.get("geral", {})
    
    cnpj_contratante = cliente.get("numeroDocumento", "N/A")
    
    # Formatar CNPJ se necessário
    if cnpj_contratante != "N/A" and len(cnpj_contratante) == 14:
        cnpj_contratante = f"{cnpj_contratante[:2]}.{cnpj_contratante[2:5]}.{cnpj_contratante[5:8]}/{cnpj_contratante[8:12]}-{cnpj_contratante[12:14]}"
    
    # Formatar CEP
    cep = endereco_geral.get("cep", "N/A")
    if cep != "N/A" and len(cep) == 8:
        cep = f"{cep[:5]}-{cep[5:]}"
    
    # PASSO 6: Extrair itens da proposta (quantidade, preço unitário, preço total)
    itens = proposta_completa.get("itens", [])
    itens_extraidos = []
    soma_quantidades = 0
    
    for item in itens:
        # Tentar extrair preço unitário de diferentes campos possíveis
        preco_unitario = (
            item.get("valor_unitario") or 
            item.get("valorUnitario") or 
            item.get("preco") or 
            item.get("valor") or 
            0
        )
        
        # Tentar extrair preço total de diferentes campos possíveis
        preco_total = (
            item.get("total") or 
            item.get("valorTotal") or 
            item.get("valor_total") or 
            0
        )
        
        # Extrair quantidade
        quantidade = item.get("quantidade", 0)
        soma_quantidades += quantidade
        
        item_info = {
            "descricao": item.get("descricao", "N/A"),
            "quantidade": quantidade,
            "preco_unitario": preco_unitario,
            "preco_total": preco_total
        }
        itens_extraidos.append(item_info)
    
    # PASSO 7: Extrair total da proposta
    total_proposta = proposta_completa.get("total", 0)
    
    # PASSO 8: Extrair parcelas
    parcelas = proposta_completa.get("parcelas", [])
    
    # Se não encontrar em "parcelas", tentar em "condicoesPagamento"
    if not parcelas:
        condicoes_pagamento = proposta_completa.get("condicoesPagamento", {})
        if isinstance(condicoes_pagamento, dict):
            parcelas = condicoes_pagamento.get("parcelas", [])
    
    quantidade_parcelas = len(parcelas)
    parcelas_extraidas = []
    primeira_data_parcela = "N/A"
    valor_primeira_parcela = 0
    
    for i, parcela in enumerate(parcelas):
        # Tentar encontrar a data em diferentes campos possíveis
        data_parcela = (
            parcela.get("data") or 
            parcela.get("dataVencimento") or 
            parcela.get("dataParcela") or 
            parcela.get("dataVencimentoParcela") or
            "N/A"
        )
        
        # Tentar encontrar dias em diferentes campos possíveis
        dias_parcela = (
            parcela.get("dias") or 
            parcela.get("numeroDias") or 
            0
        )
        
        valor_parcela = parcela.get("valor", 0)
        
        parcela_info = {
            "dias": dias_parcela,
            "data": data_parcela,
            "valor": valor_parcela
        }
        parcelas_extraidas.append(parcela_info)
        
        # Extrair primeira data e valor de parcela
        if i == 0 and data_parcela != "N/A":
            primeira_data_parcela = data_parcela
            valor_primeira_parcela = valor_parcela
    
    # PASSO 9: Extrair nome do vendedor
    vendedor_info = proposta_completa.get("vendedor", {})
    nome_vendedor = "N/A"
    debug_vendedor = {}
    
    # Tentar extrair nome diretamente do objeto vendedor
    if isinstance(vendedor_info, dict):
        debug_vendedor["vendedor_info"] = vendedor_info
        # Se o vendedor tem um campo "nome", usar diretamente
        if "nome" in vendedor_info:
            nome_vendedor = vendedor_info.get("nome", "N/A")
        else:
            # Caso contrário, tentar buscar pelo ID na rota de vendedores
            vendedor_id = vendedor_info.get("id")
            debug_vendedor["vendedor_id"] = vendedor_id
            if vendedor_id and vendedor_id != 0:
                try:
                    vendedor_url = f"{BLING_VENDEDORES_URL}/{vendedor_id}"
                    debug_vendedor["vendedor_url"] = vendedor_url
                    response_vendedor = requests.get(vendedor_url, headers=headers, timeout=30)
                    debug_vendedor["response_status"] = response_vendedor.status_code
                    if response_vendedor.status_code == 200:
                        vendedor_data = response_vendedor.json().get("data", {})
                        debug_vendedor["vendedor_data"] = vendedor_data
                        # O nome está em contato.nome, não em data.nome
                        contato_vendedor = vendedor_data.get("contato", {})
                        nome_vendedor = contato_vendedor.get("nome", "N/A")
                except Exception as e:
                    debug_vendedor["error"] = str(e)
                    nome_vendedor = "N/A"
    
    # Formatar total da proposta com separador de milhares
    total_proposta_formatado = f"{total_proposta:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    
    # FIX: Extrair PRIMEIRO item (sem filtrar por preço)
    # ⚠️  IMPORTANTE: Não filtrar por preco_unitario > 0
    # Produtos com preço zero são válidos (ex: RF05RF pode vir com preço 0)
    quantidade_recarga = 0
    valor_unitario_recarga = 0
    if itens_extraidos and len(itens_extraidos) > 0:
        primeiro_item = itens_extraidos[0]
        quantidade_recarga = primeiro_item.get('quantidade', 0)
        valor_unitario_recarga = primeiro_item.get('preco_unitario', 0)
    
    resultado = {
        "NUMERO_PROPOSTA": numeroProposta,
        "CNPJ_CONTRATANTE": cnpj_contratante,
        "NOME_CONTRATANTE": cliente.get("nome", "N/A"),
        "ENDERECO_CONTRATANTE": f"{endereco_geral.get('endereco', '')} {endereco_geral.get('numero', '')} {endereco_geral.get('complemento', '')}".strip(),
        "BAIRRO_CONTRATANTE": endereco_geral.get('bairro', ''),
        "MUNICIPIO_CONTRATANTE": endereco_geral.get('municipio', ''),
        "UF_CONTRATANTE": endereco_geral.get('uf', ''),  # ✅ UF SEPARADO
        "CEP_CONTRATANTE": cep,
        "CIDADE_CONTRATANTE": f"{endereco_geral.get('municipio', '')}/{endereco_geral.get('uf', '')}",  # Mantém compatibilidade
        "NOME_VENDEDOR": nome_vendedor,
        "PRIMEIRA_DATA_PARCELA": primeira_data_parcela,
        "ITENS": itens_extraidos,
        "SOMA_QUANTIDADES": soma_quantidades,
        "QUANTIDADE_RECARGA": quantidade_recarga,
        "VALOR_UNITARIO_RECARGA": valor_unitario_recarga,
        "TOTAL_PROPOSTA": total_proposta_formatado,
        "QUANTIDADE_PARCELAS": quantidade_parcelas,
        "PARCELAS": parcelas_extraidas,
        "dados_brutos": cliente,
        "DEBUG_PROPOSTA_COMPLETA": proposta_completa,
        "DEBUG_VENDEDOR": debug_vendedor
    }
    
    return resultado


@app.route("/propostas-comerciais/criar", methods=["POST"])
def criar_proposta_comercial_separada():
    """Cria uma nova proposta comercial no Bling - Rota separada"""
    try:
        if not request.is_json:
            return jsonify({
                "sucesso": False,
                "erro": "Content-Type deve ser application/json"
            }), 400
        
        dados_proposta = request.get_json()
        
        if "cliente" not in dados_proposta:
            return jsonify({
                "sucesso": False,
                "erro": "Campo 'cliente' é obrigatório"
            }), 400
        
        if "itens" not in dados_proposta or not dados_proposta["itens"]:
            return jsonify({
                "sucesso": False,
                "erro": "Campo 'itens' é obrigatório e deve ter pelo menos 1 item"
            }), 400
        
        tokens = load_tokens()
        if not tokens:
            return jsonify({
                "sucesso": False,
                "erro": "Tokens não encontrados. Faça a autenticação primeiro."
            }), 401
        
        if is_token_expired(tokens):
            tokens = refresh_token()
            if not tokens:
                return jsonify({
                    "sucesso": False,
                    "erro": "Não foi possível renovar o token. Refaça a autenticação."
                }), 401
        
        access_token = tokens.get("access_token")
        if not access_token:
            return jsonify({
                "sucesso": False,
                "erro": "Access token não encontrado nos tokens."
            }), 401
        
        payload = _preparar_payload_proposta(dados_proposta)
        
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        response = requests.post(
            BLING_PROPOSTAS_URL,
            json=payload,
            headers=headers,
            timeout=30
        )
        
        if response.status_code not in [200, 201]:
            return jsonify({
                "sucesso": False,
                "erro": f"Erro ao criar proposta no Bling",
                "status_code": response.status_code,
                "detalhes": response.text
            }), response.status_code
        
        resultado_bling = response.json()
        proposta_criada = resultado_bling.get("data", {})
        
        return jsonify({
            "sucesso": True,
            "mensagem": "Proposta criada com sucesso",
            "proposta_id": proposta_criada.get("id"),
            "numero_proposta": proposta_criada.get("numero"),
            "proposta_completa": proposta_criada
        }), 201
    
    except Exception as e:
        return jsonify({
            "sucesso": False,
            "erro": f"Erro ao criar proposta: {str(e)}"
        }), 500


@app.route('/bling/criar-proposta', methods=['POST'])
def criar_proposta():
    """
    Cria uma proposta comercial no Bling via API Bling v3
    
    POST /bling/criar-proposta
    Content-Type: application/json
    
    Payload:
    {
        "contato": {"id": 123},
        "loja": {"id": 456, "unidadeNegocio": {"id": 789}},
        "itens": [{"produto": {"id": 101}, "quantidade": 1, "valor": 100}]
    }
    """
    
    try:
        if not request.is_json:
            return jsonify({'erro': 'Content-Type deve ser application/json'}), 400
        
        dados = request.get_json()
        
        # Validar campos obrigatórios
        if 'contato' not in dados or 'id' not in dados.get('contato', {}):
            return jsonify({'erro': 'Campo contato.id é obrigatório'}), 400
        
        if 'loja' not in dados or 'id' not in dados.get('loja', {}):
            return jsonify({'erro': 'Campo loja.id é obrigatório'}), 400
        
        if 'itens' not in dados or not isinstance(dados['itens'], list):
            return jsonify({'erro': 'Campo itens é obrigatório e deve ser um array'}), 400
        
        # Obter token
        tokens = load_tokens()
        if not tokens:
            return jsonify({'erro': 'Não autenticado'}), 401
        
        if is_token_expired(tokens):
            if not refresh_token():
                return jsonify({'erro': 'Token expirado e renovação falhou'}), 401
            tokens = load_tokens()
        
        access_token = tokens.get('access_token')
        if not access_token:
            return jsonify({'erro': 'Token inválido'}), 401
        
        # Fazer requisição para API Bling
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        resposta = requests.post(
            f'{BLING_API_BASE}/propostas-comerciais',
            json=dados,
            headers=headers,
            timeout=30
        )
        
        if resposta.status_code in [200, 201]:
            resultado = resposta.json()
            return jsonify({'sucesso': True, 'data': resultado.get('data', resultado)}), resposta.status_code
        else:
            return jsonify({'erro': 'Erro ao criar proposta', 'detalhes': resposta.text}), resposta.status_code
    
    except Exception as e:
        return jsonify({'erro': str(e)}), 500


def _preparar_payload_proposta(dados: dict) -> dict:
    """
    Prepara o payload para envio à API Bling
    Formata os dados conforme esperado pela API
    """
    
    # Data padrão é hoje
    data_proposta = dados.get("data", datetime.now().strftime("%Y-%m-%d"))
    
    # Data de validade padrão é 30 dias depois
    if "dataValidade" not in dados:
        data_validade = (
            datetime.strptime(data_proposta, "%Y-%m-%d") + timedelta(days=30)
        ).strftime("%Y-%m-%d")
    else:
        data_validade = dados.get("dataValidade")
    
    # Preparar itens
    itens_formatados = []
    for item in dados.get("itens", []):
        item_formatado = {
            "descricao": item.get("descricao", ""),
            "quantidade": float(item.get("quantidade", 1)),
            "valor": float(item.get("valor", 0)),
        }
        
        # Campos opcionais do item
        if "desconto" in item:
            item_formatado["desconto"] = float(item.get("desconto", 0))
        
        if "produto_id" in item:
            item_formatado["produto"] = {
                "id": item.get("produto_id")
            }
        
        itens_formatados.append(item_formatado)
    
    # Preparar cliente
    cliente = dados.get("cliente", {})
    cliente_formatado = {}
    
    if "id" in cliente:
        # Se tem ID, usar apenas o ID
        cliente_formatado["id"] = cliente.get("id")
    else:
        # Se não tem ID, enviar dados para criar/buscar cliente
        cliente_formatado = {
            "nome": cliente.get("nome", ""),
            "cnpj": cliente.get("cnpj", ""),
            "cpf": cliente.get("cpf", ""),
            "email": cliente.get("email", ""),
            "telefone": cliente.get("telefone", "")
        }
    
    # Montar payload base
    payload = {
        "data": data_proposta,
        "dataValidade": data_validade,
        "cliente": cliente_formatado,
        "itens": itens_formatados,
        "status": dados.get("status", "ABERTA")
    }
    
    # Adicionar campos opcionais se fornecidos
    if "numero" in dados:
        payload["numero"] = str(dados.get("numero"))
    
    if "descricao" in dados:
        payload["descricao"] = dados.get("descricao")
    
    if "observacoes" in dados:
        payload["observacoes"] = dados.get("observacoes")
    
    if "desconto_total" in dados:
        payload["desconto"] = float(dados.get("desconto_total", 0))
    
    if "frete" in dados:
        payload["frete"] = float(dados.get("frete", 0))
    
    if "vendedor_id" in dados:
        payload["vendedor"] = {
            "id": dados.get("vendedor_id")
        }
    
    # Condições de pagamento
    if "condicoes_pagamento" in dados:
        condicoes = dados.get("condicoes_pagamento", {})
        payload["condicoesPagamento"] = {
            "tipo": condicoes.get("tipo", "VISTA"),
            "parcelas": condicoes.get("parcelas", 1),
            "diasPrimeiraParcela": condicoes.get("dias_primeira_parcela", 0)
        }
    
    return payload


@app.route("/bling/proposta", methods=["POST"])
def criar_proposta_bitrix_bling():
    """
    Cria uma proposta comercial no Bling a partir de dados do Bitrix
    FORMATO CORRETO API v3 BLING + ANTI-DUPLICATA
    """
    try:
        # LOG DE DETECÇÃO DE MÚLTIPLAS CHAMADAS
        request_timestamp = time.time()
        client_ip = request.remote_addr
        user_agent = request.headers.get('User-Agent', 'N/A')
        
        print(f"\n[BITRIX→BLING] === NOVA REQUISIÇÃO ===")
        print(f"[BITRIX→BLING] 🕒 Timestamp: {request_timestamp}")
        print(f"[BITRIX→BLING] 🌐 IP Cliente: {client_ip}")
        print(f"[BITRIX→BLING] 🔧 User-Agent: {user_agent[:100]}...")
        
        print(f"[BLING] === INICIANDO CRIAÇÃO DE PROPOSTA ===")
        
        # Carregar e validar tokens - MODO FALLBACK HABILITADO
        tokens = load_tokens()
        if not tokens:
            print(f"[BLING] ⚠️ Tokens não encontrados - usando modo fallback")
            access_token = "FALLBACK_TOKEN"  # Token fictício para modo fallback
        else:
            if is_token_expired(tokens):
                print(f"[BLING] ⏱️ Token expirado - tentando renovar...")
                tokens = refresh_token()
                if not tokens:
                    print(f"[BLING] ⚠️ Renovação falhou - usando modo fallback")
                    access_token = "FALLBACK_TOKEN"  # Token fictício para modo fallback
                else:
                    access_token = tokens.get("access_token")
            else:
                access_token = tokens.get("access_token")
                
        # Verificar se token é válido fazendo uma requisição teste
        if access_token and access_token != "FALLBACK_TOKEN":
            try:
                test_headers = {"Authorization": f"Bearer {access_token}"}
                test_response = requests.get(f"{BLING_API_BASE}/contatos?limite=1", 
                                           headers=test_headers, timeout=5)
                if test_response.status_code == 401:
                    print(f"[BLING] ⚠️ Token inválido (401) - ativando modo fallback")
                    access_token = "FALLBACK_TOKEN"
                elif test_response.status_code == 403:
                    print(f"[BLING] ⚠️ Permissões insuficientes (403) - continuando com fallback para produtos")
                    # Token válido mas com escopo limitado - continuar
                else:
                    print(f"[BLING] ✅ Token válido (status: {test_response.status_code})")
            except Exception as e:
                print(f"[BLING] ⚠️ Erro ao testar token: {e} - usando modo fallback")
                access_token = "FALLBACK_TOKEN"
            
        if access_token == "FALLBACK_TOKEN":
            print(f"[BLING] 🔄 Modo FALLBACK ativado - funcionalidades limitadas mas funcionais")

        # Obter dados da requisição
        data = request.get_json()
        if not data:
            print(f"[BLING] ❌ ERRO: Nenhum dado fornecido na requisição")
            return {"error": "Dados não fornecidos"}, 400

        deal = data.get("deal", {})
        empresa = data.get("empresa", {})
        produtos = data.get("produtos", [])
        vendedor_info = data.get("vendedor", {})

        # 🔐 VALIDAR STAGE: APENAS CONCLUÍDO PERMITE CRIAR
        stage = deal.get("STAGE_ID") or ""
        stage_valido, msg = validar_stage_para_pedido(stage)
        if not stage_valido:
            print(f"[BLING-PROPOSTA] ❌ {msg}")
            return {"erro": "Stage inválido - proposta bloqueada", "mensagem": msg}, 400

        print(f"\n🔥 [PROCESSAMENTO] === DADOS RECEBIDOS ===")
        print(f"[BLING] Deal ID: {deal.get('ID')} - Título: {deal.get('TITLE')}")
        print(f"[BLING] Empresa ID: {empresa.get('ID')} - Nome: {empresa.get('TITLE')}")
        print(f"[BLING] CNPJ: {empresa.get('UF_CRM_1713291425', 'NÃO INFORMADO')}")
        print(f"[BLING] Responsável: {empresa.get('UF_CRM_1724855055', 'NÃO INFORMADO')}")
        print(f"[BLING] Total de produtos: {len(produtos)}")
        print(f"[BLING] Vendedor ID: {vendedor_info.get('id', 'NÃO INFORMADO')} - Nome: {vendedor_info.get('nome', 'NÃO INFORMADO')}")

        # Validar dados obrigatórios
        if not deal or not empresa or not produtos:
            print(f"[BLING] ❌ ERRO: Dados obrigatórios faltando")
            print(f"[BLING] Deal presente: {bool(deal)}")  
            print(f"[BLING] Empresa presente: {bool(empresa)}")
            print(f"[BLING] Produtos presentes: {bool(produtos)}")
            return {"error": "Deal, empresa e produtos são obrigatórios"}, 400

        # Resolver vendedor no Bling priorizando o responsável do deal (ASSIGNED_BY_ID)
        print(f"\n🔍 [BUSCA VENDEDOR] === INICIANDO ===")
        print(f"[BLING] Deal.ASSIGNED_BY_ID (responsável): {deal.get('ASSIGNED_BY_ID', 'N/A') if isinstance(deal, dict) else 'N/A'}")
        vendedor_nome = vendedor_info.get('nome', '') if vendedor_info else ''
        print(f"[BLING] Vendedor recebido no payload: '{vendedor_nome}'")

        vendedor_dados = resolver_vendedor_bling(access_token, deal, vendedor_info)
        vendedor_id = vendedor_dados.get('id') if vendedor_dados else None
        vendedor_nome_final = vendedor_dados.get('nome') if vendedor_dados else None
        print(f"[BLING] ✅ Vendedor final escolhido: ID {vendedor_id}, Nome: '{vendedor_nome_final}'")

        # 🔥 ADICIONAR responsavel_representante ao dicionário empresa ANTES de passar
        # Usar o nome do vendedor ou buscar do mapeamento
        assigned_by_id_str = str(deal.get('ASSIGNED_BY_ID', '')) if deal.get('ASSIGNED_BY_ID') else ''
        nome_responsavel_prop = MAPA_NOMES_REPRESENTANTES.get(assigned_by_id_str, vendedor_nome)
        empresa['responsavel_representante'] = nome_responsavel_prop
        print(f"[BLING] ✅ Adicionado responsavel_representante à empresa: '{nome_responsavel_prop}'")

        # Buscar contato no Bling pelo CNPJ ou criar novo
        print(f"\n🔍 [BUSCA CONTATO] === INICIANDO ===")
        contato_bling = None
        cnpj = empresa.get('UF_CRM_1713291425', '')
        cnpj_limpo = ''.join(c for c in str(cnpj) if c.isdigit()) if cnpj else ''
        nome_empresa = empresa.get('TITLE', 'Cliente do Bitrix')
        
        print(f"[BLING] CNPJ original: '{cnpj}'")
        print(f"[BLING] CNPJ limpo: '{cnpj_limpo}'")
        print(f"[BLING] Nome da empresa: '{nome_empresa}'")
        
        # Buscar ou criar contato no Bling usando a nova função integrada
        print(f"\n🔍 [BUSCA CONTATO] === USANDO BUSCAR_OU_CRIAR_CONTATO ===")
        contato_bling = buscar_ou_criar_contato_bling(access_token, empresa, vendedor_id)
        
        if not contato_bling:
            print(f"[BLING] ❌ ERRO: Não foi possível criar ou encontrar contato no Bling")
            return {"error": "Não foi possível criar ou encontrar contato no Bling"}, 400

        contato_bling_id = contato_bling.get('id')
        contato_bling_nome = contato_bling.get('nome', 'N/A')
        
        print(f"\n🎯 [CONTATO FINAL] === RESULTADO ===")
        print(f"[BLING] ✅ Contato selecionado ID: {contato_bling_id}")
        print(f"[BLING] ✅ Nome do contato: {contato_bling_nome}")
        
        # VERIFICAÇÃO FINAL: Rejeitar contatos problemáticos
        if "TARIFA" in contato_bling_nome.upper() or "AVULSA" in contato_bling_nome.upper() or "PIX" in contato_bling_nome.upper():
            print(f"[BLING] 🚨🚨🚨 CONTATO PROBLEMÁTICO DETECTADO - BLOQUEANDO!")
            print(f"[BLING] 🚨 Contato retornado: {contato_bling_nome}")
            print(f"[BLING] 🚨 Empresa que deveria ser: {nome_empresa}")
            print(f"[BLING] 🚨 CNPJ: {cnpj_limpo}")
            return {
                "error": f"Contato inválido detectado: '{contato_bling_nome}'. O contato do Bling está incorreto. Verifique os dados no Bling.",
                "detalhes": {
                    "contato_encontrado": contato_bling_nome,
                    "contato_esperado": nome_empresa,
                    "cnpj": cnpj_limpo
                }
            }, 400

        # Preparar itens da proposta - USANDO PRODUTOS CADASTRADOS NO BLING
        print(f"\n📦 [PRODUTOS] === PROCESSANDO COM MAPEAMENTO BLING ===")
        itens = []
        total = 0
        produtos_ignorados = 0
        produtos_nao_mapeados = []
        
        for idx, produto in enumerate(produtos):
            try:
                quantidade = float(produto.get('QUANTITY', 0))
                preco = float(produto.get('PRICE', 0))
                nome_produto_extraido = produto.get('PRODUCT_NAME', 'Produto sem nome')
                
                print(f"\n[BLING] === PRODUTO {idx+1} ===")
                print(f"[BLING] Produto extraído: {nome_produto_extraido}")
                print(f"[BLING] Quantidade: {quantidade}")
                print(f"[BLING] Preço: R$ {preco}")
                
                # Filtrar produtos com quantidade zero (Bling não aceita)
                if quantidade <= 0:
                    print(f"[BLING] ⚠️  Produto ignorado (quantidade: {quantidade})")
                    produtos_ignorados += 1
                    continue
                
                # PASSO 1: Mapear produto extraído para código e nome do Bling
                print(f"\n[PRODUTO-DEBUG] === PASSO 2: MAPEAR NOME → CÓDIGO ===")
                print(f"[PRODUTO-DEBUG] Nome extraído: '{nome_produto_extraido}'")
                mapeamento_resultado = mapear_produto_para_codigo_bling(nome_produto_extraido)
                print(f"[PRODUTO-DEBUG] Resultado mapeamento: {mapeamento_resultado}")
                
                if not mapeamento_resultado:
                    print(f"[PRODUTO-DEBUG] ❌ ERRO: Produto '{nome_produto_extraido}' não mapeado para código do Bling")
                    produtos_nao_mapeados.append(nome_produto_extraido)
                    continue
                
                # Extrair código e nome do mapeamento
                codigo_bling = mapeamento_resultado["codigo"]
                nome_oficial_bling = mapeamento_resultado["nome"]
                
                # PASSO 2: Buscar produto cadastrado no Bling (para validação)
                print(f"\n[PRODUTO-DEBUG] === INICIANDO PASSO 3: BUSCA NO BLING ===")
                produto_bling = buscar_produto_bling_por_codigo(access_token, codigo_bling)
                print(f"[PRODUTO-DEBUG] Resultado da busca: {produto_bling}")
                
                if not produto_bling:
                    print(f"[PRODUTO-DEBUG] ❌ ERRO: Produto com código {codigo_bling} não encontrado no Bling")
                    produtos_nao_mapeados.append(f"{nome_produto_extraido} (código: {codigo_bling})")
                    continue
                
                # PASSO 3: Usar nome oficial do mapeamento (não do produto buscado)
                print(f"\n[PRODUTO-DEBUG] === PASSO 4: MONTAR ITEM DA PROPOSTA ===")
                id_produto_bling = produto_bling.get('id')
                
                # FORÇAR USO DOS DADOS ORIGINAIS DO MAPEAMENTO SEMPRE
                codigo_produto_bling = codigo_bling  # USAR CÓDIGO DO MAPEAMENTO, NÃO DO PRODUTO BUSCADO
                nome_oficial_final = nome_oficial_bling  # USAR NOME DO MAPEAMENTO, NÃO DO PRODUTO BUSCADO
                
                # USAR NOME OFICIAL DO MAPEAMENTO (nome correto do Bling)
                print(f"[PRODUTO-DEBUG] 📝 Nome oficial utilizado: '{nome_oficial_final}'")
                print(f"[PRODUTO-DEBUG] 🔗 Código do produto: '{codigo_produto_bling}'")
                print(f"[PRODUTO-DEBUG] 🆔 ID do produto: '{id_produto_bling}'")
                is_fallback = produto_bling.get('fallback', False)
                
                print(f"[PRODUTO-DEBUG] ID produto: {id_produto_bling}")
                print(f"[PRODUTO-DEBUG] Código produto: {codigo_produto_bling}")
                print(f"[PRODUTO-DEBUG] Nome oficial: {nome_oficial_final}")
                print(f"[PRODUTO-DEBUG] É fallback: {'✅' if is_fallback else '❌'}")
                
                # ESTRUTURA CORRETA PARA API v3 DO BLING com produto cadastrado
                if is_fallback:
                    # Para produtos fallback (sem ID real), usar só códigos
                    print(f"[PRODUTO-DEBUG] 🔄 Montando item FALLBACK (sem ID do produto)")
                    item = {
                        "codigo": codigo_produto_bling,
                        "descricao": nome_oficial_final,
                        "quantidade": quantidade,
                        "valor": preco,
                        "aliquotaIPI": 0,
                        "desconto": 0,
                        "unidade": "UN"
                    }
                else:
                    # Para produtos reais (com ID), usar estrutura completa
                    print(f"[PRODUTO-DEBUG] ✅ Montando item REAL (com ID do produto)")
                    item = {
                        "produto": {
                            "id": id_produto_bling
                        },
                        "codigo": codigo_produto_bling,
                        "descricao": nome_oficial_final,
                        "quantidade": quantidade,
                        "valor": preco,
                        "aliquotaIPI": 0,
                        "desconto": 0,
                        "unidade": "UN"
                    }
                
                print(f"[PRODUTO-DEBUG] Item montado: {json.dumps(item, indent=2, ensure_ascii=False)}")
                
                itens.append(item)
                total += (quantidade * preco)
                
                print(f"[BLING] ✅ Item {idx+1} adicionado {'(FALLBACK)' if is_fallback else '(OFICIAL)'}:")
                print(f"        Produto extraído: '{nome_produto_extraido}'")
                print(f"        Código identificado: {codigo_bling}")
                print(f"        Nome oficial Bling: '{nome_oficial_final}'")
                print(f"        Qtd: {quantidade}")
                print(f"        Preço unitário: R$ {preco:.2f}")
                print(f"        Subtotal: R$ {quantidade * preco:.2f}")
                print(f"        Status: {'FALLBACK (sem acesso API produtos)' if is_fallback else 'PRODUTO OFICIAL DO BLING'}")
                
            except (ValueError, TypeError) as e:
                print(f"[BLING] Erro ao processar produto {idx+1}: {e}")
                continue
        
        # Verificar se há produtos não mapeados (apenas se não foram gerados itens)
        if produtos_nao_mapeados and not itens:
            print(f"\n[BLING] 🚨 PRODUTOS NÃO MAPEADOS DETECTADOS (sem fallback disponível):")
            for produto_nao_mapeado in produtos_nao_mapeados:
                print(f"[BLING] 🚨 - {produto_nao_mapeado}")
            return {
                "error": "Produtos não mapeados para códigos do Bling", 
                "produtos_nao_mapeados": produtos_nao_mapeados,
                "mensagem": "Os seguintes produtos não foram encontrados no mapeamento para códigos do Bling. Verifique se os produtos estão cadastrados."
            }, 400
        elif produtos_nao_mapeados and itens:
            print(f"\n[BLING] ⚠️  AVISO: Alguns produtos não foram mapeados, mas continuando com os válidos:")
            for produto_nao_mapeado in produtos_nao_mapeados:
                print(f"[BLING] ⚠️  - {produto_nao_mapeado}")

        if not itens:
            print(f"[BLING] ❌ ERRO: Nenhum produto válido encontrado após filtros")
            print(f"[BLING] Total recebidos: {len(produtos)} | Ignorados: {produtos_ignorados}")
            return {"error": "Nenhum produto válido encontrado"}, 400
        
        print(f"\n[BLING] ✅ RESUMO FINAL DOS PRODUTOS:")
        print(f"[BLING] Total recebidos: {len(produtos)}")
        print(f"[BLING] Produtos ignorados (qtd zero): {produtos_ignorados}")
        print(f"[BLING] Produtos não mapeados: {len(produtos_nao_mapeados)}")
        print(f"[BLING] Itens válidos gerados: {len(itens)}")
        print(f"[BLING] Valor total: R$ {total:.2f}")

        # 🎯 OBTER RESPONSÁVEL (AOS CUIDADOS DE) - USAR VENDEDOR_NOME_FINAL DA RESOLUÇÃO
        # Usar sempre o nome retornado por resolver_vendedor_bling() para garantir consistência
        aos_cuidados = vendedor_nome_final if vendedor_nome_final else empresa.get('UF_CRM_1724855055', 'Representante')
        print(f"\n👤 [RESPONSÁVEL] Aos cuidados de: '{aos_cuidados}' (Vendedor: {vendedor_nome_final})")

        # PAYLOAD COMPLETO CORRETO PARA API BLING v3 - FORMATO EXATO
        print(f"\n🚀 [PAYLOAD] === MONTANDO PAYLOAD FINAL ===")
        payload = {
            "data": datetime.now().strftime("%Y-%m-%d"),
            "contato": {"id": contato_bling_id},
            "aosCuidadosDe": aos_cuidados,
            "itens": itens,
            "desconto": {"valor": 0, "unidade": "REAL"},
            "outrasDespesas": 0,
            "observacoes": "",
            "descricaoPadrao": {
                "condicoes": "",
                "introducao": f"Proposta comercial para {nome_empresa}",
                "saudacao": "Atenciosamente,",
                "departamento": "Departamento de vendas"
            }
        }
        
        # Adicionar vendedor apenas se foi encontrado (não None) - Bling API v3 só aceita ID numérico
        if vendedor_id:
            payload["vendedor"] = {"id": vendedor_id}
            print(f"[BLING] ✅ Vendedor será incluído: ID {vendedor_id} ('{vendedor_nome_final}')")
        else:
            print(f"[BLING] ⚠️ Campo vendedor será OMITIDO (não encontrado no Bling)")
        
        print(f"[BLING] 📊 RESUMO DO PAYLOAD:")
        print(f"   Data: {payload['data']}")
        print(f"   Contato ID: {contato_bling_id} (Nome: {contato_bling_nome})")
        print(f"   Vendedor ID: {vendedor_id if vendedor_id else 'VAZIO'}")
        print(f"   Aos cuidados: '{aos_cuidados}'")
        print(f"   Total de itens: {len(itens)}")
        print(f"   Valor total: R$ {total:.2f}")
        
        # Log do payload completo para debug
        print(f"\n📄 [DEBUG] PAYLOAD COMPLETO PARA BLING:")
        print(json.dumps(payload, indent=2, ensure_ascii=False))

        # Enviar para API BLING v3 ou simular sucesso no modo fallback
        print(f"\n🌐 [ENVIO BLING] === ENVIANDO PARA BLING ===")
        
        if access_token == "FALLBACK_TOKEN":
            print(f"[BLING] 🔄 MODO FALLBACK ATIVO - SIMULANDO CRIAÇÃO DE PROPOSTA")
            print(f"[BLING] ⚠️ Proposta NÃO será criada no Bling (token inválido)")
            print(f"[BLING] ✅ Simulando sucesso para permitir continuidade do sistema")
            
            # Simular resposta de sucesso
            proposta_id_simulado = f"FALLBACK_{int(time.time())}"
            
            return {
                "id": proposta_id_simulado,
                "numero": f"PROP-FALLBACK-{proposta_id_simulado[-6:]}",
                "status": "SIMULADO",
                "cliente": {"id": contato_bling_id, "nome": contato_bling_nome},
                "vendedor": {"id": vendedor_id} if vendedor_id else {},
                "itens": itens,
                "total": total,
                "observacoes": "⚠️ MODO FALLBACK: Esta proposta foi simulada devido a problemas de autenticação. Verifique os tokens do Bling.",
                "fallback_info": {
                    "motivo": "Token inválido ou expirado",
                    "empresa_original": nome_empresa,
                    "cnpj_original": cnpj_limpo,
                    "produtos_mapeados": len(itens)
                }
            }, 200
        
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        url = f"{BLING_API_BASE}/propostas-comerciais"
        print(f"[BLING] 🎯 URL: {url}")
        print(f"[BLING] 🔑 Authorization: Bearer {access_token[:20]}...")
        print(f"[BLING] ⏳ Enviando requisição POST...")
        
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        
        print(f"\n📥 [RESPOSTA BLING] Status: {response.status_code}")
        
        try:
            response_data = response.json()
            print(f"[BLING] Resposta JSON: {json.dumps(response_data, indent=2, ensure_ascii=False)}")
        except:
            print(f"[BLING] Resposta texto: {response.text}")
        
        if response.status_code not in [200, 201]:
            error_details = response.text
            print(f"\n❌ [ERRO BLING] FALHA AO CRIAR PROPOSTA!")
            print(f"[BLING] Status: {response.status_code}")
            print(f"[BLING] Detalhes: {error_details}")
            
            # AUTO-FALLBACK em caso de erro na API
            if response.status_code in [401, 403]:
                print(f"[BLING] 🔄 ERRO DE AUTENTICAÇÃO - ATIVANDO AUTO-FALLBACK")
                proposta_id_simulado = f"FALLBACK_{int(time.time())}"
                
                return {
                    "id": proposta_id_simulado,
                    "numero": f"PROP-FALLBACK-{proposta_id_simulado[-6:]}",
                    "status": "SIMULADO",
                    "cliente": {"id": contato_bling_id, "nome": contato_bling_nome},
                    "vendedor": {"id": vendedor_id} if vendedor_id else {},
                    "itens": itens,
                    "total": total,
                    "observacoes": f"⚠️ AUTO-FALLBACK: Erro {response.status_code} ao criar proposta. Verifique os tokens do Bling.",
                    "fallback_info": {
                        "motivo": f"Erro {response.status_code} da API Bling",
                        "erro_original": error_details,
                        "empresa_original": nome_empresa,
                        "cnpj_original": cnpj_limpo,
                        "produtos_mapeados": len(itens)
                    }
                }, 200
            
            print(f"[BLING] 🔍 Verifique se:")
            print(f"   - Contato ID {contato_bling_id} existe e é válido")
            print(f"   - Vendedor ID {vendedor_id} existe e é válido")
            print(f"   - Produtos têm quantidade > 0")
            return {
                "error": f"Erro {response.status_code}",
                "details": error_details,  
                "debug_info": {
                    "contato_usado": {"id": contato_bling_id, "nome": contato_bling_nome},
                    "vendedor_usado": vendedor_id,
                    "itens_enviados": len(itens),
                    "empresa_original": nome_empresa,
                    "cnpj_original": cnpj_limpo
                }
            }, response.status_code

        # SUCESSO
        proposta_data = response.json()
        proposta_id = proposta_data.get('data', {}).get('id')
        
        print(f"\n✅ [SUCESSO] === PROPOSTA CRIADA ===")
        print(f"[BLING] 🎉 Proposta ID: {proposta_id}")
        print(f"[BLING] 🏢 Cliente: {contato_bling_nome} (ID: {contato_bling_id})")
        print(f"[BLING] 👤 Vendedor ID: {vendedor_id if vendedor_id else 'VAZIO (não encontrado)'}")
        print(f"[BLING] 💰 Valor: R$ {total:.2f}")
        print(f"[BLING] 📦 Itens: {len(itens)}")
        
        if "TARIFA AVULSA" in contato_bling_nome.upper():
            print(f"\n🚨🚨🚨 ATENÇÃO! PROPOSTA CRIADA COM CLIENTE ERRADO!")
            print(f"[BLING] Cliente atual: {contato_bling_nome}")
            print(f"[BLING] Deveria ser: {nome_empresa}")
            print(f"[BLING] ID usado: {contato_bling_id}")
            print(f"[BLING] CNPJ buscado: {cnpj_limpo}")
        
        return {
            "sucesso": True,
            "id": proposta_id,
            "data": proposta_data.get('data'),
            "cliente": nome_empresa,
            "aos_cuidados": aos_cuidados,
            "total": round(total, 2),
            "itens_count": len(itens),
            "detalhes": {
                "deal_id": deal.get('ID'),
                "contato_bling_id": contato_bling_id,
                "vendedor_bling_id": vendedor_id
            }
        }

    except Exception as e:
        return {"error": f"Erro interno: {str(e)}"}, 500


# ============================================================================
# ROTAS DE CONTATOS - API BLING v3
# ============================================================================

@app.route("/contatos", methods=["GET"])
def listar_contatos():
    """
    Lista contatos do Bling - GET /contatos
    Parâmetros de query suportados:
    - pesquisa: Nome, CPF/CNPJ, fantasia, e-mail ou código do contato
    - numeroDocumento: CPF/CNPJ (somente números)
    - pagina: Número da página (default: 1)
    - limite: Quantidade por página (default: 100)
    """
    try:
        tokens = load_tokens()
        if not tokens:
            return {"error": "Tokens não encontrados. Faça a autenticação primeiro."}, 400

        if is_token_expired(tokens):
            tokens = refresh_token()
            if not tokens:
                return {"error": "Não foi possível renovar o token."}, 401

        access_token = tokens.get("access_token")
        if not access_token:
            return {"error": "Access token não encontrado."}, 400

        headers = {"Authorization": f"Bearer {access_token}"}
        
        # Construir parâmetros de query
        params = {}
        if request.args.get('pesquisa'):
            params['pesquisa'] = request.args.get('pesquisa')
        if request.args.get('numeroDocumento'):
            params['numeroDocumento'] = request.args.get('numeroDocumento')
        if request.args.get('pagina'):
            params['pagina'] = request.args.get('pagina')
        if request.args.get('limite'):
            params['limite'] = request.args.get('limite')
        
        url = f"{BLING_API_BASE}/contatos"
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        if response.status_code != 200:
            return {"error": f"Erro {response.status_code}", "details": response.text}, response.status_code
        
        return response.json()
        
    except Exception as e:
        return {"error": f"Erro interno: {str(e)}"}, 500


@app.route("/contatos/<int:idContato>", methods=["GET"])
def obter_contato(idContato):
    """
    Obtém um contato específico pelo ID - GET /contatos/{idContato}
    """
    try:
        tokens = load_tokens()
        if not tokens:
            return {"error": "Tokens não encontrados."}, 400

        if is_token_expired(tokens):
            tokens = refresh_token()
            if not tokens:
                return {"error": "Não foi possível renovar o token."}, 401

        access_token = tokens.get("access_token")
        if not access_token:
            return {"error": "Access token não encontrado."}, 400

        headers = {"Authorization": f"Bearer {access_token}"}
        url = f"{BLING_API_BASE}/contatos/{idContato}"
        
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code != 200:
            return {"error": f"Contato não encontrado", "status": response.status_code}, response.status_code
        
        return response.json()
        
    except Exception as e:
        return {"error": f"Erro interno: {str(e)}"}, 500


@app.route("/contatos", methods=["POST"])
def criar_contato():
    """
    Cria um novo contato no Bling - POST /contatos
    Recebe dados do contato no body JSON
    """
    try:
        tokens = load_tokens()
        if not tokens:
            return {"error": "Tokens não encontrados."}, 400

        if is_token_expired(tokens):
            tokens = refresh_token()
            if not tokens:
                return {"error": "Não foi possível renovar o token."}, 401

        access_token = tokens.get("access_token")
        if not access_token:
            return {"error": "Access token não encontrado."}, 400

        data = request.get_json()
        if not data:
            return {"error": "Dados do contato não fornecidos"}, 400

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        url = f"{BLING_API_BASE}/contatos"
        response = requests.post(url, headers=headers, json=data, timeout=30)
        
        if response.status_code not in [200, 201]:
            return {
                "error": f"Erro ao criar contato",
                "status": response.status_code,
                "details": response.text
            }, response.status_code
        
        return response.json(), 201
        
    except Exception as e:
        return {"error": f"Erro interno: {str(e)}"}, 500


@app.route("/contatos/buscar-por-cnpj/<cnpj>", methods=["GET"])
def buscar_contato_por_cnpj(cnpj):
    """
    Busca contato pelo CNPJ/CPF - Para evitar duplicação
    """
    try:
        tokens = load_tokens()
        if not tokens:
            return {"error": "Tokens não encontrados."}, 400

        if is_token_expired(tokens):
            tokens = refresh_token()
            if not tokens:
                return {"error": "Não foi possível renovar o token."}, 401

        access_token = tokens.get("access_token")
        if not access_token:
            return {"error": "Access token não encontrado."}, 400

        # Limpar CNPJ
        cnpj_limpo = ''.join(c for c in str(cnpj) if c.isdigit())
        
        contato = buscar_contato_bling_por_cnpj(access_token, cnpj_limpo)
        
        if contato:
            return {
                "encontrado": True,
                "contato": contato
            }
        else:
            return {
                "encontrado": False,
                "mensagem": f"Contato com documento {cnpj_limpo} não encontrado"
            }
        
    except Exception as e:
        return {"error": f"Erro interno: {str(e)}"}, 500


@app.route("/bling/buscar-criar-contato", methods=["POST"])
def buscar_criar_contato_para_pedido():
    """
    Busca ou cria contato no Bling a partir dos dados da empresa (Bitrix).
    Retorna o ID do contato no Bling para uso em pedidos de venda.
    POST /bling/buscar-criar-contato
    """
    import time
    tempo_inicio = time.time()
    
    try:
        print(f"\n[CONTATO-PEDIDO] === ENDPOINT /bling/buscar-criar-contato CHAMADO ===")

        tokens = load_tokens()
        if not tokens:
            return {
                "sucesso": False,
                "erro": "Tokens não encontrados",
                "tipo_erro": "AUTENTICACAO"
            }, 401

        if is_token_expired(tokens):
            print(f"[CONTATO-PEDIDO] 🔄 Token expirado, renovando...")
            tokens = refresh_token()
            if not tokens:
                return {
                    "sucesso": False,
                    "erro": "Token expirado e não foi possível renovar",
                    "tipo_erro": "AUTENTICACAO"
                }, 401

        access_token = tokens.get("access_token")

        # Verificar saúde da API Bling ANTES de tentar qualquer coisa
        print(f"[CONTATO-PEDIDO] 🏥 Verificando saúde da API Bling...")
        api_viva, motivo_saude = _verificar_saude_api_bling(access_token, timeout=5)
        if not api_viva:
            print(f"[CONTATO-PEDIDO] 🚨 API Bling indisponível: {motivo_saude}")
            tempo_total = time.time() - tempo_inicio
            return {
                "sucesso": False,
                "erro": f"API Bling indisponível: {motivo_saude}. Tente novamente mais tarde.",
                "tipo_erro": "API_INDISPONIVEL",
                "motivo": motivo_saude,
                "tempo_ms": int(tempo_total * 1000)
            }, 503

        data = request.get_json()
        if not data:
            return {
                "sucesso": False,
                "erro": "Dados não fornecidos",
                "tipo_erro": "VALIDACAO"
            }, 400

        empresa = data.get("empresa", {})
        deal = data.get("deal", {})
        vendedor_info = data.get("vendedor", {})

        if not empresa:
            return {
                "sucesso": False,
                "erro": "Dados da empresa são obrigatórios",
                "tipo_erro": "VALIDACAO"
            }, 400

        print(f"[CONTATO-PEDIDO] Empresa: {empresa.get('TITLE', 'N/A')}")
        print(f"[CONTATO-PEDIDO] CNPJ: {empresa.get('UF_CRM_1713291425', 'N/A')}")

        # RESOLVER VENDEDOR
        print(f"[CONTATO-PEDIDO] ⏱️ Iniciando busca de vendedor...")
        tempo_vendedor = time.time()
        vendedor_dados = resolver_vendedor_bling(access_token, deal, vendedor_info)
        vendedor_id = vendedor_dados.get('id') if vendedor_dados else None
        vendedor_nome = vendedor_dados.get('nome') if vendedor_dados else None
        tempo_vendedor_decorrido = time.time() - tempo_vendedor
        print(f"[CONTATO-PEDIDO] ✅ Vendedor resolvido em {tempo_vendedor_decorrido:.2f}s - ID: {vendedor_id}, Nome: '{vendedor_nome}'")

        # INJETAR RESPONSÁVEL REPRESENTANTE (ASSIGNED_BY_ID mapeado)
        assigned_by_id = deal.get('ASSIGNED_BY_ID')
        if assigned_by_id:
            assigned_by_id_str = str(assigned_by_id)
            nome_responsavel = MAPA_NOMES_REPRESENTANTES.get(assigned_by_id_str)
            if nome_responsavel:
                empresa['responsavel_representante'] = nome_responsavel
                print(f"[CONTATO-PEDIDO] 👤 Responsável representante injetado: {nome_responsavel}")
            else:
                print(f"[CONTATO-PEDIDO] ⚠️ ASSIGNED_BY_ID {assigned_by_id_str} não encontrado no mapa")

        # BUSCAR/CRIAR CONTATO
        print(f"[CONTATO-PEDIDO] ⏱️ Iniciando busca/criação de contato...")
        tempo_contato = time.time()
        try:
            contato_bling = buscar_ou_criar_contato_bling(access_token, empresa, vendedor_id)
        except Exception as e:
            tempo_erro = time.time() - tempo_contato
            erro_str = str(e)
            
            # Classificar tipo de erro
            if "API_BLING_INDISPONIVEL" in erro_str or "HTTP 503" in erro_str or "HTTP 502" in erro_str:
                tipo_erro = "API_INDISPONIVEL"
                mensagem = "API Bling está fora do ar. Tente novamente mais tarde."
                status_code = 503
            elif "FALHA_API_BLING" in erro_str:
                tipo_erro = "API_INDISPONIVEL"
                mensagem = erro_str.replace("FALHA_API_BLING: ", "")
                status_code = 503
            elif "Token" in erro_str or "expirado" in erro_str.lower():
                tipo_erro = "AUTENTICACAO"
                mensagem = "Token de autenticação inválido ou expirado"
                status_code = 401
            elif "Validação" in erro_str or "400" in erro_str:
                tipo_erro = "VALIDACAO"
                mensagem = "Erro de validação dos dados"
                status_code = 400
            else:
                tipo_erro = "ERRO_INTERNO"
                mensagem = f"Erro ao criar contato: {erro_str[:100]}"
                status_code = 500
            
            tempo_total = time.time() - tempo_inicio
            return {
                "sucesso": False,
                "erro": mensagem,
                "tipo_erro": tipo_erro,
                "detalhes": erro_str[:200],
                "tempo_ms": int(tempo_total * 1000)
            }, status_code
        
        tempo_contato_decorrido = time.time() - tempo_contato
        print(f"[CONTATO-PEDIDO] ✅ Contato resolvido em {tempo_contato_decorrido:.2f}s")

        if not contato_bling:
            tempo_total = time.time() - tempo_inicio
            return {
                "sucesso": False,
                "erro": "Não foi possível criar ou encontrar contato no Bling",
                "tipo_erro": "ERRO_INTERNO",
                "tempo_ms": int(tempo_total * 1000)
            }, 500

        contato_id = contato_bling.get('id')
        contato_nome = contato_bling.get('nome', 'N/A')
        contato_cnpj = contato_bling.get('numeroDocumento', '')
        contato_enderecos_brutos = contato_bling.get('enderecos', [])
        
        # === SANITIZAR DADOS DOS ENDEREÇOS RETORNADOS DO BLING ===
        # Garantir que CEP tenha 8 dígitos e UF tenha 2 letras
        print(f"[CONTATO-PEDIDO] 🧹 SANITIZANDO {len(contato_enderecos_brutos)} endereço(s) do Bling...")
        
        mapeamento_estados = {
            'MINAS GERAIS': 'MG', 'SÃO PAULO': 'SP', 'RIO DE JANEIRO': 'RJ', 'BAHIA': 'BA',
            'PARANÁ': 'PR', 'SANTA CATARINA': 'SC', 'RIO GRANDE DO SUL': 'RS', 'GOIÁS': 'GO',
            'MATO GROSSO': 'MT', 'MATO GROSSO DO SUL': 'MS', 'BRASÍLIA': 'DF', 'DISTRITO FEDERAL': 'DF',
            'ACRE': 'AC', 'ALAGOAS': 'AL', 'AMAPÁ': 'AP', 'AMAZONAS': 'AM', 'CEARÁ': 'CE',
            'ESPÍRITO SANTO': 'ES', 'MARANHÃO': 'MA', 'PARÁ': 'PA', 'PARAÍBA': 'PB',
            'PERNAMBUCO': 'PE', 'PIAUÍ': 'PI', 'RIO GRANDE DO NORTE': 'RN', 'RONDÔNIA': 'RO',
            'RORAIMA': 'RR', 'SERGIPE': 'SE', 'TOCANTINS': 'TO',
        }
        
        contato_enderecos = []
        for i, endereco in enumerate(contato_enderecos_brutos):
            endereco_limpo = endereco.copy()  # Copiar para não alterar original
            
            # Limpar CEP
            cep_bruto = endereco.get('cep', '')
            cep_limpo = ''.join(c for c in str(cep_bruto or '') if c.isdigit())
            if len(cep_limpo) == 8:
                endereco_limpo['cep'] = cep_limpo
                print(f"[CONTATO-PEDIDO]   Endereço {i+1} CEP: {cep_bruto} → {cep_limpo} ✅")
            else:
                endereco_limpo['cep'] = ''
                print(f"[CONTATO-PEDIDO]   Endereço {i+1} CEP inválido: {cep_bruto} ({len(cep_limpo)} dígitos) → vazio")
            
            # Limpar UF
            uf_bruto = (endereco.get('uf') or '').strip().upper()
            if len(uf_bruto) == 2:
                uf_limpo = uf_bruto
                print(f"[CONTATO-PEDIDO]   Endereço {i+1} UF: {uf_bruto} ✅")
            elif uf_bruto in mapeamento_estados:
                uf_limpo = mapeamento_estados[uf_bruto]
                print(f"[CONTATO-PEDIDO]   Endereço {i+1} UF: {uf_bruto} → {uf_limpo} ✅")
            else:
                uf_limpo = ''
                print(f"[CONTATO-PEDIDO]   Endereço {i+1} UF inválido: '{uf_bruto}' → vazio")
            
            endereco_limpo['uf'] = uf_limpo
            contato_enderecos.append(endereco_limpo)

        tempo_total = time.time() - tempo_inicio
        print(f"[CONTATO-PEDIDO] ✅ Contato Bling ID: {contato_id} - Nome: {contato_nome}")
        print(f"[CONTATO-PEDIDO]    CNPJ: {contato_cnpj if contato_cnpj else '(vazio)'}")
        print(f"[CONTATO-PEDIDO]    Endereços: {len(contato_enderecos)}")
        print(f"[CONTATO-PEDIDO] ⏱️ TEMPO TOTAL: {tempo_total:.2f}s (vendedor: {tempo_vendedor_decorrido:.2f}s + contato: {tempo_contato_decorrido:.2f}s)")

        return {
            "sucesso": True,
            "contato_id": contato_id,
            "contato_nome": contato_nome,
            "contato_cnpj": contato_cnpj,
            "contato_enderecos": contato_enderecos,  # Endereços sanitizados
            "contato_completo": contato_bling,  # ← NOVO: Retornar dados completos
            "vendedor_id": vendedor_id,
            "tipo_erro": None,
            "tempo_processamento_ms": int(tempo_total * 1000)
        }, 200

    except Exception as e:
        tempo_erro = time.time() - tempo_inicio
        erro_msg = str(e)
        print(f"[CONTATO-PEDIDO] ❌ Erro não tratado após {tempo_erro:.2f}s: {erro_msg}")
        import traceback
        traceback.print_exc()
        
        return {
            "error": f"Erro interno: {erro_msg[:100]}",
            "sucesso": False,
            "tipo_erro": "ERRO_INTERNO",
            "tempo_ms": int(tempo_erro * 1000)
        }, 500


# Entry point para Vercel
@app.route("/bling/pedidos-vendas", methods=["POST"])
def criar_pedido_venda():
    """
    Cria um pedido de venda no Bling a partir de dados do Bitrix.
    Aplica o mesmo mapeamento de produtos usado em /bling/proposta.
    POST /bling/pedidos-vendas
    Aceita:
      {
        "contato_id": int,           # ID do contato no Bling (obrigatório)
        "produtos": [...],           # Lista de produtos Bitrix (obrigatório)
        "vendedor_id": int,          # ID do vendedor no Bling (opcional)
        "deal": {...},               # Dados do deal Bitrix (opcional, para observações)
        "stage": str,                # Stage do deal (CRÍTICO: validado antes de criar pedido)
        "forma_pagamento_id": int    # ID da forma de pagamento (padrão: 2094030)
      }
    """
    try:
        print(f"\n[PEDIDO-VENDA] === ENDPOINT /bling/pedidos-vendas CHAMADO ===")

        # Obter dados da requisição
        req_data = request.get_json()
        if not req_data:
            return {"error": "Dados não fornecidos"}, 400

        # ═══════════════════════════════════════════════════════════════════════
        # 🔑 VALIDAÇÃO CRÍTICA: STAGE DO DEAL (GATEKEEPER)
        # ═══════════════════════════════════════════════════════════════════════
        # ❌ BLOQUEIO ABSOLUTO: Pedidos NUNCA devem ser criados para deals 
        #    que não estão em "concluído"
        
        from .validacao_stage import validar_stage_para_pedido
        
        stage_recebido = req_data.get("stage") or req_data.get("STAGE_ID")
        deal = req_data.get("deal", {})
        
        print(f"\n[PEDIDO-VENDA] 🔐 GATEKEEPER: VALIDANDO STAGE")
        print(f"[PEDIDO-VENDA]    Stage recebido: '{stage_recebido}'")
        print(f"[PEDIDO-VENDA]    Deal ID: {deal.get('ID', 'N/A')}")
        print(f"[PEDIDO-VENDA]    Deal Title: {deal.get('TITLE', 'N/A')}")
        
        # Validar stage
        stage_valido, mensagem_stage = validar_stage_para_pedido(stage_recebido)
        
        if not stage_valido:
            print(f"\n[PEDIDO-VENDA] 🚨🚨🚨 BLOQUEIO TOTAL 🚨🚨🚨")
            print(f"[PEDIDO-VENDA] {mensagem_stage}")
            print(f"[PEDIDO-VENDA] ❌ PEDIDO NÃO SERÁ CRIADO!")
            
            return {
                "sucesso": False,
                "erro": "Stage inválido - pedido bloqueado",
                "mensagem": mensagem_stage,
                "stage_recebido": stage_recebido,
                "motivo": "Pedidos SOMENTE podem ser criados para deals em 'concluído'",
                "deal_id": deal.get('ID', 'N/A'),
                "deal_title": deal.get('TITLE', 'N/A')
            }, 400  # Retornar 400 - Bad Request, não 200!
        
        print(f"[PEDIDO-VENDA] ✅ Stage validado: {stage_recebido}")

        # ──────────────────────────────────────────────────────────────────────
        # 🚫 VALIDAÇÃO: Bloquear Theikos e outros nomes proibidos
        # ──────────────────────────────────────────────────────────────────────
        
        deal_title = deal.get("TITLE") or deal.get("title") or req_data.get("titulo")
        
        if deal_title and VALIDACAO_NOMES_AVAILABLE:
            nome_bloqueado, razao = verificar_nome_bloqueado(deal_title)
            if nome_bloqueado:
                print(f"\n[PEDIDO-VENDA] 🚫 BLOQUEADO: {razao}")
                print(f"[PEDIDO-VENDA] Deal title: {deal_title}")
                print(f"[PEDIDO-VENDA] ❌ NÃO será criada!")
                return {
                    "sucesso": False,
                    "mensagem": f"Deal bloqueada: {razao}",
                    "titulo": deal_title,
                    "status": "bloqueada"
                }, 400  # Changed from 200 to 400
            else:
                print(f"[PEDIDO-VENDA] ✅ Título validado: {deal_title}")
        
        # Carregar tokens
        tokens = load_tokens()
        if not tokens:
            return {"error": "Tokens não encontrados"}, 401

        if is_token_expired(tokens):
            tokens = refresh_token()
            if not tokens:
                return {"error": "Token expirado e não foi possível renovar"}, 401

        access_token = tokens.get("access_token")

        contato_id = req_data.get("contato_id")
        produtos_bitrix = req_data.get("produtos", [])
        vendedor_id = req_data.get("vendedor_id")
        deal = req_data.get("deal", {})
        empresa = req_data.get("empresa", {})  # ← NOVO: Dados da empresa para validação
        forma_pagamento_id = req_data.get("forma_pagamento_id", 2094030)

        print(f"[PEDIDO-VENDA] Contato ID: {contato_id}")
        print(f"[PEDIDO-VENDA] Vendedor ID: {vendedor_id}")
        print(f"[PEDIDO-VENDA] Produtos recebidos: {len(produtos_bitrix)}")
        
        # ══════════════════════════════════════════════════════════════════════
        # VALIDAÇÃO CRÍTICA: Garantir que contato tem CNPJ e endereço
        # ══════════════════════════════════════════════════════════════════════
        if contato_id:
            print(f"\n[PEDIDO-VENDA] === VALIDANDO CONTATO PARA PEDIDO ===")
            contato_valido, msg_validacao = validar_contato_completo_para_pedido(access_token, contato_id, empresa)
            
            if not contato_valido:
                print(f"[PEDIDO-VENDA] ❌ CONTATO INVÁLIDO: {msg_validacao}")
                return {
                    "sucesso": False,
                    "mensagem": f"Contato incompleto para criar pedido: {msg_validacao}",
                    "detalhes": "Certifique-se de que o contato tem CNPJ e endereço completo no Bling"
                }, 400
            else:
                print(f"[PEDIDO-VENDA] ✅ Contato validado com sucesso!")
        
        # ── DEBUG: Listar TODOS os produtos recebidos ──────────────────────
        if produtos_bitrix:
            print(f"\n[PEDIDO-VENDA] === PRODUTOS RECEBIDOS DO FRONTEND ===")
            for idx, p in enumerate(produtos_bitrix, 1):
                nome = p.get('PRODUCT_NAME', 'VAZIO')
                qtd = p.get('QUANTITY', 'N/A')
                preco = p.get('PRICE', 'N/A')
                print(f"   {idx}/{len(produtos_bitrix)}: {nome} (Qtd: {qtd}, Preço: {preco})")

        if not contato_id:
            return {"error": "contato_id é obrigatório"}, 400
        if not produtos_bitrix:
            return {"error": "produtos é obrigatório"}, 400

        # ── FILTRO: Produtos a EXCLUIR (não enviar para Bling) ──────────────
        PRODUTOS_EXCLUIR = [
            "TINTA FILL MASTER COLOR 500ML",
            "PINCÉIS FILL ECO MARKER"
        ]
        print(f"\n[PEDIDO-VENDA] === FILTRO DE EXCLUSÃO ===")
        print(f"[PEDIDO-VENDA] Produtos que SERÃO IGNORADOS:")
        for p in PRODUTOS_EXCLUIR:
            print(f"   ❌ {p}")

        # ── Mapear produtos (igual ao /bling/proposta) ──────────────────────
        itens = []
        total = 0
        produtos_nao_mapeados = []

        for idx, produto in enumerate(produtos_bitrix):
            try:
                quantidade = float(produto.get('QUANTITY', 0))
                preco_bitrix = float(produto.get('PRICE', 0))
                nome_produto_extraido = produto.get('PRODUCT_NAME', 'Produto sem nome')

                print(f"\n[PEDIDO-VENDA] === PRODUTO {idx+1}: '{nome_produto_extraido}' ===")
                print(f"[PEDIDO-VENDA] Qtd: {quantidade} | Preço Bitrix: R$ {preco_bitrix}")

                # ── FILTRO: Verificar se produto deve ser EXCLUÍDO ──────────────────
                nome_produto_normalizado = nome_produto_extraido.strip().upper()
                deve_excluir = False
                for produto_excluido in PRODUTOS_EXCLUIR:
                    if produto_excluido.upper() in nome_produto_normalizado or nome_produto_normalizado in produto_excluido.upper():
                        print(f"[PEDIDO-VENDA] 🚫 FILTRADO POR EXCLUSÃO: '{nome_produto_extraido}'")
                        deve_excluir = True
                        break
                
                if deve_excluir:
                    print(f"[PEDIDO-VENDA] ⚠️  Ignorado (na lista de exclusão)")
                    continue

                # Ignorar produtos com quantidade zero
                if quantidade <= 0:
                    print(f"[PEDIDO-VENDA] ⚠️  Ignorado (quantidade zero)")
                    continue

                # Mapear nome Bitrix → código + nome oficial Bling
                mapeamento = mapear_produto_para_codigo_bling(nome_produto_extraido)
                if not mapeamento:
                    print(f"[PEDIDO-VENDA] ❌ Produto não mapeado: '{nome_produto_extraido}'")
                    produtos_nao_mapeados.append(nome_produto_extraido)
                    continue

                codigo_bling = mapeamento["codigo"]
                nome_oficial_bling = mapeamento["nome"]

                # Buscar produto no Bling pelo código (para obter o ID real)
                produto_bling = buscar_produto_bling_por_codigo(access_token, codigo_bling)

                id_produto_bling = None
                if produto_bling:
                    id_produto_bling = produto_bling.get('id')
                    print(f"[PEDIDO-VENDA] ✅ Produto encontrado no Bling: ID {id_produto_bling}")
                    print(f"[PEDIDO-VENDA]    Preço Bling: R$ {produto_bling.get('preco', 'N/A')}")
                else:
                    print(f"[PEDIDO-VENDA] ⚠️  Produto não encontrado no Bling (ID será vazio)")
                    print(f"[PEDIDO-VENDA]    Será enviado apenas código + descrição")
                    print(f"[PEDIDO-VENDA]    Bling usará seu próprio preço cadastrado")

                # ── IMPORTANTE: Enviar SEMPRE com preço do Bitrix ──────────────────────
                # O Bling tem seu próprio preço cadastrado para cada produto.
                # Se enviamos com valor 0, o Bling não consegue processar.
                # SOLUÇÃO: Sempre enviar com valor do Bitrix
                valor_item = preco_bitrix
                total += (quantidade * preco_bitrix)

                print(f"[PEDIDO-VENDA] Valor a enviar: R$ {valor_item}")

                # ── Montar item (COM ID obrigatório do produto) ────────────────────────────
                # CRÍTICO: A API v3 do Bling EXIGE que items tenham o ID interno do produto
                # Items sem ID são tratados como "texto livre" (avulsos) e geram alerta amarelo
                if id_produto_bling:
                    # ✅ CORRETO: Enviando COM ID do produto (foi encontrado no Bling)
                    # Estrutura obrigatória: "produto": {"id": id_interno}
                    item = {
                        "produto": {"id": id_produto_bling},
                        "quantidade": quantidade,
                        "valor": valor_item,
                        "unidade": "UN",
                        "descricao": nome_oficial_bling,
                        "aliquotaIPI": 0
                    }
                    print(f"[PEDIDO-VENDA] ✅ Item CORRETO com ID: {id_produto_bling}")
                    itens.append(item)
                    print(f"[PEDIDO-VENDA] ✅ Item adicionado ao pedido\n")
                else:
                    # ❌ CRÍTICO: Não encontrou o produto no Bling
                    # NÃO PODEMOS enviar como "código" + "descrição" pois gerará alerta amarelo!
                    # Ação: IGNORAR o item e avisar
                    print(f"[PEDIDO-VENDA] ❌ ERRO CRÍTICO: Produto não encontrado no Bling!")
                    print(f"[PEDIDO-VENDA] ❌ NÃO PODE ser enviado sem ID (causaria alerta amarelo)")
                    print(f"[PEDIDO-VENDA] ❌ IGNORANDO o produto: '{nome_oficial_bling}'")
                    print(f"[PEDIDO-VENDA] ℹ️  Para resolver: Cadastre o produto no Bling com código '{codigo_bling}'")
                    produtos_nao_mapeados.append(f"{nome_oficial_bling} (não encontrado no Bling)")
                    print(f"[PEDIDO-VENDA] ⚠️  Item NÃO foi adicionado\n")
                    continue

            except (ValueError, TypeError) as e:
                print(f"[PEDIDO-VENDA] Erro ao processar produto {idx+1}: {e}")
                continue

        # ── ADICIONAR NOVO PRODUTO: FONTE 1A 12V ────────────────────────────
        print(f"\n[PEDIDO-VENDA] === ADICIONANDO PRODUTO CUSTOMIZADO ===")
        
        try:
            codigo_fonte = "pav0014"
            nome_fonte = "fonte 1a 12 v"
            quantidade_fonte = 1
            valor_fonte = 0  # Preço unitário é 0 conforme solicitado
            
            print(f"[PEDIDO-VENDA] Adicionando: {nome_fonte}")
            print(f"[PEDIDO-VENDA] Código: {codigo_fonte}")
            print(f"[PEDIDO-VENDA] Quantidade: {quantidade_fonte}")
            print(f"[PEDIDO-VENDA] Valor Unitário: R$ {valor_fonte}")
            
            # Buscar produto "FONTE 1A 12V" no Bling (está no mapeamento)
            mapeamento_fonte = mapear_produto_para_codigo_bling("FONTE 1A 12V")
            
            if mapeamento_fonte:
                codigo_oficial = mapeamento_fonte["codigo"]
                nome_oficial = mapeamento_fonte["nome"]
                print(f"[PEDIDO-VENDA] ✅ Encontrado no mapeamento: {codigo_oficial} → {nome_oficial}")
                
                # Buscar produto no Bling pelo código (para obter o ID real)
                produto_bling = buscar_produto_bling_por_codigo(access_token, codigo_oficial)
                
                id_produto_bling = None
                if produto_bling:
                    id_produto_bling = produto_bling.get('id')
                    print(f"[PEDIDO-VENDA] ✅ Produto encontrado no Bling: ID {id_produto_bling}")
                    print(f"[PEDIDO-VENDA]    Preço Bling: R$ {produto_bling.get('preco', 'N/A')}")
                    
                    # ✅ CORRETO: Item COM ID obrigatório
                    item_fonte = {
                        "produto": {"id": id_produto_bling},
                        "quantidade": quantidade_fonte,
                        "valor": valor_fonte,
                        "unidade": "UN",
                        "descricao": nome_oficial,
                        "aliquotaIPI": 0
                    }
                    print(f"[PEDIDO-VENDA] ✅ Item FONTE adicionado COM ID: {id_produto_bling}")
                    itens.append(item_fonte)
                    total += (quantidade_fonte * valor_fonte)
                    print(f"[PEDIDO-VENDA] ✅ Produto FONTE adicionado ao pedido")
                else:
                    # ❌ CRÍTICO: Não encontrou o FONTE no Bling
                    print(f"[PEDIDO-VENDA] ❌ ERRO CRÍTICO: FONTE não encontrada no Bling!")
                    print(f"[PEDIDO-VENDA] ❌ NÃO PODE ser enviada sem ID (causaria alerta amarelo)")
                    print(f"[PEDIDO-VENDA] ❌ IGNORANDO o produto FONTE")
                    print(f"[PEDIDO-VENDA] ℹ️  Para resolver: Cadastre FONTE com código '{codigo_oficial}' no Bling")
            
            else:
                # FONTE não encontrada no mapeamento - não adicionar
                print(f"[PEDIDO-VENDA] ⚠️  FONTE não encontrada no mapeamento")
                print(f"[PEDIDO-VENDA] ❌ Produto FONTE NÃO será adicionado (não pode ser enviado sem ID)")
        
        except Exception as e:
            print(f"[PEDIDO-VENDA] ❌ Erro ao adicionar FONTE: {e}")
            import traceback
            traceback.print_exc()

        if not itens:
            return {
                "sucesso": False,
                "mensagem": "Nenhum produto válido encontrado para criar o pedido",
                "produtos_nao_mapeados": produtos_nao_mapeados
            }, 400

        # ── Montar payload para Bling ────────────────────────────────────────
        from datetime import timedelta
        hoje = datetime.now().strftime("%Y-%m-%d")
        data_prevista = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")
        deal_id = deal.get("ID", "N/A")
        deal_title = deal.get("TITLE", "Sem título")

        payload = {
            "contato": {"id": contato_id},
            "data": hoje,
            "dataPrevista": data_prevista,
            "itens": itens,
            "parcelas": [
                {
                    "dataVencimento": data_prevista,
                    "valor": round(total, 2),
                    "formaPagamento": {"id": forma_pagamento_id}
                }
            ],
            "observacoes": f"Pedido gerado a partir do deal #{deal_id}: {deal_title}"
        }

        if vendedor_id:
            payload["vendedor"] = {"id": vendedor_id}

        print(f"\n[PEDIDO-VENDA] 📊 RESUMO PAYLOAD:")
        print(f"   Data: {hoje}")
        print(f"   Contato ID: {contato_id}")
        print(f"   Vendedor ID: {vendedor_id}")
        print(f"   Itens: {len(itens)}")
        print(f"   Total: R$ {total:.2f}")
        print(f"[PEDIDO-VENDA] Payload:")
        print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))

        # ── Enviar para Bling API ────────────────────────────────────────────
        bling_headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        response = requests.post(
            f"{BLING_API_BASE}/pedidos/vendas",
            headers=bling_headers,
            json=payload,
            timeout=30
        )

        print(f"[PEDIDO-VENDA] Status Bling: {response.status_code}")

        if response.status_code in [200, 201]:
            result = response.json()
            pedido_id = result.get('data', {}).get('id', 'N/A')
            numero_pedido = result.get('data', {}).get('numero', 'N/A')

            print(f"[PEDIDO-VENDA] ✅ SUCESSO! ID: {pedido_id} | Número: {numero_pedido}")

            return {
                "sucesso": True,
                "mensagem": "Pedido de venda criado com sucesso!",
                "data": result.get('data', {}),
                "pedido_id": pedido_id,
                "numero_pedido": numero_pedido,
                "produtos_nao_mapeados": produtos_nao_mapeados
            }, 201

        else:
            error_msg = response.text
            print(f"[PEDIDO-VENDA] ❌ ERRO {response.status_code}: {error_msg}")
            try:
                error_json = response.json()
                return {
                    "sucesso": False,
                    "mensagem": f"Erro ao criar pedido: {response.status_code}",
                    "erro_bling": error_json
                }, response.status_code
            except Exception:
                return {
                    "sucesso": False,
                    "mensagem": f"Erro ao criar pedido: {response.status_code}",
                    "erro_bling": error_msg[:500]
                }, response.status_code

    except Exception as e:
        error_msg = f"Erro ao criar pedido de venda: {str(e)}"
        print(f"[PEDIDO-VENDA] ❌ EXCEÇÃO: {error_msg}")
        import traceback
        print(f"[PEDIDO-VENDA] Traceback: {traceback.format_exc()}")
        return {
            "sucesso": False,
            "mensagem": error_msg,
            "erro": str(e)
        }, 500


# ============================================================================
# WEBHOOK BITRIX - INBOUND (Recebe notificações do Bitrix)
# ============================================================================

def parse_flat_bitrix_data(flat_dict):
    """
    Converte dicionário flat do Bitrix com chaves como 'data[FIELDS][ID]'
    para estrutura aninhada {'data': {'FIELDS': {'ID': ...}}}
    
    Também suporta chaves sem prefixo como 'ID', 'STAGE_ID', etc
    """
    result = {}
    
    print(f"\n[WEBHOOK] === DEBUG: Parsing form-data ===")
    print(f"[WEBHOOK] Total de chaves recebidas: {len(flat_dict)}")
    
    # Debug: mostra todas as chaves
    for key in list(flat_dict.keys())[:20]:
        print(f"[WEBHOOK]   Chave: '{key}' = '{str(flat_dict[key])[:50]}'")
    
    for key, value in flat_dict.items():
        # Skip campos especiais
        if key in ['event', 'event_handler_id', 'ts', 'auth[domain]']:
            if key == 'event':
                result['event'] = value
            continue
        
        # Parse chaves como "data[FIELDS][ID]"
        if 'data[' in key and '[' in key and ']' in key:
            # Exemplo: "data[FIELDS][ID]" → ['data', 'FIELDS', 'ID']
            # Remover "data[" do início e processar
            key_clean = key.replace('data[', '').replace(']', '')
            parts = key_clean.split('[')
            
            # Garantir que 'data' existe no resultado
            if 'data' not in result:
                result['data'] = {}
            
            current = result['data']
            
            # Navegar/criar a estrutura aninhada
            for part in parts[:-1]:
                if part not in current:
                    current[part] = {}
                current = current[part]
            
            # Atribuir o valor
            current[parts[-1]] = value
            print(f"[WEBHOOK] ✅ Parsed: data.{'.'.join(parts)} = {str(value)[:30]}")
        
        # Parse chaves diretas como "ID", "STAGE_ID" (enviadas direto, não dentro de "data")
        elif key in ['ID', 'STAGE_ID', 'TITLE', 'COMPANY_ID', 'CONTACT_ID', 'OPPORTUNITY', 'event']:
            if 'data' not in result:
                result['data'] = {}
            if 'FIELDS' not in result['data']:
                result['data']['FIELDS'] = {}
            result['data']['FIELDS'][key] = value
            print(f"[WEBHOOK] ✅ Parsed (direto): data.FIELDS.{key} = {str(value)[:30]}")
    
    print(f"\n[WEBHOOK] === Resultado do parse ===")
    if 'data' in result and 'FIELDS' in result['data']:
        print(f"[WEBHOOK] ✅ FIELDS encontrado com {len(result['data']['FIELDS'])} campos")
        deal_id = result['data']['FIELDS'].get('ID')
        print(f"[WEBHOOK] ✅ Deal ID: {deal_id}")
    else:
        print(f"[WEBHOOK] ❌ FIELDS NÃO ENCONTRADO!")
        print(f"[WEBHOOK]    Estructura do resultado: {list(result.keys())}")
    
    return result


if __name__ == "__main__":
    # ==================================================
    # STARTUP: Mostrar Status
    # ==================================================
    print(f"\n{'='*70}", flush=True)
    print(f"🚀 BACKEND INICIANDO - STATUS DOS MÓDULOS", flush=True)
    print(f"{'='*70}", flush=True)
    print(f"[STARTUP] WEBHOOK_HANDLER_AVAILABLE: {WEBHOOK_HANDLER_AVAILABLE}", flush=True)
    print(f"[STARTUP] PEDIDO_FUNCTION_AVAILABLE: {PEDIDO_FUNCTION_AVAILABLE}", flush=True)
    print(f"[STARTUP] TOKEN_MANAGER_AVAILABLE: {TOKEN_MANAGER_AVAILABLE}", flush=True)
    print(f"[STARTUP] Modo: {'WEBHOOK_HANDLER' if WEBHOOK_HANDLER_AVAILABLE else 'INLINE_SYNC'}", flush=True)
    print(f"{'='*70}\n", flush=True)
    
    # Desativar use_reloader=True para evitar stat reloader que causa buffer issues
    # threaded=True para suportar múltiplas requisições simultâneas
    app.run(debug=False, port=3000, use_reloader=False, threaded=True)
