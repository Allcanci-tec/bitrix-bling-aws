"""
🎣 WEBHOOK RECEIVER FOR BITRIX

Recebe notificações do Bitrix quando deal muda para "Concluído" (WON)
e automaticamente cria pedido de venda no Bling
"""

import os
import sys
import json
import unicodedata
from pathlib import Path
from datetime import datetime, timedelta
import requests
import time
# Importar token manager para renovação automática
try:
    from backend.bling_token_manager import get_valid_bling_token, make_bling_request_with_auto_refresh
except ImportError:
    try:
        from api.bling_token_manager import get_valid_bling_token, make_bling_request_with_auto_refresh
    except ImportError as e:
        print(f"[HANDLER] ❌ Erro ao importar token manager: {e}")
        # Stub function if import fails
        def get_valid_bling_token():
            return os.getenv("BLING_ACCESS_TOKEN", "").strip()

# ============================================================================
# CONFIGURAÇÃO
# ============================================================================

WEBHOOK_PROCESSED_FILE = Path(__file__).parent / '.webhook_processed_deals.json'

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

# Mapeamento Bitrix ID → Bling Vendor ID (IDs conhecidos)
VENDEDOR_MAP_HANDLER = {
    # Desativado.
    # Não usar ID fixo para vendedor.
    # Vendedor será resolvido somente por nome exato no Bling.
}

# Carregar cache de usuários Bitrix
try:
    import sys as _sys
    _sys.path.insert(0, str(Path(__file__).parent.parent))
    from cache_loader import get_cache as _get_cache
    _HANDLER_CACHE = _get_cache()
    print(f"[HANDLER-CACHE] ✅ Cache carregado: {len(_HANDLER_CACHE.users_cache)} usuários")
except Exception as _e:
    _HANDLER_CACHE = None
    print(f"[HANDLER-CACHE] ⚠️ Cache não disponível: {_e}")


def _buscar_nome_usuario_bitrix(bitrix_url: str, bitrix_headers: dict, user_id: str) -> str:
    """
    Consulta a API do Bitrix para obter o nome completo de um usuário pelo ID.
    Usado como último fallback quando o ID não está no cache nem no mapa estático.

    Returns:
        Nome completo do usuário (ex: 'João Silva') ou None se falhar.
    """
    try:
        print(f"[HANDLER] 🌐 Consultando API Bitrix para nome do usuário ID={user_id}...")
        resp = requests.post(
            bitrix_url + 'user.get',
            json={'ID': user_id},
            headers=bitrix_headers,
            timeout=10
        )
        if resp.status_code == 200:
            resultado = resp.json().get('result', [])
            if isinstance(resultado, list) and len(resultado) > 0:
                usuario = resultado[0]
            elif isinstance(resultado, dict):
                usuario = resultado
            else:
                print(f"[HANDLER] ⚠️ API Bitrix retornou lista vazia para user_id={user_id}")
                return None
            nome = ' '.join(filter(None, [
                usuario.get('NAME', ''),
                usuario.get('LAST_NAME', '')
            ])).strip()
            if nome:
                print(f"[HANDLER] ✅ Nome obtido da API Bitrix: '{nome}' (ID: {user_id})")
                # Atualizar mapa estático em memória para próximas chamadas
                MAPA_NOMES_REPRESENTANTES[str(user_id)] = nome
                return nome
            else:
                print(f"[HANDLER] ⚠️ Usuário {user_id} encontrado mas sem nome")
                return None
        else:
            print(f"[HANDLER] ⚠️ API Bitrix user.get retornou HTTP {resp.status_code}")
            return None
    except Exception as e:
        print(f"[HANDLER] ❌ Erro ao buscar usuário Bitrix {user_id}: {e}")
        return None

# ============================================================================
# FUNÇÃO BLINDADA: BUSCAR OU CRIAR CONTATO COM TRATAMENTO DE ERRO 400
# ============================================================================

import re

def _extrair_nome_contato_do_erro_bling(mensagem_erro: str) -> str:
    """
    Extrai o nome do contato da mensagem de erro 400 do Bling.
    
    Exemplo de erro: "O CNPJ já está cadastrado no contato teste 2 rennifer"
    Extraído: "teste 2 rennifer"
    
    Args:
        mensagem_erro: String da mensagem de erro do Bling
    
    Returns:
        Nome do contato extraído (ou string vazia se não conseguir)
    """
    try:
        # Regex para extrair o texto após "contato "
        # Padrão: "... contato NOME_AQUI" ou "... contato NOME_AQUI..."
        match = re.search(r'contato\s+(.+?)(?:\s*$|"|\||,)', mensagem_erro, re.IGNORECASE)
        if match:
            nome_extraido = match.group(1).strip()
            print(f"[BLINDAGEM] ✅ Nome extraído do erro: '{nome_extraido}'")
            return nome_extraido
        else:
            print(f"[BLINDAGEM] ⚠️ Regex não encontrou nome no erro")
            return ""
    except Exception as e:
        print(f"[BLINDAGEM] ❌ Erro ao extrair nome: {e}")
        return ""

def _buscar_contato_por_nome_bling(bling_token: str, nome_empresa: str) -> dict:
    """
    Busca um contato no Bling por NOME (usando parâmetro pesquisa).
    
    IMPORTANTE: 🚫 NÃO retorna resultados aleatórios se o nome é genérico/vazio!
    
    Args:
        bling_token: Token de autenticação do Bling
        nome_empresa: Nome da empresa a buscar
    
    Returns:
        dict contendo dados do contato (com 'id', 'nome', 'cnpj', etc.) ou {} se não encontrado
    """
    # ✅ BLOQUEIO: Nunca buscar por nome genérico (evita contato aleatório)
    if not nome_empresa or len(nome_empresa.strip()) < 3:
        print(f"[BLINDAGEM] 🚫 BLOQUEIO: Nome muito curto para buscar: '{nome_empresa}'")
        print(f"[BLINDAGEM]    Retornando vazio para evitar contato ALEATÓRIO")
        return {}
    
    # ✅ BLOQUEIO: Negar nomes genéricos/inválidos
    nomes_bloqueados = ['sem nome', 'cliente', 'empresa', 'n/a', 'desconhecido', 'genérico', '']
    if nome_empresa.lower().strip() in nomes_bloqueados:
        print(f"[BLINDAGEM] 🚫 BLOQUEIO: Nome genérico/inválido: '{nome_empresa}'")
        print(f"[BLINDAGEM]    Não vou buscar por nome genérico (evita contato ALEATÓRIO)")
        return {}
    
    try:
        url_busca = f"https://www.bling.com.br/Api/v3/contatos?pesquisa={nome_empresa}"
        headers = {"Authorization": f"Bearer {bling_token}"}
        
        print(f"[BLINDAGEM] 🔍 Buscando contato por NOME...")
        print(f"[BLINDAGEM]    → GET /contatos?pesquisa={nome_empresa}")
        
        resp_busca = requests.get(url_busca, headers=headers, timeout=15)
        
        if resp_busca.status_code == 200:
            dados_resposta = resp_busca.json()
            contatos_lista = dados_resposta.get('data', [])
            
            if contatos_lista and len(contatos_lista) > 0:
                contato_encontrado = contatos_lista[0]
                contato_id = contato_encontrado.get('id')
                nome_contato = contato_encontrado.get('nome', 'Sem nome')
                
                print(f"[BLINDAGEM] ✅ Contato encontrado por NOME:")
                print(f"[BLINDAGEM]    ID: {contato_id}")
                print(f"[BLINDAGEM]    Nome: {nome_contato}")
                print(f"[BLINDAGEM]    CNPJ: {contato_encontrado.get('numeroDocumento', 'N/A')}")
                
                return {
                    'id': contato_id,
                    'nome': nome_contato,
                    'numeroDocumento': contato_encontrado.get('numeroDocumento', ''),
                    'data_completa': contato_encontrado  # Guardar dados completos
                }
            else:
                print(f"[BLINDAGEM] ⚠️ Nenhum contato encontrado com nome '{nome_empresa}'")
                return {}
        else:
            print(f"[BLINDAGEM] ⚠️ Erro HTTP {resp_busca.status_code} ao buscar por nome")
            print(f"[BLINDAGEM]    Detalhes: {resp_busca.text[:200]}")
            return {}
            
    except Exception as e:
        print(f"[BLINDAGEM] ❌ Exception ao buscar contato por nome: {type(e).__name__}: {str(e)}")
        return {}

def _buscar_contato_por_cnpj_bling(bling_token: str, cnpj_limpo: str) -> dict:
    """
    Busca um contato no Bling por CNPJ (numeroDocumento).
    
    Args:
        bling_token: Token de autenticação do Bling
        cnpj_limpo: CNPJ apenas com dígitos
    
    Returns:
        dict contendo dados do contato (com 'id', 'nome', 'cnpj', etc.) ou {} se não encontrado
    """
    if not cnpj_limpo or len(cnpj_limpo) != 14:
        print(f"[BLINDAGEM] ⚠️ CNPJ inválido para busca: '{cnpj_limpo}'")
        return {}
    
    try:
        url_busca = f"https://www.bling.com.br/Api/v3/contatos?numeroDocumento={cnpj_limpo}"
        headers = {"Authorization": f"Bearer {bling_token}"}
        
        print(f"[BLINDAGEM] 🔍 Buscando contato por CNPJ...")
        print(f"[BLINDAGEM]    → GET /contatos?numeroDocumento={cnpj_limpo}")
        
        resp_busca = requests.get(url_busca, headers=headers, timeout=15)
        
        if resp_busca.status_code == 200:
            dados_resposta = resp_busca.json()
            contatos_lista = dados_resposta.get('data', [])
            
            if contatos_lista and len(contatos_lista) > 0:
                contato_encontrado = contatos_lista[0]
                contato_id = contato_encontrado.get('id')
                nome_contato = contato_encontrado.get('nome', 'Sem nome')
                
                print(f"[BLINDAGEM] ✅ Contato encontrado por CNPJ:")
                print(f"[BLINDAGEM]    ID: {contato_id}")
                print(f"[BLINDAGEM]    Nome: {nome_contato}")
                print(f"[BLINDAGEM]    CNPJ: {cnpj_limpo}")
                
                return {
                    'id': contato_id,
                    'nome': nome_contato,
                    'numeroDocumento': cnpj_limpo,
                    'data_completa': contato_encontrado  # Guardar dados completos
                }
            else:
                print(f"[BLINDAGEM] ⚠️ Nenhum contato encontrado com CNPJ {cnpj_limpo}")
                return {}
        else:
            print(f"[BLINDAGEM] ⚠️ Erro HTTP {resp_busca.status_code} ao buscar por CNPJ")
            print(f"[BLINDAGEM]    Detalhes: {resp_busca.text[:200]}")
            return {}
            
    except Exception as e:
        print(f"[BLINDAGEM] ❌ Exception ao buscar contato por CNPJ: {type(e).__name__}: {str(e)}")
        return {}

def buscar_ou_criar_contato_blindado(bling_token: str, empresa_data: dict, criar_contato_func, vendedor_id=None, responsavel_nome=None) -> tuple:
    """
    FUNÇÃO BLINDADA COM DUPLA VERIFICAÇÃO E TRATAMENTO DE ERRO 400
    
    Fluxo:
    1. Extrair CNPJ do Bitrix
    2. **BUSCA DUPLA (Double Check):**
       a) Buscar por numeroDocumento (CNPJ)
       b) Se não encontrar, buscar por nome (pesquisa)
    3. Se ainda não encontrou, tentar criar POST /contatos
    4. **SE POST FALHAR COM ERRO 400 (CNPJ duplicado):**
       a) Extrair nome do contato da mensagem de erro
       b) Buscar pelo nome encontrado
       c) Se achar, retornar o ID com sucesso
    5. Se tudo falhar, retornar erro
    
    Args:
        bling_token: Token do Bling
        empresa_data: Dict com dados da empresa do Bitrix
        criar_contato_func: Função callback para criar novo contato
        vendedor_id: ID do vendedor no Bling (responsável do deal) - OPCIONAL
        responsavel_nome: Nome do responsável/representante do contato - OPCIONAL
    
    Returns:
        (contato_id: str, sucesso: bool, mensagem: str)
    """
    
    print(f"\n{'='*70}")
    print(f"[BLINDAGEM] ===== BUSCAR/CRIAR CONTATO (BLINDADO) =====")
    print(f"{'='*70}")
    
    # ============================================================================
    # PASSO 1: EXTRAIR CNPJ DO BITRIX
    # ============================================================================
    
    nome_empresa = empresa_data.get('TITLE', 'SEM NOME').strip()
    # 🔧 Campo correto do CNPJ (atualizado conforme indicação do usuário)
    cnpj_bruto = empresa_data.get('UF_CRM_1713291425', '').strip()  # Campo correto do CNPJ
    cnpj_limpo = ''.join(c for c in str(cnpj_bruto) if c.isdigit()) if cnpj_bruto else ''
    
    print(f"\n[BLINDAGEM] PASSO 1: Extraindo CNPJ")
    print(f"[BLINDAGEM]   Empresa: '{nome_empresa}'")
    print(f"[BLINDAGEM]   CNPJ bruto: {repr(cnpj_bruto)}")
    print(f"[BLINDAGEM]   CNPJ limpo: {repr(cnpj_limpo)} ({len(cnpj_limpo)} dígitos)")
    
    # ============================================================================
    # VALIDAÇÃO OBRIGATÓRIA: CNPJ DEVE ESTAR PREENCHIDO
    # ============================================================================
    
    if not cnpj_limpo or len(cnpj_limpo) != 14:
        erro_msg = f"❌ BLOQUEADO: Deal processado SEM CNPJ válido (recebeu: '{cnpj_bruto}'). Preencha o campo UF_CRM_1713291425 (CNPJ) no Bitrix com um CNPJ válido de 14 dígitos."
        print(f"\n[BLINDAGEM] {erro_msg}")
        return (None, True, erro_msg)
    
    # ============================================================================
    # PASSO 2: BUSCA DUPLA (Double Check)
    # ============================================================================
    
    print(f"\n[BLINDAGEM] PASSO 2: Busca Dupla (Double Check)")
    
    # 2A: Buscar por CNPJ (numeroDocumento)
    contato_encontrado = None
    
    if cnpj_limpo and len(cnpj_limpo) == 14:
        print(f"\n[BLINDAGEM] 2A: Buscar por CNPJ...")
        contato_encontrado = _buscar_contato_por_cnpj_bling(bling_token, cnpj_limpo)
    else:
        print(f"\n[BLINDAGEM] 2A: SKIPPED (CNPJ inválido: {len(cnpj_limpo)} dígitos)")
    
    # 2B: Se não encontrou por CNPJ, buscar por NOME
    if not contato_encontrado:
        print(f"\n[BLINDAGEM] 2B: Buscar por NOME (já que CNPJ não retornou)...")
        contato_encontrado = _buscar_contato_por_nome_bling(bling_token, nome_empresa)
    else:
        print(f"\n[BLINDAGEM] 2B: SKIPPED (contato já encontrado por CNPJ)")
    
    # Se encontrou em qualquer uma das buscas, retornar ID
    if contato_encontrado:
        contato_id = contato_encontrado.get('id')
        print(f"\n[BLINDAGEM] ✅ CONTATO ENCONTRADO NA BUSCA DUPLA")
        print(f"[BLINDAGEM]    ID: {contato_id}")
        print(f"[BLINDAGEM]    USANDO ESTE CONTATO (nenhum POST necessário)")
        return (contato_id, True, f"Contato encontrado no Bling: {contato_encontrado.get('nome', 'N/A')}")
    
    # ============================================================================
    # PASSO 3: NÃO ENCONTROU - TENTAR CRIAR NOVO
    # ============================================================================
    
    print(f"\n[BLINDAGEM] PASSO 3: Nenhum contato encontrado - TENTANDO CRIAR")
    
    try:
        # Adicionar responsável aos dados da empresa se fornecido
        if responsavel_nome:
            empresa_data['responsavel_representante'] = responsavel_nome
            print(f"[BLINDAGEM] 👤 Responsável adicionado: {responsavel_nome}")
        
        # Chamar callback para criar (passando vendedor_id se disponível)
        contato_result, contato_error = criar_contato_func(bling_token, empresa_data, vendedor_id=vendedor_id)
        
        if isinstance(contato_result, dict) and contato_result.get('id'):
            contato_id = contato_result.get('id')
            print(f"\n[BLINDAGEM] ✅ NOVO CONTATO CRIADO COM SUCESSO")
            print(f"[BLINDAGEM]    ID: {contato_id}")
            return (contato_id, True, f"Novo contato criado: {contato_result.get('nome', 'N/A')}")
        
        # Se chegou aqui, CREATE retornou erro
        erro_msg_create = contato_error or "Erro desconhecido na criação"
        print(f"\n[BLINDAGEM] ❌ CREATE FALHOU: {erro_msg_create}")
        
        # ============================================================================
        # PASSO 4: TRATAMENTO DO ERRO 400 (CNPJ duplicado)
        # ============================================================================
        
        # Verificar se a mensagem de erro contém indicação de CNPJ duplicado
        if "CNPJ" in erro_msg_create or "cadastrado" in erro_msg_create:
            print(f"\n[BLINDAGEM] PASSO 4: Detectado erro de CNPJ duplicado")
            print(f"[BLINDAGEM]    Extraindo nome do contato da mensagem de erro...")
            
            # Extrair nome do contato da mensagem
            nome_extraido = _extrair_nome_contato_do_erro_bling(erro_msg_create)
            
            if nome_extraido:
                print(f"[BLINDAGEM] ✅ Nome extraído: '{nome_extraido}'")
                print(f"[BLINDAGEM] 🔍 Fazendo NOVO GET /contatos?pesquisa={nome_extraido}...")
                
                # Buscar pelo nome extraído
                contato_fantasma = _buscar_contato_por_nome_bling(bling_token, nome_extraido)
                
                if contato_fantasma:
                    contato_id = contato_fantasma.get('id')
                    print(f"\n[BLINDAGEM] ✅✅ CONTATO 'FANTASMA' ENCONTRADO!")
                    print(f"[BLINDAGEM]    ID: {contato_id}")
                    print(f"[BLINDAGEM]    Nome: {contato_fantasma.get('nome', 'N/A')}")
                    print(f"[BLINDAGEM]    CNPJ: {contato_fantasma.get('numeroDocumento', 'N/A')}")
                    print(f"\n[BLINDAGEM] 🎉 BLINDAGEM FUNCIONOU!")
                    print(f"[BLINDAGEM]    O erro 400 foi capturado, nome foi extraído,")
                    print(f"[BLINDAGEM]    contato foi encontrado e fluxo continua...")
                    
                    return (contato_id, True, f"Contato encontrado via blindagem: {contato_fantasma.get('nome', 'N/A')}")
                else:
                    print(f"\n[BLINDAGEM] ⚠️ Nome extraído mas contato NÃO foi encontrado no GET")
                    print(f"[BLINDAGEM]    Retornando erro...")
                    return (None, False, f"Erro 400: CNPJ duplicado mas contato não foi encontrado (nome: '{nome_extraido}')")
            else:
                print(f"\n[BLINDAGEM] ⚠️ Não conseguiu extrair nome da mensagem de erro")
                print(f"[BLINDAGEM]    Resposta bruta: {erro_msg_create[:200]}")
                return (None, False, f"Erro 400: CNPJ duplicado mas não conseguiu extrair nome do contato")
        
        # Se não é erro de CNPJ duplicado, retorna erro genérico
        return (None, False, f"Erro ao criar contato: {erro_msg_create}")
        
    except Exception as e:
        print(f"\n[BLINDAGEM] ❌ Exception ao processar blindagem: {type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()
        return (None, False, f"Exceção ao processar contato: {str(e)}")

# ============================================================================
# MAPEAMENTO DE PRODUTOS: BITRIX → (CÓDIGO BLING, NOME EXATO BLING)
# ============================================================================
# IMPORTANTE: O NOME DEVE SER EXATO CONFORME CADASTRADO NO BLING
# Caso contrário, gera erro na criação de NF (nota fiscal)
# 
# Campos obrigatórios que NÃO podem ser modificados:
# - codigo: Código do produto (ex: "RF05RF", "PAV0010")
# - descricao: NOME EXATO como cadastrado no Bling
#
# FONTE: Dados confirmados do Bling API (2026-03-25)

PRODUTOS_BLING_MAPEAMENTO = {
    # Formato: "NOME_BITRIX": ("CÓDIGO_BLING", "NOME_EXATO_BLING")
    
    # 1. RECARGA DE PINCEL PARA QUADRO BRANCO
    "RECARGA DE PINCEL PARA QUADRO BRANCO": ("RF05RF", "Recarga de pincel para quadro branco"),
    "recarga de pincel para quadro branco": ("RF05RF", "Recarga de pincel para quadro branco"),
    
    # 2. PONTEIRA
    "PONTEIRAS PARA REPOSIÇÃO": ("PAV0016", "PONTEIRA"),
    "ponteiras para reposição": ("PAV0016", "PONTEIRA"),
    "PONTEIRA": ("PAV0016", "PONTEIRA"),
    "ponteira": ("PAV0016", "PONTEIRA"),
    
    # 3. MÁQUINA FILL INK INJECTOR
    "MÁQUINA FILL INK INJECTOR - 4G": ("PAV0500", "MÁQUINA FILL INK INJECTOR - 4G"),
    "máquina fill ink injector - 4g": ("PAV0500", "MÁQUINA FILL INK INJECTOR - 4G"),
    
    # 4. PINCÉIS FILL ECO MARKER
    "PINCÉIS FILL ECO MARKER": ("PAV1001", "PINCÉIS FILL ECO MARKER"),
    "pincéis fill eco marker": ("PAV1001", "PINCÉIS FILL ECO MARKER"),
    "pinceis fill eco marker": ("PAV1001", "PINCÉIS FILL ECO MARKER"),
    
    # 5. PINCEL FILL CHEIO - AZUL
    "PINCEL FILL CHEIO - AZUL": ("PAV0010", "PINCEL FILL CHEIO - AZUL"),
    "pincel fill cheio - azul": ("PAV0010", "PINCEL FILL CHEIO - AZUL"),
    
    # 6. PINCEL FILL CHEIO - PRETO
    "PINCEL FILL CHEIO - PRETO": ("PAV0009", "PINCEL FILL CHEIO - PRETO"),
    "pincel fill cheio - preto": ("PAV0009", "PINCEL FILL CHEIO - PRETO"),
    
    # 7. PINCEL FILL CHEIO - VERMELHO
    "PINCEL FILL CHEIO - VERMELHO": ("PAV0008", "PINCEL FILL CHEIO - VERMELHO"),
    "pincel fill cheio - vermelho": ("PAV0008", "PINCEL FILL CHEIO - VERMELHO"),
    
    # 8. PINCEL FILL VAZIO - AZUL
    "PINCEL FILL VAZIO - AZUL": ("PAV0004", "PINCEL FILL VAZIO - AZUL"),
    "pincel fill vazio - azul": ("PAV0004", "PINCEL FILL VAZIO - AZUL"),
    
    # 9. PINCEL FILL VAZIO - PRETO
    "PINCEL FILL VAZIO - PRETO": ("PAV0002", "PINCEL FILL VAZIO - PRETO"),
    "pincel fill vazio - preto": ("PAV0002", "PINCEL FILL VAZIO - PRETO"),
    
    # 10. PINCEL FILL VAZIO - VERMELHO
    "PINCEL FILL VAZIO - VERMELHO": ("PAV0003", "PINCEL FILL VAZIO - VERMELHO"),
    "pincel fill vazio - vermelho": ("PAV0003", "PINCEL FILL VAZIO - VERMELHO"),
    
    # 11. TINTA FILL 500ML - AZUL
    "TINTA FILL 500ML - AZUL": ("PAV0007", "TINTA FILL 500ML - AZUL"),
    "tinta fill 500ml - azul": ("PAV0007", "TINTA FILL 500ML - AZUL"),
    
    # 12. TINTA FILL 500ML - PRETA
    "TINTA FILL 500ML - PRETA": ("PAV0005", "TINTA FILL 500ML - PRETA"),
    "tinta fill 500ml - preta": ("PAV0005", "TINTA FILL 500ML - PRETA"),
    
    # 13. TINTA FILL 500ML - VERMELHA
    "TINTA FILL 500ML - VERMELHA": ("PAV0006", "TINTA FILL 500ML - VERMELHA"),
    "tinta fill 500ml - vermelha": ("PAV0006", "TINTA FILL 500ML - VERMELHA"),
    
    # 14. KIT APAGADOR
    "KIT APAGADOR POR PROFESSOR": ("PAV1003", "KIT APAGADOR: Estojo Apagador Fill Master Clean 3P; Capa Protetora"),
    "kit apagador por professor": ("PAV1003", "KIT APAGADOR: Estojo Apagador Fill Master Clean 3P; Capa Protetora"),
    "KIT APAGADOR": ("PAV1003", "KIT APAGADOR: Estojo Apagador Fill Master Clean 3P; Capa Protetora"),
    "kit apagador": ("PAV1003", "KIT APAGADOR: Estojo Apagador Fill Master Clean 3P; Capa Protetora"),
}

# ============================================================================
# ITEM FIXO: FONTE 1A 12V (ADICIONADA AUTOMATICAMENTE)
# ============================================================================
FONTE_ITEM_FIXO = {
    "codigo": "PAV0014",
    "descricao": "FONTE 1A 12V",
    "unidade": "UN",
    "quantidade": 1.0,
    "valor": 0.0
}

# Cache simples em memória para evitar várias consultas repetidas ao Bling
PRODUTO_ID_CACHE = {}

def _get_codigo_bling_para_produto(nome_produto):
    """
    Retorna (código_bling, nome_exato_bling) para um produto do Bitrix
    
    Args:
        nome_produto: Nome do produto conforme vem do Bitrix
    
    Returns:
        tuple: (codigo_bling, nome_exato_bling) ou (None, None) se não encontrado
    """
    if not nome_produto:
        return (None, None)
    
    nome_produto = nome_produto.strip()
    
    # Tentar correspondência exata
    if nome_produto in PRODUTOS_BLING_MAPEAMENTO:
        codigo, nome_exato = PRODUTOS_BLING_MAPEAMENTO[nome_produto]
        return (codigo, nome_exato)
    
    # Tentar correspondência normalizada (minúsculas + sem acentos)
    nome_lower = nome_produto.lower()
    if nome_lower in PRODUTOS_BLING_MAPEAMENTO:
        codigo, nome_exato = PRODUTOS_BLING_MAPEAMENTO[nome_lower]
        return (codigo, nome_exato)
    
    # 🔑 NOVA: Tentar com normalização de acentos (MÁQUINA == MAQUINA)
    nome_normalizado = _normalizar_nome_produto(nome_produto)
    for produto_mapeado in PRODUTOS_BLING_MAPEAMENTO.keys():
        produto_normalizado = _normalizar_nome_produto(produto_mapeado)
        if nome_normalizado == produto_normalizado:
            codigo, nome_exato = PRODUTOS_BLING_MAPEAMENTO[produto_mapeado]
            print(f"   [MAPPING] Corresponência (acentos): '{nome_produto}' → '{produto_mapeado}'")
            return (codigo, nome_exato)
    
    # Tentar correspondência parcial (se o nome contém o mapeado)
    for produto_mapeado, (codigo, nome_exato) in PRODUTOS_BLING_MAPEAMENTO.items():
        if produto_mapeado.lower() in nome_lower or nome_lower in produto_mapeado.lower():
            print(f"   [MAPPING] Corresponência parcial: '{nome_produto}' → '{produto_mapeado}'")
            print(f"   [MAPPING] Código Bling: {codigo}")
            print(f"   [MAPPING] Nome exato Bling: {nome_exato}")
            return (codigo, nome_exato)
    
    return (None, None)

def _normalizar_nome_produto(nome):
    """Remove acentos e converte para minúsculas para comparação"""
    if not nome:
        return ""
    # Remove acentos
    nfkd_form = unicodedata.normalize('NFKD', nome)
    return ''.join([c for c in nfkd_form if not unicodedata.combining(c)]).lower()

def _filtrar_produtos_validos(produtos):
    """
    Remove produtos bloqueados:
    - "Theikós" (em qualquer variação)
    - "TINTA FILL MASTER COLOR 500ML"
    - "PINCÉIS FILL ECO MARKER"
    
    Retorna:
        (lista de produtos filtrados, nomes dos bloqueados)
    """
    bloqueados = []
    validos = []
    
    # Lista de produtos a bloquear
    PRODUTOS_BLOQUEADOS = [
        'theikos',  # Variação normalizada
        'tinta fill master color',  # Será comparado normalizado
        'pincéis fill eco marker'    # Será comparado normalizado
    ]
    
    print(f"\n[HANDLER] 🔍 Filtragem de Produtos:")
    print(f"[HANDLER]    Total recebido: {len(produtos)}")
    sys.stdout.flush()
    
    for produto in produtos:
        nome_original = produto.get('PRODUCT_NAME', '').strip()
        nome_normalizado = _normalizar_nome_produto(nome_original)
        
        # Verificar se produto está na lista de bloqueados
        deve_bloquear = False
        for produto_bloqueado in PRODUTOS_BLOQUEADOS:
            if produto_bloqueado in nome_normalizado or nome_normalizado in produto_bloqueado:
                print(f"[HANDLER]    🚫 BLOQUEADO: '{nome_original}'")
                bloqueados.append(nome_original)
                deve_bloquear = True
                sys.stdout.flush()
                break
        
        if not deve_bloquear:
            print(f"[HANDLER]    ✅ VÁLIDO: '{nome_original}'")
            validos.append(produto)
            sys.stdout.flush()
    
    print(f"[HANDLER]    Resultado: {len(validos)} válido(s), {len(bloqueados)} bloqueado(s)")
    if bloqueados:
        print(f"[HANDLER]    Bloqueados: {', '.join(bloqueados)}")
    
    sys.stdout.flush()
    
    return validos, bloqueados

def load_processed_deals():
    """Carrega lista de deal IDs já processadas (evita duplicata)
    
    EXCEÇÃO: Deals em DEALS_LIVRES_PARA_TESTE não são bloqueadas por deduplicação
    """
    if WEBHOOK_PROCESSED_FILE.exists():
        try:
            with open(WEBHOOK_PROCESSED_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                deals = set(data.get('deals', []))
                print(f"[DEDUP] ✅ Carregou {len(deals)} deals processadas de {WEBHOOK_PROCESSED_FILE}")
                return deals
        except Exception as e:
            print(f"[DEDUP] ⚠️ Erro ao ler arquivo: {e}")
            return set()
    else:
        print(f"[DEDUP] 📝 Arquivo não existe ainda: {WEBHOOK_PROCESSED_FILE}")
        return set()

def save_processed_deal(deal_id):
    """Marca uma deal como processada no arquivo de cache
    
    Salva deal_id no arquivo JSON para evitar processar a mesma deal novamente
    """
    try:
        # Carregar deals já processadas
        if WEBHOOK_PROCESSED_FILE.exists():
            with open(WEBHOOK_PROCESSED_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                processed_deals = set(data.get('deals', []))
        else:
            processed_deals = set()
        
        # Adicionar nova deal
        processed_deals.add(str(deal_id))
        
        # Salvar arquivo
        with open(WEBHOOK_PROCESSED_FILE, 'w', encoding='utf-8') as f:
            json.dump({'deals': list(processed_deals)}, f, indent=2)
        
        print(f"[DEDUP] ✅ Deal #{deal_id} marcada como processada (total: {len(processed_deals)})")
        print(f"[DEDUP] 📁 Arquivo: {WEBHOOK_PROCESSED_FILE}")
        
    except Exception as e:
        print(f"[DEDUP] ⚠️ Erro ao salvar deal processada: {e}")

def buscar_produto_bling_por_codigo_webhook(access_token, codigo_produto):
    """
    Busca produto no Bling pelo código com:
    - cache em memória
    - pausa entre requisições
    - retry em HTTP 429
    """
    if not access_token or access_token == "FALLBACK_TOKEN":
        print(f"[PRODUTO] ⚠️ Token inválido - não buscando produto {codigo_produto}")
        return None

    codigo_chave = str(codigo_produto).strip().upper()

    if codigo_chave in PRODUTO_ID_CACHE:
        product_id = PRODUTO_ID_CACHE[codigo_chave]
        print(f"[PRODUTO-CACHE] HIT: {codigo_chave} → ID {product_id}")
        return product_id

    print(f"[PRODUTO-CACHE] MISS: {codigo_chave} - buscando no Bling...")

    BLING_API_BASE = "https://www.bling.com.br/Api/v3"
    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"{BLING_API_BASE}/produtos"

    def get_com_retry(params, descricao_busca):
        esperas = [2, 4, 6]

        for tentativa in range(1, 4):
            if tentativa > 1:
                espera = esperas[tentativa - 2]
                print(f"[BLING-RATE-LIMIT] Produto {codigo_chave}: aguardando {espera}s antes da tentativa {tentativa}/3...")
                time.sleep(espera)
            else:
                # pausa curta para respeitar limite de 3 req/s
                time.sleep(0.4)

            response = requests.get(url, headers=headers, params=params, timeout=15)

            if response.status_code == 429:
                print(f"[BLING-RATE-LIMIT] HTTP 429 ao buscar produto {codigo_chave} ({descricao_busca}) tentativa {tentativa}/3")
                continue

            return response

        return response

    try:
        # MÉTODO 1: Buscar com 'pesquisa'
        for tipo in ["P", "S"]:
            params = {
                "pesquisa": codigo_chave,
                "criterio": 2,
                "tipo": tipo,
                "limite": 20
            }

            response = get_com_retry(params, f"pesquisa/tipo={tipo}")

            if response.status_code == 200:
                result = response.json()
                produtos = result.get('data', [])

                for produto in produtos:
                    if str(produto.get('codigo', '')).upper() == codigo_chave:
                        product_id = produto.get('id')
                        PRODUTO_ID_CACHE[codigo_chave] = product_id
                        print(f"[PRODUTO] ✅ Encontrado: {codigo_chave} → ID {product_id}")
                        return product_id

            elif response.status_code == 429:
                print(f"[PRODUTO] ❌ Rate limit persistente ao buscar {codigo_chave}")
                return None

        # MÉTODO 2: Buscar com 'codigos[]'
        for tipo in ["P", "S"]:
            params = {
                "codigos[]": codigo_chave,
                "criterio": 2,
                "tipo": tipo,
                "limite": 10
            }

            response = get_com_retry(params, f"codigos/tipo={tipo}")

            if response.status_code == 200:
                result = response.json()
                produtos = result.get('data', [])

                for produto in produtos:
                    if str(produto.get('codigo', '')).upper() == codigo_chave:
                        product_id = produto.get('id')
                        PRODUTO_ID_CACHE[codigo_chave] = product_id
                        print(f"[PRODUTO] ✅ Encontrado: {codigo_chave} → ID {product_id}")
                        return product_id

            elif response.status_code == 429:
                print(f"[PRODUTO] ❌ Rate limit persistente ao buscar {codigo_chave}")
                return None

        print(f"[PRODUTO] ❌ NÃO ENCONTRADO: {codigo_chave}")
        return None

    except Exception as e:
        print(f"[PRODUTO] ❌ Erro ao buscar {codigo_chave}: {e}")
        return None

def is_deal_processed(deal_id):
    """Verifica se deal já foi processada - DEDUPLICAÇÃO ATIVADA
    
    Carrega arquivo de cache e verifica se deal_id está na lista de processadas
    """
    processed_deals = load_processed_deals()
    is_processed = str(deal_id) in processed_deals
    
    if is_processed:
        print(f"[DEDUP] ⏭️ Deal #{deal_id} JÁ FOI PROCESSADA - BLOQUEANDO (evitar duplicata)")
    else:
        print(f"[DEDUP] ✅ Deal #{deal_id} é novo - permitir processamento")
    
    return is_processed

# ============================================================================
# WEBHOOK HANDLER
# ============================================================================

def processar_webhook_bitrix(payload, bitrix_url, bling_endpoint_url):
    """
    VERSÃO SÍNCRONA: Processa webhook completamente ANTES de retornar.
    
    ⚠️ IMPORTANTE PARA VERCEL:
    Na Vercel (serverless), qualquer thread ou async iniciada APÓS o return
    é congelada e morta. Por isso esta função é 100% SÍNCRONA.
    
    Fluxo:
    1. Validar payload e stage
    2. Buscar dados do Bitrix (deal, empresa, contato, produtos)
    3. Validar produtos (não bloqueados)
    4. Criar contato no Bling
    5. Criar pedido no Bling
    6. Marcar deal como processada
    7. Retornar (sucesso, mensagem)
    
    Args:
        payload: dict do webhook do Bitrix
        bitrix_url: URL do webhook REST do Bitrix
        bling_endpoint_url: não usado nesta versão (mantido para compatibilidade)
    
    Returns:
        Tuple: (sucesso: bool, mensagem: str)
    """
    
    print(f"\n{'='*70}")
    print(f"[HANDLER] ===== WEBHOOK HANDLER INICIADO (SÍNCRONO) =====")
    print(f"{'='*70}\n")
    sys.stdout.flush()  # ⚠️ FORÇA SAÍDA IMEDIATA

    # Garantir que a URL do Bitrix nunca venha None
    bitrix_url = (bitrix_url or os.getenv("BITRIX_WEBHOOK_URL", "")).strip()

    if bitrix_url and not bitrix_url.endswith("/"):
        bitrix_url += "/"

    if not bitrix_url:
        msg = "BITRIX_WEBHOOK_URL não configurada ou não recebida pelo handler"
        print(f"[HANDLER] ❌ {msg}")
        return (False, msg)

    
    try:
        # ====================================================================
        # 1. EXTRAIR E VALIDAR DADOS DO PAYLOAD
        # ====================================================================
        
        print("[HANDLER] 1️⃣ Validando payload e stage...")
        sys.stdout.flush()
        
        event = payload.get('event', '')
        data = payload.get('data', {})
        fields = data.get('FIELDS', {})
        
        # Validar evento
        if event != 'ONCRMDEALUPDATE':
            msg = f"Evento inválido: {event} (esperado: ONCRMDEALUPDATE)"
            print(f"[HANDLER] ⏭️ {msg}")
            return (False, msg)
        
        # Extrair Deal ID
        deal_id = fields.get('ID')
        if not deal_id:
            msg = "Deal ID não encontrado no payload"
            print(f"[HANDLER] ❌ {msg}")
            return (False, msg)
        
        stage_id = fields.get('STAGE_ID', '').upper()
        deal_title = fields.get('TITLE', 'Deal')
        
        # ⚠️ LOG EXPLÍCITO DO STAGE RECEBIDO (IMPORTANTE PARA DEBUG)
        print(f"[HANDLER] 🔍 STAGE RECEBIDO DO BITRIX: '{stage_id}' (tipo: {type(stage_id).__name__})")
    
        if not stage_id or stage_id == '':
            print(f"[HANDLER] ⚠️ STAGE VAZIO NO WEBHOOK! Buscando da API do Bitrix...")
            try:
                # Montar headers do Bitrix
                bitrix_headers = {'Content-Type': 'application/json'}
                
                # Chamar API para buscar dados da deal
                resp_deal = requests.post(
                    bitrix_url + 'crm.deal.get',
                    json={'id': deal_id},
                    headers=bitrix_headers,
                    timeout=15
                )
                
                if resp_deal.status_code == 200:
                    deal_data = resp_deal.json().get('result', {})
                    stage_id = deal_data.get('STAGE_ID', '').upper()
                    deal_title = deal_data.get('TITLE', deal_title)

                    moved_time_raw = deal_data.get('MOVED_TIME') or deal_data.get('DATE_MODIFY') or ''
                    print(f"[HANDLER] ✅ Stage obtido via API: '{stage_id}'")
                    print(f"[HANDLER] 🕒 MOVED_TIME/DATE_MODIFY da deal: {repr(moved_time_raw)}")
                else:
                    print(f"[HANDLER] ⚠️ Erro ao buscar dados da deal: HTTP {resp_deal.status_code}")
                    stage_id = ''
                    deal_data = {}
                    moved_time_raw = ''
                    
            except Exception as e:
                print(f"[HANDLER] ❌ Erro ao buscar stage via API: {e}")
                stage_id = ''
                deal_data = {}
                moved_time_raw = ''
        
        # 🔐 Validação RIGOROSA: APENAS estágio de conclusão (WON)
        # Stages que indicam deal concluída/ganha no Bitrix
        STAGES_VALIDOS = ['WON']
        
        # Se vazio mesmo depois de tentar buscar, rejeitar
        if not stage_id or stage_id == '':
            msg = f"❌ STAGE FINAL VAZIO APÓS TUDO! REJEITAR A DEAL!"
            print(f"[HANDLER] {msg}")
            return (False, msg)
        
        # Early return: se não for estágio de conclusão, interromper imediatamente
        if stage_id not in STAGES_VALIDOS:
            print(f"[HANDLER] ⏭️ Deal ignorada: Não está na etapa Concluído")
            print(f"[HANDLER]    Stage recebido: '{stage_id}' (Esperado: {STAGES_VALIDOS[0]})")
            return (False, f"Stage inválido: {stage_id}")
        
        print(f"[HANDLER] ✅ Stage '{stage_id}' é válido para processamento")

                # ====================================================================
        # 1.1 TRAVA: PROCESSAR SOMENTE SE CAIU EM WON AGORA
        # ====================================================================
        try:
            moved_time_raw = ''
            if 'deal_data' in locals() and isinstance(deal_data, dict):
                moved_time_raw = deal_data.get('MOVED_TIME') or deal_data.get('DATE_MODIFY') or ''

            if moved_time_raw:
                moved_time_str = str(moved_time_raw).replace('Z', '+00:00')

                try:
                    moved_dt = datetime.fromisoformat(moved_time_str)
                except ValueError:
                    moved_dt = None

                if moved_dt:
                    now_dt = datetime.now(moved_dt.tzinfo) if moved_dt.tzinfo else datetime.now()
                    diff_seconds = abs((now_dt - moved_dt).total_seconds())

                    print(f"[HANDLER] 🕒 Validação de entrada em WON:")
                    print(f"[HANDLER]    MOVED_TIME/DATE_MODIFY: {moved_time_raw}")
                    print(f"[HANDLER]    Diferença em segundos: {diff_seconds:.0f}")

                    if diff_seconds > 300:
                        msg = (
                            f"Deal #{deal_id} já estava em Concluído antes "
                            f"(MOVED_TIME/DATE_MODIFY: {moved_time_raw}). Ignorando."
                        )
                        print(f"[HANDLER] ⏭️ {msg}")
                        return (False, msg)
                    else:
                        print(f"[HANDLER] ✅ Deal entrou/foi atualizada recentemente em WON - permitido processar")
                else:
                    print(f"[HANDLER] ⚠️ Não foi possível interpretar MOVED_TIME/DATE_MODIFY. Continuando.")
            else:
                print(f"[HANDLER] ⚠️ MOVED_TIME/DATE_MODIFY não encontrado. Continuando.")

        except Exception as e:
            print(f"[HANDLER] ⚠️ Erro na validação de MOVED_TIME/DATE_MODIFY: {e}")
            print(f"[HANDLER] ⚠️ Continuando.")
        
        # ====================================================================
        # 1.5 VERIFICAR DEDUPLICAÇÃO (evitar processar mesma deal 2x)
        # ====================================================================
        # ✅ CRÍTICO: Verificar se deal já foi processada
        # 🧪 COMENTADO PARA TESTES: Verificação anti-duplicata
        # Isso evita que o mesmo pedido seja criado múltiplas vezes quando:
        # - O webhook é acionado múltiplas vezes pelo Bitrix (retry)
        # - Há múltiplos webhooks configurados
        # - Um webhook é processado duas vezes em paralelo
        # ✅ ATIVADO: Agora verifica se já foi processada
        if is_deal_processed(deal_id):
            msg = f"Deal #{deal_id} já foi processada anteriormente (DUPLICATE - IGNORAR)"
            print(f"[HANDLER] ⏭️ {msg}")
            print(f"[HANDLER] ℹ️  Se ela foi processada incorretamente, delete o arquivo em .webhook_processed_deals.json")
            return (False, msg)
        print(f"[HANDLER] ✅ Deduplicação ATIVA - primeiro processamento desta deal")
        
        print(f"[HANDLER] ✅ Payload válido")
        print(f"[HANDLER]    Deal ID: {deal_id}")
        print(f"[HANDLER]    Stage: {stage_id}")
        print(f"[HANDLER]    Título: {deal_title}")
        
        # ====================================================================
        # 2. BUSCAR DADOS DO BITRIX
        # ====================================================================
        
        print(f"\n[HANDLER] 2️⃣ Buscando dados do Bitrix...")
        
        # Headers para Bitrix (MUITO IMPORTANTE: Connection: close)
        bitrix_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            'Connection': 'close'
        }
        
        # Buscar dados da deal
        try:
            print(f"[HANDLER]    → Buscando deal #{deal_id}...")
            resp_deal = requests.post(
                bitrix_url + 'crm.deal.get',
                json={'id': deal_id},
                headers=bitrix_headers,
                timeout=15
            )
            
            if resp_deal.status_code != 200:
                error_detail = resp_deal.text[:200]
                msg = f"Erro Bitrix ao buscar deal: HTTP {resp_deal.status_code} - {error_detail}"
                print(f"[HANDLER] ❌ {msg}")
                return (False, msg)
            
            deal_data = resp_deal.json().get('result', {})
            print(f"[HANDLER]    ✅ Deal encontrada")
            
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # DIAGNÓSTICO: MOSTRAR TODOS OS CAMPOS DA DEAL (CAMPOS CHAVE)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            print(f"\n[DIAG] {'='*60}")
            print(f"[DIAG] 🔎 CAMPOS DA DEAL #{deal_id} (Bitrix API)")
            print(f"[DIAG] {'='*60}")
            campos_importantes = ['ID', 'TITLE', 'STAGE_ID', 'ASSIGNED_BY_ID', 'COMPANY_ID', 'CONTACT_ID', 'OPPORTUNITY']
            for campo in campos_importantes:
                print(f"[DIAG]   {campo}: {repr(deal_data.get(campo))}")
            # Mostrar TODOS os campos com ASSIGNED no nome
            campos_assigned = {k: v for k, v in deal_data.items() if 'ASSIGN' in k.upper()}
            if campos_assigned:
                print(f"[DIAG] Campos ASSIGN encontrados: {campos_assigned}")
            else:
                print(f"[DIAG] ⚠️ NENHUM campo ASSIGN na deal_data!")
                print(f"[DIAG] Todos os campos disponíveis: {list(deal_data.keys())}")
            print(f"[DIAG] {'='*60}\n")
            
            # ════════════════════════════════════════════════════════════════
            # 🔥 CAPTURA CRITERIOSA DO ASSIGNED_BY_ID DA API (NÃO DO PAYLOAD)
            # ════════════════════════════════════════════════════════════════
            print(f"\n[HANDLER] 🔍 === EXTRAÇÃO CRITERIOSA DO RESPONSÁVEL ===")
            print(f"[HANDLER]    1️⃣  Tentando extrair ASSIGNED_BY_ID da resposta da API...")
            
            assigned_by_id_from_api = deal_data.get('ASSIGNED_BY_ID')
            print(f"[HANDLER]       • ASSIGNED_BY_ID (API): {repr(assigned_by_id_from_api)} (tipo: {type(assigned_by_id_from_api).__name__})")
            
            # Se estiver vazio/None, tentar do payload
            assigned_by_id_from_payload = fields.get('ASSIGNED_BY_ID')
            print(f"[HANDLER]    2️⃣  ASSIGNED_BY_ID (PAYLOAD): {repr(assigned_by_id_from_payload)} (tipo: {type(assigned_by_id_from_payload).__name__})")
            
            # Usar da API (mais confiável), fallback para payload
            assigned_by_id_final = assigned_by_id_from_api or assigned_by_id_from_payload
            print(f"[HANDLER]    3️⃣  ASSIGNED_BY_ID (FINAL): {repr(assigned_by_id_final)}")
            
            if assigned_by_id_final:
                print(f"[HANDLER] ✅ RESPONSÁVEL CAPTURADO: {repr(assigned_by_id_final)}")
            else:
                print(f"[HANDLER] ⚠️  RESPONSÁVEL VAZIO! Deal não tem responsável definido")
                print(f"[HANDLER]    Payload fields completo: {dict(list(fields.items())[:10])}")
            
            print(f"[HANDLER] === FIM EXTRAÇÃO ===")
            
        except Exception as e:
            msg = f"Exception ao buscar deal: {type(e).__name__}: {str(e)}"
            print(f"[HANDLER] ❌ {msg}")
            return (False, msg)
        
        # Buscar empresa
        empresa = {}
        company_id = fields.get('COMPANY_ID')
        
        # DEBUG: Verificar se COMPANY_ID veio no payload
        if not company_id:
            print(f"[HANDLER] ⚠️ COMPANY_ID vazio! Tentando extrair de deal_data...")
            company_id = deal_data.get('COMPANY_ID')
        
        print(f"[HANDLER]    📍 COMPANY_ID: {company_id}")
        
        if company_id:
            try:
                print(f"[HANDLER]    → Buscando empresa #{company_id}...")
                resp_empresa = requests.post(
                    bitrix_url + 'crm.company.get',
                    json={'id': company_id},
                    headers=bitrix_headers,
                    timeout=15
                )
                
                if resp_empresa.status_code == 200:
                    empresa = resp_empresa.json().get('result', {})
                    titulo_empresa = empresa.get('TITLE', '')
                    print(f"[HANDLER]    ✅ Empresa encontrada: '{titulo_empresa}'")
                    
                    # DEBUG: Confirmar que tem nome real
                    if not titulo_empresa or titulo_empresa == 'Cliente':
                        print(f"[HANDLER] ⚠️ ⚠️ ALERTA: Nome da empresa vazio ou genérico!")
                else:
                    print(f"[HANDLER] ⚠️ Erro HTTP {resp_empresa.status_code} ao buscar empresa")
                    
            except Exception as e:
                print(f"[HANDLER]    ❌ Erro ao buscar empresa: {e}")
        else:
            print(f"[HANDLER] ❌ Sem COMPANY_ID - não pode buscar empresa")
        
        # Buscar contato
        contato = {}
        contact_id = fields.get('CONTACT_ID')
        
        print(f"[HANDLER]    📍 CONTACT_ID: {contact_id}")
        
        if contact_id:
            try:
                print(f"[HANDLER]    → Buscando contato #{contact_id}...")
                resp_contato = requests.post(
                    bitrix_url + 'crm.contact.get',
                    json={'id': contact_id},
                    headers=bitrix_headers,
                    timeout=15
                )
                
                if resp_contato.status_code == 200:
                    contato = resp_contato.json().get('result', {})
                    print(f"[HANDLER]    ✅ Contato encontrado: {contato.get('NAME', 'Unknown')}")
                else:
                    print(f"[HANDLER] ⚠️ Erro HTTP {resp_contato.status_code} ao buscar contato")
                    
            except Exception as e:
                print(f"[HANDLER]    ⚠️ Erro ao buscar contato: {e}")
        
        # Buscar produtos
        produtos = []
        try:
            print(f"[HANDLER]    → Buscando produtos...")
            resp_produtos = requests.post(
                bitrix_url + 'crm.deal.productrows.get',
                json={'id': deal_id},
                headers=bitrix_headers,
                timeout=15
            )
            
            if resp_produtos.status_code == 200:
                produtos = resp_produtos.json().get('result', [])
                print(f"[HANDLER]    ✅ {len(produtos)} produto(s) encontrado(s)")
                
        except Exception as e:
            print(f"[HANDLER]    ⚠️ Erro ao buscar produtos: {e}")
        
        if not produtos:
            msg = "❌ Deal sem produtos (não será processada)"
            print(f"[HANDLER] ⏭️ {msg}")
            sys.stdout.flush()
            return (False, msg)
        
        # ====================================================================
        # 3. FILTRAR PRODUTOS BLOQUEADOS
        # ====================================================================
        
        print(f"\n[HANDLER] 3️⃣ Analisando produtos...")
        sys.stdout.flush()
        
        produtos_validos, produtos_bloqueados = _filtrar_produtos_validos(produtos)
        
        if not produtos_validos and produtos_bloqueados:
            msg = f"❌ Deal contém APENAS produtos bloqueados ({len(produtos_bloqueados)})"
            print(f"[HANDLER] ⏭️ {msg}")
            for p in produtos_bloqueados:
                print(f"[HANDLER]    • {p}")
            sys.stdout.flush()
            return (False, msg)
        
        if not produtos_validos:
            msg = "❌ Deal sem produtos válidos"
            print(f"[HANDLER] ⏭️ {msg}")
            sys.stdout.flush()
            return (False, msg)
        
        print(f"[HANDLER] ✅ {len(produtos_validos)} produto(s) válido(s) para processar")
        sys.stdout.flush()
        
        # ====================================================================
        # 4. CRIAR CONTATO NO BLING - BUSCAR PRIMEIRO, DEPOIS CRIAR
        # ====================================================================
        
        print(f"\n[HANDLER] 4️⃣ Processando contato no Bling (Buscar → Criar)...")
        
        # Preparar dados de empresa para a função
        empresa_data = {
            **empresa,           # Todos os dados da empresa do Bitrix
            'bitrix_company_id': company_id
        }
        
        # DEBUG: Confirmar Nome da Empresa que será usado
        nome_empresa_bling = empresa_data.get('TITLE', 'SEM NOME').strip()
        print(f"[HANDLER]    📝 Nome da empresa para Bling: '{nome_empresa_bling}'")
        
        # DEBUG: Confirmar CNPJ da empresa (tentar múltiplos campos)
        cnpj_empresa_bitrix = ''
        campos_cnpj_debug = [
            'UF_CRM_1713291425',  # Campo correto identificado
            'UF_CRM_1713291425',  # Campo correto do CNPJ
        ]
        for campo in campos_cnpj_debug:
            valor = empresa_data.get(campo, '').strip()
            if valor and valor.lower() != 'none':
                cnpj_empresa_bitrix = valor
                print(f"[HANDLER]    💳 CNPJ encontrado em {campo}: '{valor}'")
                break
        
        cnpj_limpo_debug = ''.join(c for c in str(cnpj_empresa_bitrix) if c.isdigit()) if cnpj_empresa_bitrix else ''
        print(f"[HANDLER]    📋 CNPJ do Bitrix: '{cnpj_empresa_bitrix}' → Limpo: '{cnpj_limpo_debug}'")
        
        # ====================================================================
        # 🚫 VALIDAÇÃO CRÍTICA: BLOQUEIO DE CONTATO ALEATÓRIO
        # ====================================================================
        # NUNCA deve usar contato aleatório quando empresa do Bitrix não é válida!
        # Se não temos CNPJ E não temos nome válido da empresa, REJEITAR
        
        empresa_tem_cnpj_valido = bool(cnpj_limpo_debug and len(cnpj_limpo_debug) >= 11)
        empresa_tem_nome_valido = bool(nome_empresa_bling and nome_empresa_bling not in ['', 'SEM NOME', 'Cliente', 'Empresa'])
        empresa_e_valida = empresa_tem_cnpj_valido or empresa_tem_nome_valido
        
        print(f"\n[HANDLER] 🔒 VALIDAÇÃO DE EMPRESA:")
        print(f"[HANDLER]    ✓ CNPJ válido? {empresa_tem_cnpj_valido} ('{cnpj_limpo_debug}')")
        print(f"[HANDLER]    ✓ Nome válido? {empresa_tem_nome_valido} ('{nome_empresa_bling}')")
        print(f"[HANDLER]    ✓ Empresa é válida? {empresa_e_valida}")
        
        if not empresa_e_valida:
            msg = f"""❌ BLOQUEADO: Empresa inválida do Bitrix
[HANDLER]    • Nome da Deal: {deal_title}
[HANDLER]    • Company ID: {company_id} (vazio? {company_id is None or company_id == ''})
[HANDLER]    • Nome empresa: '{nome_empresa_bling}' (vazio ou genérico)
[HANDLER]    • CNPJ empresa: '{cnpj_limpo_debug}' (< 11 dígitos)
[HANDLER]    
[HANDLER]    🚫 NÃO é permitido usar contato aleatório do Bling!
[HANDLER]    ℹ️ Solução: Vinculare uma empresa válida no Bitrix para este deal"""
            print(f"[HANDLER] {msg}")
            return (False, msg)
        
        # DEBUG: Responsável/Representante
        responsavel_bitrix = empresa_data.get('UF_CRM_1721072755', '')
        print(f"[HANDLER]    👤 Responsável/Representante: '{responsavel_bitrix}'")
        
        # Token do Bling (com renovação automática via Vercel KV)
        bling_token = get_valid_bling_token()
        if not bling_token:
            msg = "Falha ao obter token Bling (KV ou Variável de Ambiente)"
            print(f"[HANDLER] ❌ {msg}")
            return (False, msg)
        
        # ===== IMPORTS LOCAIS (evitar ciclo de imports) =====
        # IMPORTANTE: Tentar backend PRIMEIRO pois há conflito com /api/index.py no root
        try:
            from backend.api.index import criar_contato_bling, buscar_vendedor_por_nome_flexivel
        except ImportError:
            try:
                from api.index import criar_contato_bling, buscar_vendedor_por_nome_flexivel
            except ImportError:
                print(f"[HANDLER] ❌ Não conseguiu importar criar_contato_bling")
                return (False, "Função criar_contato_bling não disponível")
        
        try:
            from backend.api.pedidos_vendas import criar_pedido_venda_bling
        except ImportError:
            try:
                from api.pedidos_vendas import criar_pedido_venda_bling
            except ImportError:
                print(f"[HANDLER] ❌ Não conseguiu importar criar_pedido_venda_bling")
                return (False, "Função criar_pedido_venda_bling não disponível")
        
        try:
            # ====================================================================
            # MAPEAR VENDEDOR BITRIX PARA BLING (responsável do deal)
            # ====================================================================
            
            # ════════════════════════════════════════════════════════════════
            # RESOLUÇÃO DO VENDEDOR: CACHE → MAPA → NOME
            # NUNCA usa Nayara como padrão para outros representantes!
            # ════════════════════════════════════════════════════════════════
            print(f"\n[HANDLER] 👤 === RESOLUÇÃO DO VENDEDOR ===")
            print(f"[HANDLER]    ASSIGNED_BY_ID a usar: {repr(assigned_by_id_final)}")
            
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # DIAGNÓSTICO: STATUS DO CACHE E IDs DISPONÍVEIS
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            print(f"[DIAG] Cache status: {'✅ carregado' if _HANDLER_CACHE else '❌ NÃO carregado'}")
            if _HANDLER_CACHE:
                print(f"[DIAG] IDs no cache de usuários: {sorted(_HANDLER_CACHE.users_cache.keys())}")
                print(f"[DIAG] IDs no cache de vendedores: {sorted(_HANDLER_CACHE.vendors_cache.keys())}")
            else:
                print(f"[DIAG] ⚠️ Cache ausente - usando apenas mapa estático")
            print(f"[DIAG] Mapa estático IDs: {list(MAPA_NOMES_REPRESENTANTES.keys())}")
            
            bitrix_assigned_id = str(assigned_by_id_final) if assigned_by_id_final else None
            vendedor_bling_id = None
            nome_vendedor = None

            # Passo 1: Resolver NOME pelo cache (mais completo)
            if _HANDLER_CACHE and bitrix_assigned_id:
                nome_vendedor = _HANDLER_CACHE.get_user_name(bitrix_assigned_id)
                if nome_vendedor:
                    print(f"[HANDLER] ✅ [1/4] Nome do cache: '{nome_vendedor}' (ID Bitrix: {bitrix_assigned_id})")

            # Passo 2: Fallback para mapa estático de nomes
            if not nome_vendedor:
                nome_vendedor = MAPA_NOMES_REPRESENTANTES.get(bitrix_assigned_id or '')
                if nome_vendedor:
                    print(f"[HANDLER] ✅ [2/4] Nome do mapa estático: '{nome_vendedor}' (ID Bitrix: {bitrix_assigned_id})")

            # Passo 3: Fallback para API do Bitrix (busca o nome real do usuário)
            if not nome_vendedor and bitrix_assigned_id:
                print(f"[HANDLER] ⚠️ [3/4] ID {bitrix_assigned_id} não encontrado no cache nem no mapa - consultando API Bitrix...")
                nome_vendedor = _buscar_nome_usuario_bitrix(bitrix_url, bitrix_headers, bitrix_assigned_id)
                if not nome_vendedor:
                    print(f"[HANDLER] ❌ [3/4] API Bitrix também não retornou nome para ID {bitrix_assigned_id}")

            # Passo 4: NÃO usar ID fixo como decisão final.
            # O vendedor deve ser resolvido pelo NOME no Bling.
            # Se o nome não existir no Bling, vendedor_bling_id fica None.
            vendedor_bling_id = None

            if nome_vendedor:
                print(f"[HANDLER] 🔍 [4/4] Buscando vendedor no Bling pelo nome: '{nome_vendedor}'")
                vendedor_bling_id = buscar_vendedor_por_nome_flexivel(bling_token, nome_vendedor)

                if vendedor_bling_id:
                    print(f"[HANDLER] ✅ Vendedor encontrado no Bling:")
                    print(f"[HANDLER]    Nome Bitrix: '{nome_vendedor}'")
                    print(f"[HANDLER]    ID Bling: {vendedor_bling_id}")
                else:
                    print(f"[HANDLER] ⚠️ Vendedor NÃO encontrado no Bling:")
                    print(f"[HANDLER]    Nome Bitrix: '{nome_vendedor}'")
                    print(f"[HANDLER]    Contato e pedido serão criados SEM vendedor")
            else:
                print(f"[HANDLER] ⚠️ Nome do vendedor vazio - contato e pedido serão criados SEM vendedor")

            print(f"[HANDLER]    Resultado: nome='{nome_vendedor}', bling_id={vendedor_bling_id}")
            print(f"[HANDLER] === FIM RESOLUÇÃO ===")

            
            # ====================================================================
            # CRIAR CONTATO DIRETAMENTE NO BLING
            # ====================================================================
            
            print(f"[HANDLER]    Criando/buscando contato direto no Bling...")
            
            # Adicionar responsável/vendedor se disponível
            if responsavel_bitrix:
                empresa_data['responsavel_representante'] = responsavel_bitrix
                print(f"[HANDLER]    👤 Responsável adicionado: {responsavel_bitrix}")
            
            # Adicionar nome do vendedor resolvido ao dicionário de dados
            empresa_data['vendor_name_final'] = nome_vendedor
            empresa_data['vendor_id_final'] = vendedor_bling_id
            print(f"[HANDLER]    👤 Vendedor adicionado aos dados: {nome_vendedor} (ID: {vendedor_bling_id})")
            
            # Chamar função de criação/busca de contato
            # ⚠️ CRÍTICO: Passar vendedor_nome E vendedor_id para garantir que ambos são usados no Bling
            vendedor_nome_para_contato = nome_vendedor if vendedor_bling_id else None

            contato_result, contato_error = criar_contato_bling(
                bling_token,
                empresa_data,
                vendedor_nome=vendedor_nome_para_contato,
                vendedor_id=vendedor_bling_id
            )

            
            # Extrair ID do resultado
            contato_id_bling = None
            if isinstance(contato_result, dict):
                contato_id_bling = contato_result.get('id')
            
            if not contato_id_bling:
                msg = f"Falha ao criar contato no Bling: {contato_error or 'ID não retornado'}"
                print(f"[HANDLER] ❌ {msg}")
                return (False, msg)
            
            print(f"[HANDLER] ✅ Contato disponível (ID: {contato_id_bling})")
            
        except Exception as e:
            msg = f"Exception ao processar contato Bling (blindagem): {type(e).__name__}: {str(e)}"
            print(f"[HANDLER] ❌ {msg}")
            import traceback
            traceback.print_exc()
            return (False, msg)
        
        # ====================================================================
        # 5. CRIAR PEDIDO DE VENDA NO BLING - USANDO FUNÇÃO EXISTENTE
        # ====================================================================
        
        print(f"\n[HANDLER] 5️⃣ Criando pedido de venda no Bling...")
        
        # Montar itens do pedido com estrutura CORRETA: "produto": {"id": ...}
        itens = []
        for produto in produtos_validos:
            try:
                quantidade = float(produto.get('QUANTITY', 1.0))
                preco = float(produto.get('PRICE', 0))
                product_id = produto.get('PRODUCT_ID')
                product_name = produto.get('PRODUCT_NAME', '').strip()
                
                # Garantir que sempre há descrição (obrigatório no Bling)
                if not product_name:
                    product_name = f"Produto {product_id}" if product_id else "Produto"
                
                # ✅ BUSCAR CÓDIGO E NOME EXATO NO BLING (DUPLA RETORNADA)
                codigo_bling, nome_exato_bling = _get_codigo_bling_para_produto(product_name)
                
                if not codigo_bling or not nome_exato_bling:
                    # ❌ STRICT: Produto não está mapeado - NUNCA usar nome do Bitrix
                    print(f"[HANDLER]    ❌ ERRO: Produto '{product_name}' NÃO ENCONTRADO no mapeamento!")
                    print(f"[HANDLER]       📍 Product ID Bitrix: {product_id}")
                    print(f"[HANDLER]       ⚠️ Este produto NÃO será enviado para o Bling!")
                    print(f"[HANDLER]       ℹ️ Adicione '{product_name}' ao PRODUTOS_BLING_MAPEAMENTO em webhook_handler.py")
                    continue  # ← PULA este produto, não envia
                else:
                    print(f"[HANDLER]    ✅ Produto mapeado: '{product_name}' → '{codigo_bling}'")
                
                # 🔍 BUSCAR ID DO PRODUTO NO BLING (CRÍTICO!)
                id_produto_bling = buscar_produto_bling_por_codigo_webhook(bling_token, codigo_bling)
                
                if not id_produto_bling:
                    # ❌ CRÍTICO: Produto não encontrado no Bling
                    print(f"[HANDLER]    ❌ ERRO CRÍTICO: Produto '{codigo_bling}' NÃO ENCONTRADO NO BLING!")
                    print(f"[HANDLER]       ⚠️ Sem ID interno, não pode criar item no pedido")
                    print(f"[HANDLER]       ℹ️ Cadastre '{codigo_bling}' no Bling ou corrija o código no mapeamento")
                    print(f"[HANDLER]       Este item será IGNORADO do pedido")
                    continue  # ← PULA este produto, não envia
                else:
                    print(f"[HANDLER]    ✅ ID encontrado: {id_produto_bling}")
                
                # ✅ ESTRUTURA CORRETA PARA BLING API v3
                # OBRIGATÓRIO: "produto": {"id": id_interno}
                item = {
                    "produto": {"id": id_produto_bling},  # ← ID OBRIGATÓRIO!
                    "quantidade": quantidade,
                    "valor": float(preco),
                    "unidade": "UN",
                    "descricao": nome_exato_bling,
                    "aliquotaIPI": 0
                }
                itens.append(item)
                print(f"[HANDLER]    ✅ Item adicionado com ID: {id_produto_bling}\n")
                
            except (ValueError, TypeError) as e:
                print(f"[HANDLER]    ⚠️ Erro ao processar produto {produto}: {e}")
                continue
        
        print(f"[HANDLER]    {len(itens)} item(s) no pedido")
        
        # ====================================================================
        # ADICIONAR ITEM FIXO: FONTE 1A 12V
        # ====================================================================
        # Este item é adicionado AUTOMATICAMENTE em todo pedido com valor R$ 0
        print(f"\n[HANDLER] 📦 Adicionando item fixo: FONTE 1A 12V...")
        
        try:
            # 🔍 Buscar ID da FONTE no Bling
            id_fonte_bling = buscar_produto_bling_por_codigo_webhook(bling_token, "PAV0014")
            
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
                print(f"[HANDLER]    ✅ FONTE adicionada COM ID: {id_fonte_bling}")
            else:
                # ❌ Não encontrou no Bling
                print(f"[HANDLER]    ❌ FONTE não encontrada no Bling (procurado: PAV0014)")
                print(f"[HANDLER]    ⚠️ IGNORANDO item fixo para evitar alerta")
                item_fonte = None
            
            if item_fonte:
                itens.append(item_fonte)
                print(f"[HANDLER]    Total: {len(itens)} item(s) (incluindo FONTE)")
            else:
                print(f"[HANDLER]    Total: {len(itens)} item(s) (FONTE não adicionada)")
                
        except Exception as e:
            print(f"[HANDLER]    ⚠️ Erro ao adicionar FONTE: {e}")
        
        try:
            # Data prevista para geração de parcelas (30 dias a partir de hoje)
            hoje = datetime.now()
            data_parcelas = (hoje + timedelta(days=30)).strftime('%Y-%m-%d')
            
            # 🔑 GERAR IDENTIFICADOR ÚNICO PARA EVITAR REJEIÇÃO DE DUPLICATA PELO BLING
            # O Bling rejeita vendas com "Informações idênticas à última venda salva"
            # Adicionar timestamp torna cada venda única mesmo testando a mesma deal
            timestamp_unico = hoje.isoformat()
            
            # ⚠️ USAR O VENDEDOR JÁ RESOLVIDO (não buscar novamente)
            # O nome_vendedor foi resolvido acima a partir do ASSIGNED_BY_ID da API
            # Usar diretamente em vez de buscar novamente
            print(f"[HANDLER]    👤 Representante (já resolvido): {nome_vendedor} (ID Bitrix: {bitrix_assigned_id})")
            print(f"[HANDLER]    🆔 Vendedor Bling ID: {vendedor_bling_id}")
            
            # Calcular valor total dos itens
            valor_total = 0.0
            for item in itens:
            # Se não tem valor no item, tenta pegar do montante
            # Caso contrário usa 0 (Bling completa)
                valor_total += item.get('valor', 0.0)
            
            # Não tentar resolver vendedor novamente aqui.
            # O vendedor já foi decidido na etapa de resolução.
            # Se vendedor_bling_id estiver vazio, o pedido será criado sem vendedor.
            if not vendedor_bling_id:
                print(f"[HANDLER]    ℹ️ Vendedor não resolvido anteriormente - pedido será criado SEM vendedor")

            # Montar o payload COMPLETO com todos os campos que o Bling espera
            # ⚠️  CRÍTICO: Incluir "vendedor" com o ID resolvido do Bling
            # Só inclui vendedor se tiver ID válido (não enviar null)
            vendedor_payload = {'id': vendedor_bling_id} if vendedor_bling_id else None

            pedido_payload = {
                'contato': {'id': contato_id_bling},
                **({'vendedor': vendedor_payload} if vendedor_payload else {}),  # ← só se tiver ID
                'itens': itens,
                'data': hoje.strftime('%Y-%m-%d'),
                'dataSaida': hoje.strftime('%Y-%m-%d'),  # 🔑 Campo obrigatório
                'dataPrevista': data_parcelas,
                'observacoes': f'Deal #{deal_id} - {deal_title} [TEST: {timestamp_unico}]'
            }
            
            print(f"[HANDLER]    📦 Payload do pedido:")
            print(f"[HANDLER]       Contato: {contato_id_bling}")
            print(f"[HANDLER]       Vendedor: {vendedor_bling_id} ({nome_vendedor})")
            print(f"[HANDLER]       Itens: {len(itens)}")
            print(f"[HANDLER]       Data: {hoje.strftime('%Y-%m-%d')}")
            print(f"[HANDLER]       Data Prevista: {data_parcelas}")
            
            # Chamar a função existente do usuário
            sucesso, resultado, mensagem = criar_pedido_venda_bling(bling_token, pedido_payload)
            
            if not sucesso:
                msg = f"Falha ao criar pedido no Bling: {mensagem}"
                print(f"[HANDLER] ❌ {msg}")
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
                print(f"[HANDLER] ❌ {msg}")
                return (False, msg)
            
            # ════════════════════════════════════════════════════════════════
            # ✅ LOG DE SUCESSO COM RASTREAMENTO DO VENDEDOR
            # ════════════════════════════════════════════════════════════════
            print(f"\n[HANDLER] {'='*70}")
            print(f"[HANDLER] ✅ === PEDIDO CRIADO COM SUCESSO ===")
            print(f"[HANDLER] {'='*70}")
            print(f"[HANDLER] Deal Bitrix: #{deal_id} - {deal_title}")
            print(f"[HANDLER] Contato Bling: #{contato_id_bling}")
            print(f"[HANDLER] 👤 Vendedor Bling: {nome_vendedor} (ID: {vendedor_bling_id})")
            print(f"[HANDLER] 📦 Pedido Bling: #{pedido_id}")
            print(f"[HANDLER] Itens: {len(itens)}")
            print(f"[HANDLER] Data: {hoje.strftime('%Y-%m-%d')}")
            print(f"[HANDLER] {'='*70}\n")
            
        except Exception as e:
            msg = f"Exception ao criar pedido Bling: {type(e).__name__}: {str(e)}"
            print(f"[HANDLER] ❌ {msg}")
            import traceback
            traceback.print_exc()
            return (False, msg)
        
        # ====================================================================
        # 6. MARCAR DEAL COMO PROCESSADA
        # ====================================================================
        
        print(f"\n[HANDLER] 6️⃣ Marcando deal como processada...")
        
        try:
            save_processed_deal(deal_id)
            print(f"[HANDLER]    ✅ Deal #{deal_id} marcada como processada")
        except Exception as e:
            print(f"[HANDLER]    ⚠️ Erro ao marcar deal: {e}")
        
        # ====================================================================
        # 7. RETORNAR SUCESSO
        # ====================================================================
        
        mensagem = f"Deal #{deal_id} processada com sucesso. Pedido (ID: {pedido_id}) criado no Bling."
        
        print(f"\n[HANDLER] ✅ PROCESSAMENTO CONCLUÍDO COM SUCESSO!")
        print(f"[HANDLER] {mensagem}")
        print(f"[HANDLER] ===== FIM DA FUNÇÃO processar_webhook_bitrix =====\n")
        
        return (True, mensagem)
        
    except Exception as e:
        msg = f"Erro geral ao processar webhook: {type(e).__name__}: {str(e)}"
        print(f"[HANDLER] ❌ {msg}")
        import traceback
        traceback.print_exc()
        return (False, msg)
