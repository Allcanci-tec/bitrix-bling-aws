"""
Endpoint para criar Pedidos de Venda no Bling
POST /pedidos/vendas
"""

import requests
import json
from datetime import datetime, timedelta
import uuid
import os
import time

# Token manager para lazy refresh (auto-renovação)
try:
    from .bling_token_manager import get_valid_bling_token, refresh_bling_token
except ImportError:
    get_valid_bling_token = None
    refresh_bling_token = None

BLING_API_BASE = "https://www.bling.com.br/Api/v3"

def fazer_requisicao_bling_com_retry(metodo, url, headers, json_data=None, params=None, timeout=30, max_tentativas=3):
    """
    Faz requisição ao Bling respeitando rate limit.
    Se receber HTTP 429, espera e tenta novamente.
    """
    esperas = [2, 4, 6]

    for tentativa in range(1, max_tentativas + 1):
        if tentativa > 1:
            espera = esperas[tentativa - 2]
            print(f"[BLING-RATE-LIMIT] Aguardando {espera}s antes da tentativa {tentativa}/{max_tentativas}...")
            time.sleep(espera)

        if metodo.upper() == "POST":
            response = requests.post(
                url,
                headers=headers,
                json=json_data,
                timeout=timeout
            )
        elif metodo.upper() == "GET":
            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=timeout
            )
        else:
            raise ValueError(f"Método HTTP inválido: {metodo}")

        if response.status_code != 429:
            return response

        print(f"[BLING-RATE-LIMIT] HTTP 429 recebido na tentativa {tentativa}/{max_tentativas}")

    return response
def criar_pedido_venda_bling(access_token, payload):
    """
    Cria um pedido de venda no Bling via API v3
    
    Estrutura do payload esperado:
    {
        "contato": {"id": int},
        "data": "YYYY-MM-DD",
        "dataSaida": "YYYY-MM-DD",
        "dataPrevista": "YYYY-MM-DD",
        "itens": [
            {
                "produto": {"id": int},
                "quantidade": float,
                "valor": float,
                "unidade": "UN",
                "descricao": "string",
                "aliquotaIPI": 0
            }
        ],
        "parcelas": [
            {
                "dataVencimento": "YYYY-MM-DD",
                "valor": float,
                "formaPagamento": {"id": int}
            }
        ],
        "observacoes": "string",
        "vendedor": {"id": int} (opcional),
        "desconto": float (opcional),
        "transporte": {} (opcional)
    }
    """
    
    try:
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        # ===== 🔴 VALIDAÇÃO CRÍTICA: ARRAY DE ITENS VAZIO =====
        # 🚫 EARLY RETURN: Rejeitar imediatamente se sem itens
        itens_list = payload.get('itens', [])
        
        if not itens_list or len(itens_list) == 0:
            error_msg = "Pedido sem itens - rejeição imediata (array vazio)"
            print(f"\n[PEDIDO-VENDA] ❌ {error_msg}")
            print(f"[PEDIDO-VENDA] ⚠️ NÃO será enviado para o Bling (erro 400 garantido)")
            print(f"[PEDIDO-VENDA] 💡 Adicione pelo menos 1 item ao pedido antes de tentar criar")
            
            return False, {}, f"Erro: {error_msg}"
        
        print(f"\n[PEDIDO-VENDA] === CRIANDO PEDIDO DE VENDA NO BLING ===")
        print(f"[PEDIDO-VENDA] Endpoint: POST {BLING_API_BASE}/pedidos/vendas")
        print(f"[PEDIDO-VENDA] Contato ID: {payload.get('contato', {}).get('id', 'N/A')}")
        print(f"[PEDIDO-VENDA] Total de itens: {len(itens_list)}")
        print(f"[PEDIDO-VENDA] Total de parcelas: {len(payload.get('parcelas', []))}")
        
        # Log do payload completo
        print(f"\n[PEDIDO-VENDA] 📦 PAYLOAD ENVIADO:")
        print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
        
        # Fazer requisição POST
                # Fazer requisição POST com retry para HTTP 429
        response = fazer_requisicao_bling_com_retry(
            metodo="POST",
            url=f"{BLING_API_BASE}/pedidos/vendas",
            headers=headers,
            json_data=payload,
            timeout=30,
            max_tentativas=3
        )
        
        print(f"\n[PEDIDO-VENDA] Status da resposta: {response.status_code}")
        
        # ===== TRATAMENTO DE 401: LAZY REFRESH COM RETRY =====
        if response.status_code == 401:
            print(f"[PEDIDO-VENDA] ⚠️ HTTP 401 - Token expirado! Tentando renovar...")
            
            if refresh_bling_token and get_valid_bling_token:
                try:
                    # Renovar token usando a função correta do token manager
                    success, new_tokens = refresh_bling_token()
                    
                    if success:
                        print(f"[PEDIDO-VENDA] ✅ Token renovado com sucesso!")
                        print(f"[PEDIDO-VENDA] 🔄 Retry da requisição com novo token...")
                        
                        # Obter o novo token
                        new_token = get_valid_bling_token()
                        
                        if new_token:
                            # Atualizar headers com novo token
                            headers["Authorization"] = f"Bearer {new_token}"
                            
                            # RETRY: Fazer requisição novamente com proteção contra HTTP 429
                            response = fazer_requisicao_bling_com_retry(
                                metodo="POST",
                                url=f"{BLING_API_BASE}/pedidos/vendas",
                                headers=headers,
                                json_data=payload,
                                timeout=30,
                                max_tentativas=3
                            )
                            
                            print(f"[PEDIDO-VENDA] ✅ Retry status: {response.status_code}")
                        else:
                            print(f"[PEDIDO-VENDA] ❌ Não conseguiu obter novo token após renovação")
                    else:
                        print(f"[PEDIDO-VENDA] ❌ Falha ao renovar token")
                        
                except Exception as e:
                    print(f"[PEDIDO-VENDA] ❌ Erro ao renovar token: {e}")
                    import traceback
                    traceback.print_exc()
            else:
                print(f"[PEDIDO-VENDA] ⚠️ Token manager não disponível, não conseguindo renovar")
        
        if response.status_code in [200, 201]:
            result = response.json()
            pedido_id = result.get('data', {}).get('id', 'N/A')
            numero_pedido = result.get('data', {}).get('numero', 'N/A')
            
            print(f"[PEDIDO-VENDA] ✅ PEDIDO CRIADO COM SUCESSO!")
            print(f"[PEDIDO-VENDA] ID do Pedido: {pedido_id}")
            print(f"[PEDIDO-VENDA] Número do Pedido: {numero_pedido}")
            print(f"[PEDIDO-VENDA] 📦 RESPOSTA COMPLETA:")
            print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
            
            return True, result, f"Pedido de venda criado com sucesso! ID: {pedido_id}"
        
        else:
            error_msg = response.text
            print(f"[PEDIDO-VENDA] ❌ ERRO {response.status_code}")
            print(f"[PEDIDO-VENDA] Detalhes: {error_msg}")
            
            try:
                error_json = response.json()
                print(f"[PEDIDO-VENDA] Erro JSON: {json.dumps(error_json, indent=2, ensure_ascii=False)}")
            except:
                pass
            
            return False, {}, f"Erro ao criar pedido: {response.status_code} - {error_msg[:200]}"
    
    except requests.exceptions.Timeout:
        error_msg = "Timeout na requisição (30s)"
        print(f"[PEDIDO-VENDA] ❌ {error_msg}")
        return False, {}, error_msg
    
    except requests.exceptions.ConnectionError:
        error_msg = "Erro de conexão com a API Bling"
        print(f"[PEDIDO-VENDA] ❌ {error_msg}")
        return False, {}, error_msg
    
    except Exception as e:
        error_msg = f"Erro ao criar pedido: {str(e)}"
        print(f"[PEDIDO-VENDA] ❌ {error_msg}")
        import traceback
        print(f"[PEDIDO-VENDA] Traceback: {traceback.format_exc()}")
        return False, {}, error_msg


def preparar_payload_pedido_venda(deal, empresa, produtos, forma_pagamento_id=1, vendedor_id=None):
    """
    Prepara o payload correto para criar pedido de venda no Bling
    
    Args:
        deal: Dados do deal do Bitrix
        empresa: Dados da empresa (contato)
        produtos: Lista de produtos do deal
        forma_pagamento_id: ID da forma de pagamento (padrão: 1)
        vendedor_id: ID do vendedor no Bling (opcional)
    
    Returns:
        Dict: Payload formatado para a API do Bling
    """
    
    # Extrair ID do contato
    contato_id = empresa.get("ID")
    if not contato_id:
        raise ValueError("Empresa não possui ID válido")
    
    # Preparar itens
    itens = []
    valor_total = 0
    
    for idx, produto in enumerate(produtos, 1):
        try:
            quantidade = float(produto.get("QUANTITY", 0))
            valor_unitario = float(produto.get("PRICE", 0))
            
            # Pular produtos com quantidade zero
            if quantidade <= 0:
                print(f"[PEDIDO-VENDA] ⚠️ Produto {idx} ignorado (quantidade: {quantidade})")
                continue
            
            valor_item = quantidade * valor_unitario
            valor_total += valor_item
            
            # Estrutura correta para item de pedido de venda
            item = {
                "produto": {
                    "id": produto.get("PRODUCT_ID")
                },
                "quantidade": quantidade,
                "valor": valor_unitario,
                "unidade": "UN",
                "descricao": produto.get("PRODUCT_NAME", f"Produto {idx}"),
                "aliquotaIPI": 0
            }
            
            itens.append(item)
            
            print(f"[PEDIDO-VENDA] Item {idx}: {produto.get('PRODUCT_NAME')} - Qtd: {quantidade} - Valor: R$ {valor_unitario}")
            
        except (ValueError, TypeError) as e:
            print(f"[PEDIDO-VENDA] ❌ Erro ao processar produto {idx}: {e}")
            continue
    
    if not itens:
        raise ValueError("Nenhum produto válido encontrado")
    
    # Preparar parcela única com valor total
    data_vencimento = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")
    parcelas = [
        {
            "dataVencimento": data_vencimento,
            "valor": round(valor_total, 2),
            "formaPagamento": {
                "id": forma_pagamento_id
            }
        }
    ]
    
    # Preparar observações com identificador único para evitar duplicatas
    deal_id = deal.get("ID", "N/A")
    deal_title = deal.get("TITLE", "Sem título")
    timestamp_unico = datetime.now().strftime("%Y%m%d%H%M%S%f")[-6:]  # Últimos 6 dígitos do timestamp
    observacoes = f"Pedido gerado a partir do deal #{deal_id}: {deal_title} [ID: {timestamp_unico}]"
    
    # Construir payload
    payload = {
        "contato": {
            "id": contato_id
        },
        "data": datetime.now().strftime("%Y-%m-%d"),
        "dataSaida": datetime.now().strftime("%Y-%m-%d"),
        "dataPrevista": data_vencimento,
        "itens": itens,
        "parcelas": parcelas,
        "observacoes": observacoes
    }
    
    # Adicionar vendedor se fornecido
    if vendedor_id:
        payload["vendedor"] = {
            "id": vendedor_id
        }
        print(f"[PEDIDO-VENDA] 👤 Vendedor: ID {vendedor_id}")
    
    print(f"[PEDIDO-VENDA] 💰 Valor total: R$ {valor_total:.2f}")
    print(f"[PEDIDO-VENDA] 📅 Data vencimento: {data_vencimento}")
    
    return payload
