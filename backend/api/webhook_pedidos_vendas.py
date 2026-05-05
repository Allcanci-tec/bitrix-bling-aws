"""
🚀 WEBHOOK AUTOMÁTICO: Pedidos de Venda
Quando deal entra em "CONCLUÍDO" no Bitrix → Cria pedido de venda no Bling
"""

import os
import requests
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
from flask import request, jsonify
from pedidos_vendas import criar_pedido_venda_bling, preparar_payload_pedido_venda
=======
from flask import request
>>>>>>> parent of 59317fe (teste de webhook configurando)
=======
=======
>>>>>>> parent of 79d2fb5 (teste webhook antigo)
from flask import request

# Imports das funções do backend principal (evitar import circular)
# Serão importadas dinamicamente dentro da função
<<<<<<< HEAD
>>>>>>> parent of 79d2fb5 (teste webhook antigo)
=======
>>>>>>> parent of 79d2fb5 (teste webhook antigo)


def webhook_deal_concluido_pedido_venda():
    """
    Webhook automático disparado pelo Bitrix quando um deal é concluído.
    
    O Bitrix envia um POST com os dados do deal.
    Este endpoint:
    1. Valida se é um deal "concluído"
    2. Busca dados completos do deal
    3. Busca contato (empresa) no Bling
    4. Busca/cria contato se não existir
    5. Busca produtos do deal
    6. Cria pedido de venda no Bling
    """
    try:
        print(f"\n{'='*80}")
        print(f"[WEBHOOK-PEDIDO] === WEBHOOK BITRIX RECEBIDO ===")
        print(f"{'='*80}")
        
        # ──────────────────────────────────────────────────────────────────────
        # PASSO 1: Validar dados recebidos
        # ──────────────────────────────────────────────────────────────────────
        
        request_data = request.get_json()
        if not request_data:
            print(f"[WEBHOOK-PEDIDO] ❌ Sem dados na requisição")
            return {"ok": True}, 200
        
        deal_data = request_data.get('data', {}).get('FIELDS', {})
        deal_id = deal_data.get('ID')
        deal_title = deal_data.get('TITLE', 'Sem título')
        stage_id = deal_data.get('STAGE_ID', '')
        
        print(f"[WEBHOOK-PEDIDO] Deal ID: {deal_id}")
        print(f"[WEBHOOK-PEDIDO] Deal Title: {deal_title}")
        print(f"[WEBHOOK-PEDIDO] Stage: {stage_id}")
        
        # ──────────────────────────────────────────────────────────────────────
        # PASSO 2: Validar status (só processar se "concluído")
        # ──────────────────────────────────────────────────────────────────────
        
        STAGES_CONCLUIDA = ['WON', 'C', 'CONCLUIDA', 'CONCLUÍDO']
        
        if stage_id not in STAGES_CONCLUIDA:
            print(f"[WEBHOOK-PEDIDO] ℹ️ Deal não está concluído (stage={stage_id})")
            return {"ok": True, "motivo": f"Stage {stage_id} não é conclusão"}, 200
        
        if not deal_id:
            print(f"[WEBHOOK-PEDIDO] ❌ Deal ID não encontrado")
            return {"ok": False, "erro": "Deal ID ausente"}, 400
        
        # ──────────────────────────────────────────────────────────────────────
        # PASSO 3: Carregar tokens (do backend/api/index.py)
        # ──────────────────────────────────────────────────────────────────────
        
        from index import load_tokens, is_token_expired, refresh_token, buscar_ou_criar_contato_bling
        
        tokens = load_tokens()
        if not tokens:
            print(f"[WEBHOOK-PEDIDO] ❌ Tokens não encontrados")
            return {"ok": False, "erro": "Tokens não encontrados"}, 401
        
        if is_token_expired(tokens):
            tokens = refresh_token()
            if not tokens:
                print(f"[WEBHOOK-PEDIDO] ❌ Token expirado e não renovável")
                return {"ok": False, "erro": "Token inválido"}, 401
        
        access_token = tokens.get("access_token")
        
        # ──────────────────────────────────────────────────────────────────────
        # PASSO 4: Buscar dados completos do deal no Bitrix
        # ──────────────────────────────────────────────────────────────────────
        
        print(f"[WEBHOOK-PEDIDO] 🔄 Buscando dados completos do deal...")
        
        bitrix_url = os.getenv('BITRIX_WEBHOOK_URL')
        
        resp_deal = requests.post(
            f"{bitrix_url}crm.deal.get",
            json={"id": deal_id},
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
                'Connection': 'close'
            },
            timeout=10
        )
        
        if resp_deal.status_code != 200 or 'error' in resp_deal.json():
            print(f"[WEBHOOK-PEDIDO] ❌ Erro ao buscar deal no Bitrix")
            return {"ok": False, "erro": "Deal não encontrado no Bitrix"}, 404
        
        deal_completo = resp_deal.json().get('result', {})
        company_id = deal_completo.get('COMPANY_ID')
        
        print(f"[WEBHOOK-PEDIDO] ✅ Deal encontrado")
        print(f"[WEBHOOK-PEDIDO]    Company ID: {company_id}")
        
        # ──────────────────────────────────────────────────────────────────────
        # PASSO 5: Buscar empresa/contato no Bitrix
        # ──────────────────────────────────────────────────────────────────────
        
        if not company_id:
            print(f"[WEBHOOK-PEDIDO] ❌ Deal não tem empresa associada")
            return {"ok": False, "erro": "Deal sem empresa"}, 400
        
        print(f"[WEBHOOK-PEDIDO] 🔄 Buscando dados da empresa...")
        
        resp_company = requests.post(
            f"{bitrix_url}crm.company.get",
            json={"id": company_id},
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
                'Connection': 'close'
            },
            timeout=10
        )
        
        if resp_company.status_code != 200 or 'error' in resp_company.json():
            print(f"[WEBHOOK-PEDIDO] ❌ Erro ao buscar empresa no Bitrix")
            return {"ok": False, "erro": "Empresa não encontrada"}, 404
        
        empresa_data = resp_company.json().get('result', {})
        
        print(f"[WEBHOOK-PEDIDO] ✅ Empresa encontrada: {empresa_data.get('TITLE')}")
        
        # ──────────────────────────────────────────────────────────────────────
        # PASSO 6: Buscar ou criar contato no Bling
        # ──────────────────────────────────────────────────────────────────────
        
        print(f"[WEBHOOK-PEDIDO] 🔄 Processando contato no Bling...")
        
        contato_bling, erro_contato = buscar_ou_criar_contato_bling(
            access_token,
            empresa_data,
            vendedor_id=None
        )
        
        if not contato_bling:
            print(f"[WEBHOOK-PEDIDO] ❌ Erro ao processar contato: {erro_contato}")
            return {"ok": False, "erro": f"Contato: {erro_contato}"}, 400
        
        contato_id = contato_bling.get('id')
        print(f"[WEBHOOK-PEDIDO] ✅ Contato Bling ID: {contato_id}")
        
        # ──────────────────────────────────────────────────────────────────────
        # PASSO 7: Buscar produtos do deal
        # ──────────────────────────────────────────────────────────────────────
        
        print(f"[WEBHOOK-PEDIDO] 🔄 Buscando produtos do deal...")
        
        resp_produtos = requests.post(
            f"{bitrix_url}crm.deal.productrows.get",
            json={"id": deal_id},
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
                'Connection': 'close'
            },
            timeout=10
        )
        
        if resp_produtos.status_code != 200 or 'error' in resp_produtos.json():
            print(f"[WEBHOOK-PEDIDO] ⚠️ Erro ao buscar produtos (usando lista vazia)")
            produtos = []
        else:
            produtos = resp_produtos.json().get('result', [])
        
        print(f"[WEBHOOK-PEDIDO] ✅ {len(produtos)} produtos encontrados")
        
        # ──────────────────────────────────────────────────────────────────────
        # PASSO 8: Chamar endpoint de criação de pedido
        # ──────────────────────────────────────────────────────────────────────
        
        print(f"[WEBHOOK-PEDIDO] 🔄 Criando pedido de venda no Bling...")
        
        pedido_payload = {
            "contato_id": contato_id,
            "produtos": produtos,
            "vendedor_id": None,
            "deal": {
                "ID": deal_id,
                "TITLE": deal_title
            },
            "empresa": empresa_data
        }
        
        resp_pedido = requests.post(
            f"{request.host_url}bling/pedidos-vendas",
            headers={"Content-Type": "application/json"},
            json=pedido_payload,
            timeout=30
        )
        
        if resp_pedido.status_code in [200, 201]:
            resultado_pedido = resp_pedido.json()
            pedido_id = resultado_pedido.get('data', {}).get('id', 'N/A')
            numero_pedido = resultado_pedido.get('data', {}).get('numero', 'N/A')
            
            print(f"[WEBHOOK-PEDIDO] ✅ SUCESSO!")
            print(f"[WEBHOOK-PEDIDO]    Pedido ID: {pedido_id}")
            print(f"[WEBHOOK-PEDIDO]    Número: {numero_pedido}")
            
            return {
                "ok": True,
                "deal_id": deal_id,
                "pedido_id": pedido_id,
                "numero_pedido": numero_pedido,
                "mensagem": "Pedido de venda criado com sucesso!"
            }, 200
        
        else:
            erro = resp_pedido.json().get('mensagem', 'Erro desconhecido')
            print(f"[WEBHOOK-PEDIDO] ❌ Erro ao criar pedido: {erro}")
            
            return {
                "ok": False,
                "deal_id": deal_id,
                "erro": erro
            }, resp_pedido.status_code
        
    except Exception as e:
        error_msg = f"Erro ao processar webhook: {str(e)}"
        print(f"[WEBHOOK-PEDIDO] ❌ EXCEÇÃO: {error_msg}")
        import traceback
        print(f"[WEBHOOK-PEDIDO] Traceback: {traceback.format_exc()}")
        
        return {
            "ok": False,
            "erro": error_msg
        }, 500


def teste_webhook_pedido():
    """Endpoint de teste para simular webhook"""
    print(f"\n[WEBHOOK-TESTE] Simulando webhook do Bitrix...")
    
    dados_teste = {
        "event": "ONCRMDEALUPDATE",
        "data": {
            "FIELDS": {
                "ID": 123,
                "TITLE": "Deal Teste Automação",
                "STAGE_ID": "WON",
                "CONTACT_ID": 456,
                "COMPANY_ID": 789
            }
        }
    }
    
    resp = requests.post(
        f"http://localhost:3000/bitrix/webhook-pedidos-vendas",
        json=dados_teste,
        timeout=30
    )
    
    return {
        "teste": "enviado",
        "status": resp.status_code,
        "resposta": resp.json()
    }
