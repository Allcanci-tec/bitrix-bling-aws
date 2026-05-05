"""
Módulo centralizado de validação de stage para o Bling
Garante que pedidos NUNCA sejam criados para deals não em "concluído"
"""

# Stages que indicam deal concluída/ganha no Bitrix
# 🔐 RESTRITO: Apenas estágio de conclusão (WON)
STAGES_VALIDOS = ['WON']

def validar_stage_para_pedido(stage_id):
    """
    Valida se o stage da deal permite criação de pedido no Bling.
    
    REGRA: Pedidos NUNCA devem ser criados para deals que não estão em "concluído"
    
    Args:
        stage_id: ID do stage do deal no Bitrix (será feita uppercase)
    
    Returns:
        tuple: (é_válido: bool, mensagem: str)
    
    Exemplos:
        validar_stage_para_pedido('WON')      # (True, "Stage válido")
        validar_stage_para_pedido('PIPELINE') # (False, "Stage inválido: PIPELINE")
    """
    if not stage_id:
        return (False, "❌ BLOQUEADO: Stage está vazio!")
    
    stage_upper = str(stage_id).upper().strip()
    
    if stage_upper not in STAGES_VALIDOS:
        return (False, f"""❌ BLOQUEADO: Deal NÃO está em conclusão!
        Stage recebido: '{stage_upper}'
        Stages válidos: {STAGES_VALIDOS}
        
        ℹ️  Pedidos de venda SOMENTE podem ser criados para deals em "concluído"
        💡 Se o stage acima deveria ser válido, contate o administrador""")
    
    return (True, f"✅ Stage '{stage_upper}' é válido para pedido")


def obter_stage_do_deal(campos_payload, bitrix_url=None, deal_id=None):
    """
    Extrai o stage ID da deal do payload ou busca via API Bitrix se necessário.
    
    Args:
        campos_payload: Dict com campos FIELDS do payload webhook
        bitrix_url: URL do Bitrix (opcional, para busca via API)
        deal_id: ID da deal (opcional, para busca via API)
    
    Returns:
        str: Stage ID em uppercase, ou '' se não encontrado
    """
    import requests
    
    # PASSO 1: Tentar obter do payload
    stage_id = campos_payload.get('STAGE_ID', '').upper().strip()
    
    if stage_id:
        print(f"[VALIDACAO-STAGE] 🎯 Stage do payload: {stage_id}")
        return stage_id
    
    # PASSO 2: Se não encontrou e temos dados de API, buscar via Bitrix
    if bitrix_url and deal_id:
        print(f"[VALIDACAO-STAGE] 🔍 Stage não found em payload, buscando via API...")
        try:
            bitrix_headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
                'Connection': 'close'
            }
            resp = requests.post(
                bitrix_url + 'crm.deal.get',
                json={'id': deal_id},
                headers=bitrix_headers,
                timeout=15
            )
            
            if resp.status_code == 200:
                deal_data = resp.json().get('result', {})
                stage_id = deal_data.get('STAGE_ID', '').upper().strip()
                print(f"[VALIDACAO-STAGE] ✅ Stage obtido via API: {stage_id}")
                return stage_id
        except Exception as e:
            print(f"[VALIDACAO-STAGE] ⚠️  Erro ao buscar stage via API: {e}")
    
    print(f"[VALIDACAO-STAGE] ❌ Stage não encontrado")
    return ''
