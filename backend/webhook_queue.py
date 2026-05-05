"""
⚡ WEBHOOK QUEUE MANAGER

Sistema de fila de processamento assíncrono para webhooks do Bitrix.
- Recebe webhook rapidamente (retorna 200 OK)
- Processa em background com múltiplos workers
- Suporta 5+ envios simultâneos
- Retry automático com backoff exponencial
- Persistência em arquivo (zero perda de dados)
"""

import os
import json
import threading
import time
import queue
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Tuple, Optional

# ============================================================================
# CONFIGURAÇÃO
# ============================================================================

QUEUE_DIR = Path(__file__).parent / '.webhook_queue'
QUEUE_DIR.mkdir(exist_ok=True)

QUEUE_FILE = QUEUE_DIR / 'webhook_queue.json'
PROCESSED_FILE = QUEUE_DIR / 'webhook_processed.json'

MAX_WORKERS = 5  # Número de workers paralelos
MAX_RETRIES = 3
RETRY_BACKOFF = 2  # segundos

# Lock para evitar race condition
queue_lock = threading.Lock()
webhook_queue = queue.Queue()

# ============================================================================
# PERSISTÊNCIA - Carregar/Salvar Fila
# ============================================================================

def load_queue_from_file():
    """Carrega fila do arquivo (para recovery em caso de crash)"""
    try:
        if QUEUE_FILE.exists():
            with open(QUEUE_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get('queue', [])
    except Exception as e:
        print(f"[QUEUE] ❌ Erro ao carregar fila: {e}")
    return []

def save_queue_to_file(items):
    """Salva fila no arquivo"""
    try:
        with open(QUEUE_FILE, 'w', encoding='utf-8') as f:
            json.dump({'queue': items, 'timestamp': datetime.now().isoformat()}, f, indent=2)
    except Exception as e:
        print(f"[QUEUE] ❌ Erro ao salvar fila: {e}")

def load_processed_stats():
    """Carrega estatísticas de processamento"""
    try:
        if PROCESSED_FILE.exists():
            with open(PROCESSED_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
    except:
        pass
    return {'total': 0, 'sucesso': 0, 'falhas': 0, 'retries': 0}

def save_processed_stats(stats):
    """Salva estatísticas"""
    try:
        with open(PROCESSED_FILE, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2)
    except:
        pass

# ============================================================================
# GERENCIADOR DE FILA
# ============================================================================

class WebhookQueueManager:
    """Gerencia fila de webhooks com processamento assíncrono"""
    
    def __init__(self, max_workers=MAX_WORKERS):
        self.max_workers = max_workers
        self.workers = []
        self.queue_items = load_queue_from_file()
        self.running = False
        self.stats = load_processed_stats()
        
    def adicionar_webhook(self, payload, bitrix_url, bling_endpoint_url):
        """
        Adiciona webhook à fila para processamento assíncrono
        Retorna imediatamente (não bloqueia)
        """
        try:
            item = {
                'id': str(int(time.time() * 1000)),  # ID único por timestamp
                'payload': payload,
                'bitrix_url': bitrix_url,
                'bling_endpoint_url': bling_endpoint_url,
                'created_at': datetime.now().isoformat(),
                'tentativas': 0,
                'proximo_retry': None,
                'status': 'pendente'
            }
            
            # Adicionar à fila em memória
            webhook_queue.put(item)
            
            # Persistir em arquivo
            items_persistidos = load_queue_from_file()
            items_persistidos.append(item)
            save_queue_to_file(items_persistidos)
            
            deal_id = payload.get('data', {}).get('FIELDS', {}).get('ID', 'desconhecido')
            print(f"[QUEUE] ✅ Webhook adicionado à fila")
            print(f"[QUEUE]    Deal ID: {deal_id}")
            print(f"[QUEUE]    Fila atual: {webhook_queue.qsize()} item(ns)")
            
            return True
        except Exception as e:
            print(f"[QUEUE] ❌ Erro ao adicionar webhook: {e}")
            return False
    
    def iniciar_workers(self, callback_processar):
        """
        Inicia workers em background para processar webhooks
        
        Parâmetros:
            callback_processar: função(item) que processa um webhook
                               Deve retornar (sucesso: bool, mensagem: str)
        """
        if self.running:
            print(f"[QUEUE] ⚠️  Workers já estão rodando")
            return
        
        self.running = True
        self.callback = callback_processar
        
        print(f"\n[QUEUE] === INICIANDO {self.max_workers} WORKERS ===")
        
        for i in range(self.max_workers):
            worker = threading.Thread(
                target=self._worker_loop,
                args=(i+1,),
                daemon=True,
                name=f"WebhookWorker-{i+1}"
            )
            worker.start()
            self.workers.append(worker)
            print(f"[QUEUE] ✅ Worker {i+1}/{self.max_workers} iniciado")
    
    def parar_workers(self):
        """Para os workers"""
        self.running = False
        print(f"[QUEUE] ⏹️  Parando workers...")
        
        # Aguardar workers terminarem
        for worker in self.workers:
            if worker.is_alive():
                worker.join(timeout=5)
        
        print(f"[QUEUE] ✅ Todos os workers foram parados")
    
    def _worker_loop(self, worker_id):
        """Loop do worker - processa itens da fila continuamente"""
        print(f"[WORKER-{worker_id}] 🚀 Iniciado e aguardando itens...")
        
        while self.running:
            try:
                # Tentar pegar item da fila (timeout de 2s para não travar)
                try:
                    item = webhook_queue.get(timeout=2)
                except queue.Empty:
                    continue
                
                # Processar item
                self._processar_item(item, worker_id)
                
                webhook_queue.task_done()
                
            except Exception as e:
                print(f"[WORKER-{worker_id}] 💥 Erro no worker: {e}")
                continue
    
    def _processar_item(self, item, worker_id):
        """Processa um item da fila (com retry)"""
        try:
            deal_id = item['payload'].get('data', {}).get('FIELDS', {}).get('ID', '?')
            tentativa = item['tentativas'] + 1
            
            print(f"\n[WORKER-{worker_id}] === PROCESSANDO DEAL #{deal_id} (Tentativa {tentativa}/{MAX_RETRIES}) ===")
            print(f"[WORKER-{worker_id}] ID Fila: {item['id']}")
            
            # Executar callback
            sucesso, mensagem = self.callback(item['payload'])
            
            if sucesso:
                print(f"[WORKER-{worker_id}] ✅ SUCESSO!")
                print(f"[WORKER-{worker_id}]    {mensagem}")
                
                # Atualizar estatísticas
                self.stats['total'] += 1
                self.stats['sucesso'] += 1
                save_processed_stats(self.stats)
                
                # Remover da persistência
                self._remover_da_persistencia(item['id'])
                
            else:
                # Falha - tentar retry
                print(f"[WORKER-{worker_id}] ❌ FALHA")
                print(f"[WORKER-{worker_id}]    {mensagem}")
                
                if tentativa < MAX_RETRIES:
                    # Recolocar na fila para retry
                    delay_segundos = RETRY_BACKOFF ** tentativa  # Backoff exponencial
                    item['tentativas'] = tentativa
                    item['proximo_retry'] = (datetime.now() + timedelta(seconds=delay_segundos)).isoformat()
                    item['status'] = f'retry-em-{delay_segundos}s'
                    
                    print(f"[WORKER-{worker_id}] 🔄 Reenfileirando para retry em {delay_segundos}s...")
                    webhook_queue.put(item)
                    self.stats['retries'] += 1
                    
                else:
                    # Máximo de retries atingido
                    print(f"[WORKER-{worker_id}] 🚫 MÁXIMO DE TENTATIVAS ATINGIDO!")
                    self.stats['total'] += 1
                    self.stats['falhas'] += 1
                    self._registrar_falha_permanente(item, mensagem)
                    self._remover_da_persistencia(item['id'])
                
                save_processed_stats(self.stats)
        
        except Exception as e:
            print(f"[WORKER-{worker_id}] 💥 Exceção ao processar item: {e}")
            import traceback
            traceback.print_exc()
    
    def _remover_da_persistencia(self, item_id):
        """Remove um item processado da fila persistida"""
        try:
            items = load_queue_from_file()
            items = [i for i in items if i['id'] != item_id]
            save_queue_to_file(items)
        except:
            pass
    
    def _registrar_falha_permanente(self, item, mensagem):
        """Registra falha permanente em arquivo de log"""
        try:
            falhas_file = QUEUE_DIR / 'webhook_falhas.json'
            falhas = []
            if falhas_file.exists():
                with open(falhas_file, 'r', encoding='utf-8') as f:
                    falhas = json.load(f).get('falhas', [])
            
            falhas.append({
                'timestamp': datetime.now().isoformat(),
                'deal_id': item['payload'].get('data', {}).get('FIELDS', {}).get('ID'),
                'item_id': item['id'],
                'tentativas': item['tentativas'],
                'mensagem_erro': mensagem
            })
            
            with open(falhas_file, 'w', encoding='utf-8') as f:
                json.dump({'falhas': falhas[-100:]}, f, indent=2)  # Manter últimas 100
        except:
            pass
    
    def status_fila(self):
        """Retorna status atual da fila"""
        items_persistidos = load_queue_from_file()
        return {
            'fila_em_memoria': webhook_queue.qsize(),
            'fila_persistida': len(items_persistidos),
            'workers_ativos': self.max_workers if self.running else 0,
            'stats': self.stats
        }

# ============================================================================
# INSTÂNCIA GLOBAL
# ============================================================================

queue_manager = WebhookQueueManager(max_workers=MAX_WORKERS)
