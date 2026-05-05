#!/bin/bash
# 🚀 SCRIPT DE TESTE PARA EC2
# Execute em: /home/ec2-user/allcanci-bitrix-bling/backend

set -e

echo "╔════════════════════════════════════════════════════════════════════╗"
echo "║        🚀 TESTE DE AUTO-RENOVAÇÃO NA EC2                          ║"
echo "╚════════════════════════════════════════════════════════════════════╝"
echo ""

# Test 1: Python versão
echo "1️⃣  Verificando Python..."
python3 --version || python3.11 --version
echo "✅ Python OK"
echo ""

# Test 2: Dependências instaladas
echo "2️⃣  Verificando APScheduler..."
python3 -c "import apscheduler; print('✅ APScheduler instalado')" || {
    echo "❌ APScheduler não encontrado"
    echo "   Execute: pip install APScheduler==3.10.4"
    exit 1
}
echo ""

# Test 3: Token manager importável
echo "3️⃣  Verificando token manager..."
cd backend
python3 -c "from api.bling_token_manager import refresh_bling_token; print('✅ Token manager OK')" || {
    echo "❌ Erro ao importar token manager"
    exit 1
}
cd ..
echo ""

# Test 4: Flask app carregável
echo "4️⃣  Verificando Flask app..."
python3 -c "from backend.api.index import app; print('✅ Flask app OK')" || {
    echo "❌ Erro ao carregar Flask app"
    exit 1
}
echo ""

# Test 5: Verificar arquivo .env
echo "5️⃣  Verificando .env..."
if [ ! -f "backend/.env" ]; then
    echo "⚠️  .env não encontrado"
    echo "   Execute: cp backend/.env.ec2.example backend/.env"
    echo "   Depois preencha com seus valores"
else
    echo "✅ .env encontrado"
    grep -q "BITRIX_WEBHOOK_URL" backend/.env && echo "   ✅ BITRIX_WEBHOOK_URL configurado"
    grep -q "BLING_CLIENT_ID" backend/.env && echo "   ✅ BLING_CLIENT_ID configurado"
fi
echo ""

# Test 6: Criar tokens.json de teste
echo "6️⃣  Testando escrita de tokens.json..."
python3 -c "
import json
import os
path = 'tokens.json'
test_data = {'access_token': 'test', 'refresh_token': 'test', 'expires_in': 21600}
with open(path, 'w') as f:
    json.dump(test_data, f)
os.remove(path)
print('✅ Permissão de escrita OK')
" || {
    echo "❌ Erro ao testar escrita de arquivo"
    exit 1
}
echo ""

# Test 7: Gunicorn
echo "7️⃣  Verificando gunicorn..."
python3 -c "import gunicorn; print('✅ Gunicorn instalado')" || {
    echo "❌ Gunicorn não encontrado"
    exit 1
}
echo ""

echo "╔════════════════════════════════════════════════════════════════════╗"
echo "║            ✅ TODOS OS TESTES PASSARAM!                           ║"
echo "╚════════════════════════════════════════════════════════════════════╝"
echo ""
echo "🚀 Próximo passo:"
echo "   gunicorn -w 4 -b 0.0.0.0:5000 wsgi:app --timeout 120"
echo ""
echo "📊 Monitorar scheduler:"
echo "   curl http://localhost:5000/api/scheduler-status"
echo ""
