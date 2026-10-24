#!/usr/bin/env bash
# Обновление и настройка уже установленного коннектора.
# Запуск (в папке wb-mcp-server):  bash update.sh
set -euo pipefail
cd "$(dirname "$0")"

if [ ! -f .env ]; then
  echo "Файл .env не найден. Сначала выполните установку: bash install.sh"
  exit 1
fi

# Добавляем в .env новые настройки, если их ещё нет.
add_default() { grep -q "^$1=" .env || echo "$1=$2" >> .env; }
add_default ENABLE_FEEDBACK_WRITE false
add_default ENABLE_PRICE_WRITE false
add_default ENABLE_ADS_WRITE false
add_default MAX_PRICE_CHANGE_PCT 20
add_default MAX_DISCOUNT_PCT 70
add_default MAX_BID_CHANGE_PCT 30

set_value() { sed -i "s|^$1=.*|$1=$2|" .env; }

echo
echo "== 1. Токен WB =="
read -r -s -p "Вставьте НОВЫЙ токен WB и нажмите Enter (или просто Enter, чтобы оставить прежний): " NEW_TOKEN
echo
if [ -n "$NEW_TOKEN" ]; then
  set_value WB_API_TOKEN "$NEW_TOKEN"
  echo "   Токен заменён."
else
  echo "   Токен оставлен прежним."
fi
unset NEW_TOKEN

echo
echo "== 2. Изменения в кабинете =="
echo "   Ответы на отзывы и вопросы, цены и скидки, реклама (ставки, пауза/запуск, минус-фразы)."
echo "   Любое изменение Claude сможет применить только после вашего подтверждения в чате."
echo "   Для этого токен WB должен быть создан БЕЗ галочки «Только на чтение»."
read -r -p "   Включить изменения? [y/N]: " ANSWER
if [[ "${ANSWER:-}" =~ ^[YyДд] ]]; then
  set_value ENABLE_FEEDBACK_WRITE true
  set_value ENABLE_PRICE_WRITE true
  set_value ENABLE_ADS_WRITE true
  echo "   Изменения включены."
else
  set_value ENABLE_FEEDBACK_WRITE false
  set_value ENABLE_PRICE_WRITE false
  set_value ENABLE_ADS_WRITE false
  echo "   Только чтение."
fi

echo
echo "== 3. Секретный адрес =="
read -r -p "   Сменить секретный адрес коннектора? [Y/n]: " ANSWER
if [[ ! "${ANSWER:-}" =~ ^[NnНн] ]]; then
  NEW_SECRET=$(tr -dc 'A-Za-z0-9' </dev/urandom | head -c 40 || true)
  set_value MCP_SECRET "$NEW_SECRET"
  echo "   Адрес сменён: старый перестанет работать, в Claude нужно будет добавить новый."
fi

set -a; . ./.env; set +a

echo
echo "== 4. Сборка и перезапуск =="
docker compose up -d --build --force-recreate

echo
echo "Проверяем, что сервер отвечает..."
OK=""
for i in $(seq 1 30); do
  if curl -fsS "https://$DOMAIN/health" >/dev/null 2>&1; then OK=1; break; fi
  sleep 3
done
[ -n "$OK" ] && echo "Сервер работает." || echo "Сервер пока не отвечает по HTTPS. Подождите пару минут и откройте https://$DOMAIN/health"

echo
echo "=============================================================="
echo " Адрес коннектора для Claude (держите в секрете,"
echo " не показывайте на скриншотах):"
echo
echo "   https://$DOMAIN/$MCP_SECRET/mcp"
echo
echo " Если адрес изменился: в Claude удалите старый коннектор"
echo " и добавьте этот."
echo "=============================================================="
