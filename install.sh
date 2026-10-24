#!/usr/bin/env bash
# Установка коннектора WB на чистый сервер Ubuntu/Debian.
# Запуск: bash install.sh
set -euo pipefail

cd "$(dirname "$0")"

# На серверах с малым объёмом памяти (512 МБ) добавляем файл подкачки,
# чтобы сборка и работа не падали из-за нехватки памяти.
MEM_MB=$(awk '/MemTotal/ {print int($2/1024)}' /proc/meminfo)
if [ "$MEM_MB" -lt 1500 ] && ! swapon --show | grep -q .; then
  echo "== Памяти ${MEM_MB} МБ, создаю файл подкачки 1 ГБ =="
  fallocate -l 1G /swapfile || dd if=/dev/zero of=/swapfile bs=1M count=1024
  chmod 600 /swapfile
  mkswap /swapfile && swapon /swapfile
  grep -q '/swapfile' /etc/fstab || echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi

echo "== Установка Docker (если ещё нет) =="
if ! command -v docker >/dev/null 2>&1; then
  curl -fsSL https://get.docker.com | sh
fi

if [ ! -f .env ]; then
  echo
  echo "== Настройка =="
  read -r -s -p "Вставьте токен WB (ввод не отображается) и нажмите Enter: " WB_API_TOKEN
  echo
  if [ -z "$WB_API_TOKEN" ]; then echo "Токен пустой, выход."; exit 1; fi

  MCP_SECRET=$(tr -dc 'A-Za-z0-9' </dev/urandom | head -c 40 || true)
  IP=$(curl -fsS https://api.ipify.org || curl -fsS https://ifconfig.me)
  # Бесплатное доменное имя для IP-адреса: 1.2.3.4 -> 1-2-3-4.sslip.io
  DOMAIN="$(echo "$IP" | tr '.' '-').sslip.io"

  umask 077
  cat > .env <<EOF
WB_API_TOKEN=$WB_API_TOKEN
MCP_SECRET=$MCP_SECRET
DOMAIN=$DOMAIN
ENABLE_FEEDBACK_WRITE=false
ENABLE_PRICE_WRITE=false
MAX_PRICE_CHANGE_PCT=20
MAX_DISCOUNT_PCT=70
ENABLE_ADS_WRITE=false
MAX_BID_CHANGE_PCT=30
EOF
fi

set -a; . ./.env; set +a

echo
echo "== Сборка и запуск =="
docker compose up -d --build

echo
echo "Ждём получения HTTPS-сертификата..."
for i in $(seq 1 30); do
  if curl -fsS "https://$DOMAIN/health" >/dev/null 2>&1; then
    echo "Сервер работает."
    break
  fi
  sleep 4
done

echo
echo "=============================================================="
echo " Адрес коннектора для Claude (держите в секрете):"
echo
echo "   https://$DOMAIN/$MCP_SECRET/mcp"
echo
echo " Добавьте его в Claude: настройки -> Коннекторы ->"
echo " добавить пользовательский коннектор."
echo "=============================================================="
