#!/bin/bash
set -e

if ! command -v socat &>/dev/null; then
  echo "socat no está instalado. Ejecuta: brew install socat"
  exit 1
fi

echo "Levantando proxies socat para bases de datos VPN..."

socat TCP-LISTEN:11433,fork,reuseaddr,bind=0.0.0.0 TCP:172.17.40.5:1433 &
PID_SS=$!
echo "  SQL Server : localhost:11433 → 172.17.40.5:1433 (PID $PID_SS)"

socat TCP-LISTEN:15432,fork,reuseaddr,bind=0.0.0.0 TCP:172.17.40.7:5432 &
PID_PG=$!
echo "  PostgreSQL : localhost:15432 → 172.17.40.7:5432 (PID $PID_PG)"

socat TCP-LISTEN:10446,fork,reuseaddr,bind=0.0.0.0 TCP:192.168.9.139:446 &
PID_DB2=$!
echo "  DB2 (database) : localhost:10446 → 192.168.9.139:446   (PID $PID_DB2)"

# JT400 también necesita el servidor central y de autenticación de IBM i
socat TCP-LISTEN:8470,fork,reuseaddr,bind=0.0.0.0 TCP:192.168.9.139:8470 &
PID_DB2_CENTRAL=$!
echo "  DB2 (central)  : localhost:8470  → 192.168.9.139:8470  (PID $PID_DB2_CENTRAL)"

socat TCP-LISTEN:8472,fork,reuseaddr,bind=0.0.0.0 TCP:192.168.9.139:8472 &
PID_DB2_SIGNON=$!
echo "  DB2 (signon)   : localhost:8472  → 192.168.9.139:8472  (PID $PID_DB2_SIGNON)"

cleanup() {
  echo ""
  echo "Deteniendo proxies socat y contenedores..."
  kill "$PID_SS" "$PID_PG" "$PID_DB2" "$PID_DB2_CENTRAL" "$PID_DB2_SIGNON" 2>/dev/null || true
  docker compose down
}
trap cleanup INT TERM

echo ""
docker compose up -d "$@"

echo ""
echo "Servicios activos. Ctrl+C para detener proxies y contenedores."
wait
