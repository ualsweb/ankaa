#!/usr/bin/env bash
# A Mosquitto broker with TLS for the broker tests (bins/ankaad/tests/brokers.rs), certificates
# from a throw-away CA:
#   127.0.0.1:11883  plain (the test publisher)
#   127.0.0.1:18883  TLS, server certificate only
#   127.0.0.1:18884  TLS, client certificate required (mutual TLS)
#
#   scripts/mqtt-tls-broker.sh <dir>     # writes <dir>/{ca,client}.pem, client.key; starts the broker
#   ANKAA_MQTT_TLS_DIR=<dir> ANKAA_BROKER_TESTS=1 cargo test -p ankaad --test brokers mqtt_tls
#   docker rm -f ankaa-mqtt-tls          # stop it
set -euo pipefail
dir="${1:?directory for the certificates}"
mkdir -p "$dir"
cd "$dir"
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 2 \
  -subj "/CN=ankaa test CA" -keyout ca.key -out ca.pem 2>/dev/null
for who in server client; do
  openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
    -subj "/CN=ankaa test $who" -keyout "$who.key" -out "$who.csr" 2>/dev/null
  printf 'subjectAltName=IP:127.0.0.1,DNS:localhost\n' > "$who.ext"
  openssl x509 -req -in "$who.csr" -CA ca.pem -CAkey ca.key -CAcreateserial -days 2 \
    -extfile "$who.ext" -out "$who.pem" 2>/dev/null
done
chmod 0644 ./*.key ./*.pem   # read by the broker's own user inside the container (test keys only)
cat > mosquitto.conf <<'EOF'
allow_anonymous true
listener 1883
listener 8883
cafile /certs/ca.pem
certfile /certs/server.pem
keyfile /certs/server.key
listener 8884
cafile /certs/ca.pem
certfile /certs/server.pem
keyfile /certs/server.key
require_certificate true
EOF
docker rm -f ankaa-mqtt-tls >/dev/null 2>&1 || true
docker run -d --name ankaa-mqtt-tls -v "$PWD:/certs:ro" \
  -p 127.0.0.1:11883:1883 -p 127.0.0.1:18883:8883 -p 127.0.0.1:18884:8884 \
  eclipse-mosquitto:2 mosquitto -c /certs/mosquitto.conf >/dev/null
for _ in $(seq 1 30); do (echo > /dev/tcp/127.0.0.1/18883) 2>/dev/null && break; sleep 0.5; done
echo "mqtt TLS broker up; certificates in $PWD"
