#!/usr/bin/env bash
# An Apache Kafka broker (KRaft, one node) with TLS and SASL listeners for the broker tests
# (bins/ankaad/tests/brokers.rs), certificates from a throw-away CA, one SASL user (`ankaa`) with
# a random password:
#   127.0.0.1:19092  plaintext (the test producer)
#   127.0.0.1:19094  TLS, server certificate only
#   127.0.0.1:19095  TLS, client certificate required (mutual TLS)
#   127.0.0.1:19096  TLS + SASL (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
#   127.0.0.1:19097  SASL without TLS (the loopback case)
#
#   scripts/kafka-tls-broker.sh <dir>    # writes <dir>/{ca,client}.pem, client.key, sasl-password; starts the broker
#   ANKAA_KAFKA_TLS_DIR=<dir> cargo test -p ankaad --test brokers kafka_tls
#   docker rm -f ankaa-kafka-tls         # stop it
#
# <dir> is mounted into the container: with colima or Docker Desktop it must be under a shared
# directory (the home directory), not /tmp. ANKAA_KAFKA_TLS_NAME changes the container name.
set -euo pipefail
dir="${1:?directory for the certificates}"
name="${ANKAA_KAFKA_TLS_NAME:-ankaa-kafka-tls}"
mkdir -p "$dir/config"
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
# Kafka reads a PEM key store: the PKCS#8 private key followed by the certificate
openssl pkcs8 -topk8 -nocrypt -in server.key -out server.p8
cat server.p8 server.pem > server-keystore.pem
password="$(openssl rand -hex 16)"
printf '%s' "$password" > sasl-password
jaas_plain="org.apache.kafka.common.security.plain.PlainLoginModule required user_ankaa=\"$password\";"
jaas_scram="org.apache.kafka.common.security.scram.ScramLoginModule required;"
# the ports are the same inside and outside the container, so the advertised addresses work
# for the tests on the host and for the admin command below
cat > config/server.properties <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:19093
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listeners=PLAINTEXT://:19092,CONTROLLER://:19093,TLS://:19094,MTLS://:19095,SASLTLS://:19096,SASLPLAIN://:19097
advertised.listeners=PLAINTEXT://127.0.0.1:19092,TLS://127.0.0.1:19094,MTLS://127.0.0.1:19095,SASLTLS://127.0.0.1:19096,SASLPLAIN://127.0.0.1:19097
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,TLS:SSL,MTLS:SSL,SASLTLS:SASL_SSL,SASLPLAIN:SASL_PLAINTEXT
ssl.keystore.type=PEM
ssl.keystore.location=/certs/server-keystore.pem
ssl.truststore.type=PEM
ssl.truststore.location=/certs/ca.pem
ssl.client.auth=none
listener.name.mtls.ssl.client.auth=required
sasl.enabled.mechanisms=PLAIN,SCRAM-SHA-256,SCRAM-SHA-512
listener.name.sasltls.plain.sasl.jaas.config=$jaas_plain
listener.name.sasltls.scram-sha-256.sasl.jaas.config=$jaas_scram
listener.name.sasltls.scram-sha-512.sasl.jaas.config=$jaas_scram
listener.name.saslplain.plain.sasl.jaas.config=$jaas_plain
listener.name.saslplain.scram-sha-256.sasl.jaas.config=$jaas_scram
listener.name.saslplain.scram-sha-512.sasl.jaas.config=$jaas_scram
log.dirs=/tmp/kraft-combined-logs
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
EOF
# read by the broker's own user inside the container (test keys only)
chmod 0644 ./*.key ./*.pem ./*.p8 sasl-password config/server.properties
docker rm -f "$name" >/dev/null 2>&1 || true
docker run -d --name "$name" -v "$PWD:/certs:ro" -v "$PWD/config:/mnt/shared/config:ro" \
  -p 127.0.0.1:19092:19092 -p 127.0.0.1:19094:19094 -p 127.0.0.1:19095:19095 \
  -p 127.0.0.1:19096:19096 -p 127.0.0.1:19097:19097 \
  apache/kafka:3.9.1 >/dev/null
# the SCRAM credentials live in the cluster metadata: add them once the broker answers (one
# mechanism per request: KRaft refuses two changes of one user in the same request)
add_scram() {
  docker exec "$name" /opt/kafka/bin/kafka-configs.sh --bootstrap-server 127.0.0.1:19092 \
    --alter --add-config "$1=[password=$password]" --entity-type users --entity-name ankaa \
    >/dev/null 2>&1
}
for _ in $(seq 1 60); do
  if add_scram SCRAM-SHA-256 && add_scram SCRAM-SHA-512; then
    echo "kafka TLS/SASL broker up ($name); certificates and sasl-password in $PWD"
    exit 0
  fi
  sleep 1
done
echo "kafka TLS/SASL broker did not come up:" >&2
docker logs --tail 40 "$name" >&2 || true
exit 1
