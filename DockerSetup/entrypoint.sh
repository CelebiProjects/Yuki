#!/bin/sh
set -e

export RABBITMQ_ALLOW_INPUT_NON_SENSITIVE_DATA=1

export RABBITMQ_MNESIA_BASE=/tmp/rabbitmq-data
export RABBITMQ_LOG_BASE=/tmp/rabbitmq-data
export RABBITMQ_PID_FILE=/tmp/rabbitmq-data/rabbit.pid

mkdir -p /tmp/rabbitmq-data
chmod -R 777 /tmp/rabbitmq-data

# Start RabbitMQ in **foreground** (DO NOT detach)
rabbitmq-server &

# Wait until RabbitMQ is ready
python3 - <<'EOF'
import socket, time
while True:
    try:
        s = socket.create_connection(('127.0.0.1', 5672), timeout=1)
        print("connected")
        s.close()
        break
    except OSError:
        print("Waiting for RabbitMQ...")
        time.sleep(5)
EOF

echo "RabbitMQ is up and running."

# Start Yuki server
/root/.local/bin/yuki server start
