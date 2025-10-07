#!/bin/bash
# entrypoint.sh

# Default coordinator URL
COORDINATOR_URL=${COORDINATOR_URL:-http://opcua-coordinator:8000}

# Wait for HTTP to respond
until curl -sSf $COORDINATOR_URL/assignments; do
    echo "Waiting for coordinator service ..."
    sleep 5
done
echo

# Get the container's overlay IP
GATEWAY_IP=$(hostname -i)

# Register with coordinator
echo "Registering gateway $GATEWAY_IP with coordinator ..."

curl -X POST "$COORDINATOR_URL/register_gateway" \
     -H "Content-Type: application/json" \
     -d "{\"gateway_host\":\"http://$GATEWAY_IP:8001\"}"
echo

# Start the FastAPI app
exec uvicorn src.main:app --host 0.0.0.0 --port 8001
