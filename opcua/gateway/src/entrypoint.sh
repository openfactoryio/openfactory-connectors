#!/bin/bash
# entrypoint.sh

# Default coordinator URL
COORDINATOR_URL=${COORDINATOR_URL:-http://coordinator:8000}

# Gateway container ID or name
GATEWAY_HOST=${GATEWAY_HOST}

# Register with coordinator
echo "Registering gateway $GATEWAY_HOST with coordinator..."
curl -X POST "$COORDINATOR_URL/register_gateway" \
     -H "Content-Type: application/json" \
     -d "{\"gateway_host\":\"http://$GATEWAY_HOST:8001\"}"
echo

# Start the FastAPI app
exec uvicorn src.main:app --host 0.0.0.0 --port 8001
