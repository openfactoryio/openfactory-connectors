#!/bin/bash
# entrypoint.sh

# Default coordinator URL
COORDINATOR_URL=${COORDINATOR_URL:-http://opcua-coordinator:8000}

# Wait for the Coordinator to respond
until curl -sSf $COORDINATOR_URL/assignments; do
    echo "Waiting for coordinator service ..."
    sleep 5
done
echo

# Start the Gateway
exec uvicorn src.main:app --host 0.0.0.0 --port 8001
