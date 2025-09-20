# ------------------------------------------------------------------------------
# 🐳 Dockerfile for OPCUA Coordinator (FastAPI)
#
# This image is used in both development and production environments.
# It installs required system dependencies, sets up a non-root user,
# installs Python packages, and runs the FastAPI app.
#
# Key Features:
#   - Uses Python 3.12-slim base for small footprint
#   - Runs as a non-root user for better container security
#   - Compatible with both local and Swarm deployments
# ------------------------------------------------------------------------------

FROM python:3.12-slim

# Arguments for non-root user creation
ARG UNAME=opcua_coordinator
ARG UID=1200
ARG GID=1200
# Arguments for version management
ARG VERSION=dev
ARG APPLICATION_MANUFACTURER=OpenFactory
ARG OPENFACTORY_VERSION=main

# Install git and create a non-root user and group
RUN apt-get update && apt-get install -y git curl \
    && groupadd --gid $GID $UNAME \
    && useradd --create-home --uid $UID --gid $GID $UNAME \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements_docker.txt .

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements_docker.txt

# Install OpenFactory explicitly from GitHub (if not included in requirements)
RUN pip install --no-cache-dir \
      "git+https://github.com/openfactoryio/openfactory-core.git@${OPENFACTORY_VERSION}"

# Copy application code
COPY src src

# Change ownership to the non-root user
RUN chown -R $UNAME:$UNAME /app

# Set environment variables from build arguments
ENV APPLICATION_VERSION=${VERSION}
ENV APPLICATION_MANUFACTURER=${APPLICATION_MANUFACTURER}
ENV OPENFACTORY_VERSION=${OPENFACTORY_VERSION}

# Switch to non-root user
USER $UNAME

# Expose the port your app runs on
EXPOSE 8000

# Command to run the app with uvicorn
CMD ["uvicorn", "src.coordinator:app", "--host", "0.0.0.0", "--port", "8000"]
