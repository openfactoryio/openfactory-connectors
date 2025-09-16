#!/bin/bash
# -----------------------------------------------------------------------------
# Purpose: Install the latest OpenFactory Core from the main branch.
# Usage:   ./dev-setup.sh
# -----------------------------------------------------------------------------
# Notes:   This script is intended for development environments to ensure that
#          the latest version of OpenFactory Core is installed.
#===============================================================================
set -e

echo "Installing latest OpenFactory Core from main branch..."
pip install --upgrade --force-reinstall "OpenFactory @ git+https://github.com/openfactoryio/openfactory-core.git@main"

echo "Setup complete!"
