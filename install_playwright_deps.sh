#!/bin/bash
# Script to install Playwright system dependencies
# Run with: bash install_playwright_deps.sh

echo "Installing Playwright system dependencies..."
echo "This requires sudo privileges."
echo ""

# Run the Playwright install-deps command
python3 -m playwright install-deps

echo ""
echo "Done! You can now use Playwright browsers."

