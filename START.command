#!/bin/bash
# Double-click this file to launch the Boom Records Invoice Parser

cd "$(dirname "$0")"

echo ""
echo "  Starting Boom Records Invoice Parser..."
echo ""

# Install dependencies if needed
pip3 install -r requirements.txt -q 2>/dev/null || pip install -r requirements.txt -q

# Launch
python3 app.py || python app.py
