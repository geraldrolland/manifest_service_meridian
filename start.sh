#!/bin/bash
echo "Starting manifest service..."
exec uvicorn app.main:app --host 0.0.0.0 --port 8002