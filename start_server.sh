#!/bin/bash
echo "启动VideoLLM后端服务..."
gunicorn --worker-class eventlet \
         -w 1 \
         --bind 0.0.0.0:5000 \
         --timeout 120 \
         --keep-alive 2 \
         --max-requests 1000 \
         --max-requests-jitter 100 \
         --log-level info \
         --access-logfile - \
         --error-logfile - \
         app:app 