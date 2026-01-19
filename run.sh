#!/bin/bash
cd /home/ubuntu/microservicio-productos
source venv/bin/activate
gunicorn main:app -w 4 -k uvicorn.workers.UvicornWorker --bind 127.0.0.1:8000
