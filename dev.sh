#!/bin/bash
# Fire up our server

PYTHONPATH=$PYTHONPATH:$(pwd)/src uvicorn \
    --host 0.0.0.0 \
    --port 8000 \
    --root-path /api/1 \
    --reload iemws.main:app
