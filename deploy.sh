# Fire up our server
export PYTHONPATH=$(pwd)/src

# Require script to be called with two arguments, workers and max-requests
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <workers> <max-requests>"
    echo "  workers: number of worker processes to spawn"
    echo "  max-requests: number of requests a worker will process before restarting"
    exit 1
fi

WORKERS=$1
MAXREQUESTS=$2

# Something (likely pygrib) leaks memory, so we don't let an individual worker
# run for too long
# Jitter staggers worker recycling to avoid simultaneous restarts.

uvicorn \
    --host 0.0.0.0 \
    --port 8000 \
    --root-path /api/1 \
    --workers "$WORKERS" \
    --limit-max-requests "$MAXREQUESTS" \
    --limit-max-requests-jitter 500 \
    --timeout-graceful-shutdown 60 \
    --log-level warning \
    iemws.main:app 2>&1 | logger -p local1.notice --tag iemws &
