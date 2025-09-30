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

# https://www.uvicorn.org/deployment/
# Something (likely pygrib) leaks memory, so we don't let an individual worker
# run for too long
# --preload can not be used with --reload
# Shutdown with just kill <PID>, no core dumps?
# jitter tries to keep all workers restarting at the same time
# graceful-timeout is how long we wait for a worker to finish

gunicorn \
 -w $WORKERS \
 --graceful-timeout 60 \
 -k iemws.worker.RestartableUvicornWorker \
 -b 0.0.0.0:8000 \
 --max-requests $MAXREQUESTS \
 --max-requests-jitter 500 \
 --reload \
 --log-level warning \
 iemws.main:app 2>&1 | logger -p local1.notice --tag iemws &
