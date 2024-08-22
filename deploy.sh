# Fire up our server
export PYTHONPATH=$(pwd)/src

if [ "$#" -eq  "0" ]
   then
     WORKERS=16
 else
     WORKERS=$1
 fi

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
 --max-requests 50000 \
 --max-requests-jitter 500 \
 --reload \
 --log-level warning \
 iemws.main:app 2>&1 | logger -p local1.notice --tag iemws &
