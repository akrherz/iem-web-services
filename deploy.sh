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

gunicorn \
 -w $WORKERS \
 --graceful-timeout 0 \
 -k iemws.worker.RestartableUvicornWorker \
 -b 0.0.0.0:8000 \
 --max-requests 500 \
 --max-requests-jitter 50 \
 --reload \
 iemws.main:app 2>&1 | logger -p local1.notice --tag iemws &
