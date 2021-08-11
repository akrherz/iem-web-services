# Fire up our server
export PYTHONPATH=$(pwd)/src

# https://www.uvicorn.org/deployment/
# Something (likely pygrib) leaks memory, so we don't let an individual worker
# run for too long
# --preload can not be used with --reload
# Shutdown with just kill <PID>, no core dumps?

gunicorn \
 -w 16 \
 --capture-output \
 --log-syslog \
 --log-syslog-prefix iemws \
 --log-syslog-facility local1 \
 --graceful-timeout 0 \
 -k iemws.worker.RestartableUvicornWorker \
 -b 0.0.0.0:8000 \
 --max-requests 500 \
 --reload \
 iemws.main:app &
