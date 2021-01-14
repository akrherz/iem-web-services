# Fire up our server
export PYTHONPATH=$(pwd)/src

# https://www.uvicorn.org/deployment/
# Something (likely pygrib) leaks memory, so we don't let an individual worker
# run for too long
# --reload does not seem to work, shrug

gunicorn -w 4 -k uvicorn.workers.UvicornWorker -b 0.0.0.0:8000 \
 --max-requests 500 --preload --reload iemws.main:app &
