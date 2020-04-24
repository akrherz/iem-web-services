# Fire up our server
export PYTHONPATH=$(pwd)/src

uvicorn --host 0.0.0.0 --port 8000 app.main:app &
