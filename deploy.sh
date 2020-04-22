# Fire up our server
export PYTHONPATH=$(pwd)/src

uvicorn --port 8000 app.main:app &
