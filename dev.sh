# Fire up our server

PYTHONPATH=$(pwd)/src uvicorn --host 0.0.0.0 --port 8000 --reload app.main:app --root-path=/api/1
