set -e -x

IMAGE="ghcr.io/akrherz/iem-web-services:latest"
CONTAINER="iem-web-services"
NETWORK="${IEMWS_NETWORK:-iemws-net}"
DB_CONTAINER="${IEMWS_DB_CONTAINER:-iem-database}"
DB_IMAGE="${IEMWS_DB_IMAGE:-ghcr.io/akrherz/iem_database:test_data}"
START_DB="${IEMWS_START_DB:-1}"
DBHOST="${IEMWS_DBHOST:-127.0.0.1}"
DBUSER="${IEMWS_DBUSER:-mesonet}"
DISABLE_TELEMETRY="${IEMWS_DISABLE_TELEMETRY:-1}"

# Build a runnable image with Python dependencies and app code.
docker build -t "$IMAGE" -f Dockerfile .

docker rm -f "$CONTAINER" || true

if [ "$START_DB" = "1" ]; then
	docker network create "$NETWORK" || true
	docker rm -f "$DB_CONTAINER" || true
	docker run --name "$DB_CONTAINER" --network "$NETWORK" -p 8000:8000 -d "$DB_IMAGE"
	for i in $(seq 1 60); do
		if docker exec "$DB_CONTAINER" pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1; then
			echo "Database is up"
			break
		fi
		sleep 2
	done
fi

docker run --name "$CONTAINER" \
	--network "container:$DB_CONTAINER" \
	-e IEMWS_DBHOST="$DBHOST" \
	-e IEMWS_DBUSER="$DBUSER" \
	-e IEMWS_DISABLE_TELEMETRY="$DISABLE_TELEMETRY" \
	-d "$IMAGE"

# Wait for Uvicorn/FastAPI startup.
for i in $(seq 1 30); do
	if curl -fsS http://127.0.0.1:8000/openapi.json >/dev/null; then
		echo "Service is up"
		echo "Open: http://127.0.0.1:8000/docs"
		exit 0
	fi
	sleep 2
done

echo "Container logs:"
docker logs "$CONTAINER"
echo "Service failed to start in time"
exit 1
