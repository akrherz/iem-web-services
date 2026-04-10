# iem-web-services

[![DeepSource](https://app.deepsource.com/gh/akrherz/iem-web-services.svg/?label=active+issues&show_trend=true&token=WvZunVBligt7HgkO2JGg5uMe)](https://app.deepsource.com/gh/akrherz/iem-web-services/)
[![codecov](https://codecov.io/gh/akrherz/iem-web-services/graph/badge.svg?token=zKXnLZdxIk)](https://codecov.io/gh/akrherz/iem-web-services)

Code servicing IEM API requests. Requires python 3.11+.  Utilizes database
schema found with [akrherz/iem-database](https://github.com/akrherz/iem-database.git).

## Production with Podman + systemd

This repository includes a systemd unit template for running the published
GHCR image with Podman:

- `config/systemd/iem-web-services-podman.service`
- `config/systemd/iemws.env.example`

### Host prerequisites

1. Podman installed (`/usr/bin/podman`).
2. Host paths available:
   - `/mesonet/data` (mounted read-only into container at same path)
   - `/opt/bufkit` (mounted read-only into container at same path)
3. Reverse proxy targets `127.0.0.1:8000`.

### Install

1. Create runtime env file:

```bash
sudo mkdir -p /etc/iem-web-services
sudo cp config/systemd/iemws.env.example /etc/iem-web-services/iemws.env
sudo $EDITOR /etc/iem-web-services/iemws.env
```

Use per-database host variables in `iemws.env` for sharded deployments (for
example `IEMWS_DBHOST_MESOSITE`, `IEMWS_DBHOST_IEM`, `IEMWS_DBHOST_POSTGIS`).

1. Create a Podman secret from your shard-aware PostgreSQL passfile:

```bash
install -m 600 /path/to/pgpass /tmp/iemws.pgpass
sudo podman secret create iemws-pgpass /tmp/iemws.pgpass
rm -f /tmp/iemws.pgpass
```

Set `PGPASS_SECRET` in `iemws.env` if you use a different secret name.

1. Install and enable the service:

```bash
sudo cp config/systemd/iem-web-services-podman.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now iem-web-services-podman.service
```

1. Verify:

```bash
sudo systemctl status iem-web-services-podman.service
sudo journalctl -u iem-web-services-podman.service -f
curl -sS http://127.0.0.1:8000/api/1/servertime.json
```

### Update/roll forward

The unit pulls `ghcr.io/akrherz/iem-web-services:latest` on each restart.

```bash
sudo systemctl restart iem-web-services-podman.service
```

### Rotate database credentials

```bash
sudo podman secret rm iemws-pgpass
sudo podman secret create iemws-pgpass /path/to/new-pgpass
sudo systemctl restart iem-web-services-podman.service
```

### Cutover from cron

1. Enable and validate the systemd service.
2. Disable/remove the legacy `@reboot` cron entry for `deploy.sh`.
