
FROM docker.io/mambaorg/micromamba:2.5.0 AS hostslink
RUN mkdir -p /tmp/rootfs/etc && ln -s /run/hostsdir/hosts /tmp/rootfs/etc/hosts

FROM docker.io/mambaorg/micromamba:2.5.0

LABEL org.opencontainers.image.source=https://github.com/akrherz/iem-web-services
LABEL org.opencontainers.image.description="IEM Web Services"

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV MAMBA_DOCKERFILE_ACTIVATE=1
ENV PYTHONPATH=/opt/iem-web-services/src

WORKDIR /opt/iem-web-services

COPY --chown=$MAMBA_USER:$MAMBA_USER environment.yml /tmp/environment.yml
RUN micromamba env create -y -n iemws -f /tmp/environment.yml \
	&& micromamba clean --all --yes

# Le Sigh, we need pyiem installed from github
RUN micromamba run -n iemws python -m pip install git+https://github.com/akrherz/pyiem.git --no-deps

COPY --chown=$MAMBA_USER:$MAMBA_USER src ./src
COPY --chown=$MAMBA_USER:$MAMBA_USER dev.sh ./dev.sh

# /etc/hosts is provided by a host directory mount at /run/hostsdir so that
# atomic replacement of the file on the host is seen by the container.
# RUN cannot replace /etc/hosts (builder bind mounts it), but COPY can.
COPY --from=hostslink /tmp/rootfs/ /

EXPOSE 8000

ENTRYPOINT ["/usr/local/bin/_entrypoint.sh"]
CMD ["micromamba", "run", "-n", "iemws", "uvicorn", "--host", "0.0.0.0", "--port", "8000", "--root-path", "/api/1", "iemws.main:app"]
