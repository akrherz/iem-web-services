
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
# atomic replacement of the file on the host is seen by the container
USER root
RUN rm -f /etc/hosts && ln -s /run/hostsdir/hosts /etc/hosts
USER $MAMBA_USER

EXPOSE 8000

ENTRYPOINT ["/usr/local/bin/_entrypoint.sh"]
CMD ["micromamba", "run", "-n", "iemws", "uvicorn", "--host", "0.0.0.0", "--port", "8000", "iemws.main:app"]

