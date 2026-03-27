
FROM docker.io/mambaorg/micromamba:2.5.0

LABEL org.opencontainers.image.source=https://github.com/akrherz/iem-web-services
LABEL org.opencontainers.image.description="IEM Web Services"

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV MAMBA_DOCKERFILE_ACTIVATE=1
ENV PYTHONPATH=/opt/iem-web-services/src

WORKDIR /opt/iem-web-services

COPY --chown=$MAMBA_USER:$MAMBA_USER environment.yml /tmp/environment.yml
RUN micromamba env create -y -f /tmp/environment.yml \
	&& micromamba clean --all --yes

COPY --chown=$MAMBA_USER:$MAMBA_USER src ./src
COPY --chown=$MAMBA_USER:$MAMBA_USER dev.sh ./dev.sh

EXPOSE 8000

ENTRYPOINT ["/usr/local/bin/_entrypoint.sh"]
CMD ["micromamba", "run", "-n", "iemws", "uvicorn", "--host", "0.0.0.0", "--port", "8000", "iemws.main:app"]

