FROM python:3.12-slim

# Create a non-root user
RUN useradd -m -u 1000 streamer-proxy

WORKDIR /usr/src

RUN pip install --no-cache-dir poetry==2.0.1 && \
    poetry config virtualenvs.create false

COPY pyproject.toml poetry.lock ./

RUN poetry install --only proxy --no-root --no-interaction --no-ansi

COPY ./proxy ./proxy

# Change ownership of the working directory (logs will be mounted from host)
RUN chown -R streamer-proxy:streamer-proxy /usr/src
RUN mkdir -p /usr/src/logs && chown -R streamer-proxy:streamer-proxy /usr/src/logs

# Switch to non-root user
USER streamer-proxy

CMD ["python", "-m", "proxy"]
