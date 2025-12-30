FROM python:3.12-slim

# Create a non-root user
RUN useradd -m -u 1000 streamer

WORKDIR /usr/src

RUN pip install --no-cache-dir poetry==2.0.1 && \
    poetry config virtualenvs.create false

COPY pyproject.toml poetry.lock ./

RUN poetry install --only main --no-root --no-interaction --no-ansi

COPY ./streamer ./streamer

# Change ownership of the working directory (logs will be mounted from host)
RUN chown -R streamer:streamer /usr/src
RUN mkdir -p /usr/src/logs && chown -R streamer:streamer /usr/src/logs

# Switch to non-root user
USER streamer

CMD ["python", "-m", "streamer"]
