FROM python:3.12.10-slim-bookworm@sha256:fd95fa221297a88e1cf49c55ec1828edd7c5a428187e67b5d1805692d11588db
RUN useradd --create-home --uid 1000 agent
WORKDIR /app
COPY outbox.py /app/outbox.py
RUN chown -R agent:agent /app
ENV PYTHONDONTWRITEBYTECODE=1
