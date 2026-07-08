FROM python:3.14-slim

RUN pip install --no-cache-dir cronstable

COPY cronstable.yaml /etc/cronstable.d/cronstable.yaml

ENTRYPOINT ["cronstable"]
