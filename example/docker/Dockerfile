FROM python:3.14-slim

RUN pip install --no-cache-dir yacron2

COPY yacron2tab.yaml /etc/yacron2.d/yacron2tab.yaml

ENTRYPOINT ["yacron2"]
