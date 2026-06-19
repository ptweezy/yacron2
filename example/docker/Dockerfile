FROM ubuntu:xenial

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y -q \
	python3 virtualenv

RUN virtualenv -p /usr/bin/python3 /yacron2 && \
	/yacron2/bin/pip install yacron2

COPY yacron2tab.yaml /etc/yacron2.d/yacron2tab.yaml

ENTRYPOINT ["/yacron2/bin/yacron2"]
