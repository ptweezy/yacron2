FROM ubuntu:20.04

RUN apt-get update && DEBIAN_FRONTEND=noninteractive \
	apt-get install -y -q \
		curl git ca-certificates build-essential zlib1g-dev \
		libbz2-dev libssl-dev libreadline-dev libffi-dev \
		upx-ucl
RUN curl https://pyenv.run | bash

ENV PATH="/root/.pyenv/bin:$PATH"

RUN pyenv install -l | grep 3.12
RUN CONFIGURE_OPTS=--enable-shared pyenv install 3.12.2
RUN pyenv global 3.12.2

RUN /root/.pyenv/versions/3.12.2/bin/python -m venv /yacron2
ENV PATH=/yacron2/bin:$PATH
RUN pip install -U pip setuptools pyinstaller==6.5.0

COPY . /root/yacron2
WORKDIR /root/yacron2
RUN git status
RUN python setup.py install
RUN python pyinstaller/yacron2 --version
RUN pyinstaller pyinstaller/yacron2.spec
RUN ls -sFh dist/yacron2
RUN dist/yacron2 --version
