FROM ubuntu:26.04

RUN apt-get update && DEBIAN_FRONTEND=noninteractive \
	apt-get install -y -q \
		curl git ca-certificates build-essential zlib1g-dev \
		libbz2-dev libssl-dev libreadline-dev libffi-dev \
		liblzma-dev libsqlite3-dev tk-dev \
		upx-ucl
RUN curl https://pyenv.run | bash

ENV PATH="/root/.pyenv/bin:$PATH"

RUN pyenv install -l | grep 3.13
RUN CONFIGURE_OPTS=--enable-shared pyenv install 3.13.14
RUN pyenv global 3.13.14

# uv accelerates the installs below (parallel downloads + a shared cache). This
# is an amd64-only local build, so the amd64/arm64 uv image covers it and the
# clean `COPY --from` pattern applies -- unlike the multi-arch release
# Dockerfile, which stays on pip for architecture portability. Pin matches the
# CI setup-uv version.
COPY --from=ghcr.io/astral-sh/uv:0.11.26 /uv /uvx /bin/

RUN /root/.pyenv/versions/3.13.14/bin/python -m venv /cronstable
# Point uv at that venv (VIRTUAL_ENV) and never let it fetch a managed Python --
# the pyenv-built 3.13.14 (--enable-shared, above) is what must be baked in.
ENV PATH=/cronstable/bin:$PATH \
    VIRTUAL_ENV=/cronstable \
    UV_PYTHON_DOWNLOADS=never
RUN uv pip install setuptools pyinstaller==6.21.0

COPY . /root/cronstable
WORKDIR /root/cronstable
RUN git status
RUN uv pip install .
# Best-effort, wheel-only: bundle uvloop so this (x86_64 glibc) local build gets
# the faster event loop too, mirroring the CI binary jobs. Falls back to asyncio
# if no wheel is available (cronstable/__main__._new_event_loop).
RUN uv pip install --only-binary=:all: "uvloop>=0.19" || echo "no uvloop wheel; using asyncio"
RUN python pyinstaller/cronstable --version
RUN pyinstaller pyinstaller/cronstable.spec
RUN ls -sFh dist/cronstable
RUN dist/cronstable --version
