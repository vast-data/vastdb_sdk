ARG BASE_IMAGE
FROM $BASE_IMAGE

RUN apt-get update -qqy && apt-get install -qqy git curl

RUN pip3 install -U pip wheel
RUN pip3 install ruff mypy pytest coverage duckdb ipython

COPY requirements.txt .
RUN pip3 install -r requirements.txt

WORKDIR /work
COPY . .
RUN scripts/build.sh
