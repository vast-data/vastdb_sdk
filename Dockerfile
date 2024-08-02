ARG BASE_IMAGE
FROM $BASE_IMAGE

RUN apt-get update -qqy && apt-get install -qqy git curl

RUN pip3 install -U pip wheel

COPY requirements.txt requirements-dev.txt .
RUN pip3 install -r requirements-dev.txt

WORKDIR /work
COPY . .
RUN scripts/build.sh
