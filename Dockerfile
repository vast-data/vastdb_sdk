ARG BASE_IMAGE
FROM $BASE_IMAGE

RUN apt-get update -qqy && apt-get install -qqy git

RUN pip3 install pytest duckdb ipython

COPY requirements.txt .
RUN pip3 install -r requirements.txt
RUN pip3 install ruff

WORKDIR /work
COPY . .
RUN ./build.sh
