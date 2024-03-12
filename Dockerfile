ARG BASE_IMAGE
FROM $BASE_IMAGE

RUN apt-get update -qqy && apt-get install -qqy git

COPY requirements.txt .
RUN pip3 install -r requirements.txt

WORKDIR /work
COPY . .
RUN ./build.sh
