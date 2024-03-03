FROM python:3.12

ENV VASTDB_APPEND_VERSION_SUFFIX=true

WORKDIR /build
COPY . .
RUN python --version
RUN python setup.py sdist bdist_wheel

RUN pip install -e .
