ARG BASE_IMAGE
FROM $BASE_IMAGE

RUN find /etc/apt/sources.list.d/ -type f | xargs -I {} mv {} {}.disabled
COPY manifests/deb/artifactory.sources /etc/apt/sources.list.d/
COPY install/pip.conf /etc/pip.conf

RUN apt-get update -qqy && apt-get install -qqy git curl

RUN pip3 install -U pip wheel

COPY requirements.txt requirements-dev.txt ./
RUN pip3 install -r requirements-dev.txt

WORKDIR /work
COPY . .
RUN pip3 install -e "."
# temporary installing by path until the package will be uploaded to the pysdk artifactory
RUN pip3 install https://artifactory.vastdata.com/artifactory/files/vastdb-native-client/2382152/adbc_driver_vastdb-0.0.26-py3-none-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
RUN scripts/build.sh
