FROM ubuntu:17.10

ARG DATA_FOLDER

RUN apt-get update

# env setup
RUN apt-get install -y \
  git \
  postgis \
  python3-pip \
  python3-gdal \
  unzip

# python 3 dependencies
RUN pip3 install lxml pandas psycopg2 kml2geojson unidecode

# clone repo
RUN git clone https://github.com/mxabierto/hamelin-exporter tmp/hamelin-exporter

CMD ["python3", "tmp/hamelin-exporter/directoryLoop.py", $DATA_FOLDER]
