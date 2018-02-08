FROM ubuntu:17.10

ARG DATA_FOLDER

apt-get update

# env setup
apt-get install -y \
  git \
  postgis \
  python3-pip

# python 3 dependencies
pip3 install lxml pandas psycopg2 kml2geojson unidecode

# clone repo
git clone https://github.com/mxabierto/hamelin-exporter tmp/hamelin-exporter

CMD ["python3", "tmp/hamelin-exporter/directoryLoop.py", $DATA_FOLDER]
