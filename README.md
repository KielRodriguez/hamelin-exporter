# hamelin-exporter

Script (en desarrollo) para la generación de bases de datos geograficas apartir de un directorio con datos.

Las bases son generadas en postgresql o exportadas mediante el API de CARTO.

Archivos soportados:
	.csv
	.kml/kmz
	.shp
	.geojson

Dependencias de python:
	- pandas
	- numpy
	- psycopg2
	- pykml

Dependencias del sistema:
	- shp2pgsql

Specs:
	python 2.7

## Uso
	python export.py directory

## Licencia
	hamelin-exporter es software libre, y puede ser redistribuido bajo los términos especificados en nuestra [licencia](https://datos.gob.mx/libreusomx).

## Sobre México Abierto
	En México Abierto creamos mecanismos de innovación y colaboración
	entre ciudadanos y gobierno con herramientas digitales, para
	impulsar el desarrollo del país.
