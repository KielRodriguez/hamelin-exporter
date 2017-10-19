# hamelin-exporter

Script de ayuda para la exportación de archivos con información geográfica a tablas de PostGIS.

Archivos soportados:
- csv
- shp
- geojson
- kml/kmz

Librerias externas de python:
- lxml
- pandas
- psycopg2
- kml2geojson
- unidecode
- zipfile

Puedes instalarlas con: ```$ pip3 install --user lxml pandas psycopg2 kml2geojson unidecode zipfile```

## Requisitos

##### Variables de entorno
- POSTGRES_DBNAME=database
- POSTGRES_USER=postgres
- POSTGRES_HOST=localhost
- POSTGRES_PASSWORD=password
- POSTGRES_PORT=port

##### PostGIS
¿Cómo instalar PostGIS?
- Red Hat / Centos / Scientific Linux: http://www.postgresonline.com/journal/archives/362-An-almost-idiots-guide-to-install-PostgreSQL-9.5,-PostGIS-2.2-and-pgRouting-2.1.0-with-Yum.html
- Ubuntu / Debian: https://trac.osgeo.org/postgis/wiki/UsersWikiPostGIS23UbuntuPGSQL96Apt
- Arch Linux: ```$ sudo pacman -S postgresql postgis```
- Docker: ```$ docker pull mdillon/postgis```

##### Specs:
- python 3: Se usa el motor de python 3 para poder resolver problemas logicos con el encoding de los datos

## Uso
```$ python3 fileToPostgis.py file new_table_name```

Si deseas procesar todos los archivos dentro de un directorio puedes hacerlo con el script directoryLoop.py

```$ python3 directoryLoop.py directoryToProcess```

El script tomara el nombre del archivo como nombre para la nueva tabla remplazando " " y "-" por "_" y agregara el caracter "a" al inicio para evitar problemas de nombrado.

## Licencia
hamelin-exporter es software libre, y puede ser redistribuido bajo los términos especificados en nuestra [licencia](https://datos.gob.mx/libreusomx).

## Sobre México Abierto
En México Abierto creamos mecanismos de innovación y colaboración entre ciudadanos y gobierno con herramientas digitales, para	impulsar el desarrollo del país.
