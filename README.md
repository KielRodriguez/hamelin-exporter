hamelin-exporter
----------------

Script para la generación de bases de datos geograficas apartir de un directorio con datos.

Las bases son generadas en postgresql usando el plugin de POSTGIS o exportadas mediante el API de CARTO.

Archivos soportados:
- csv
- shp
- kml

Librerias externas de python:
- lxml
- pandas
- psycopg2

Puedes instalarlas con: ```$ pip install --user lxml pandas psycopg2```

## Requisitos

##### Variables de entorno
- POSTGRES_DBNAME=database
- POSTGRES_USER=postgres
- POSTGRES_HOST=localhost
- POSTGRES_PASSWORD=password

##### PostGIS
¿Cómo instalar PostGIS?
- Red Hat / Centos / Scientific Linux: http://www.postgresonline.com/journal/archives/362-An-almost-idiots-guide-to-install-PostgreSQL-9.5,-PostGIS-2.2-and-pgRouting-2.1.0-with-Yum.html
- Ubuntu / Debian: https://trac.osgeo.org/postgis/wiki/UsersWikiPostGIS23UbuntuPGSQL96Apt
- Arch Linux: ```$ sudo pacman -S postgresql postgis```

##### Specs:
- python 2.7



## Uso
```python export.py directory```

## Licencia
hamelin-exporter es software libre, y puede ser redistribuido bajo los términos especificados en nuestra [licencia](https://datos.gob.mx/libreusomx).

## Sobre México Abierto
En México Abierto creamos mecanismos de innovación y colaboración entre ciudadanos y gobierno con herramientas digitales, para	impulsar el desarrollo del país.
