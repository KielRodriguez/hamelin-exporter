#!/usr/bin/python
#coding=utf-8

import os
import math
import sys
import subprocess
import re

from lxml import etree
from lxml import objectify
import pandas as pd
import psycopg2


if len(sys.argv) < 3:
    print("python export.py file table_name")
    sys.exit()


filePath = sys.argv[1]
newTableName = sys.argv[2]

fileType = filePath.split("/")[-1].split(".")[1].strip().lower()


postgres_dbname = os.getenv("POSTGRES_DBNAME") if os.getenv("POSTGRES_DBNAME") is not None else "import"
postgres_user = os.getenv("POSTGRES_USER") if os.getenv("POSTGRES_USER") is not None else "postgres"
postgres_host = os.getenv("POSTGRES_HOST") if os.getenv("POSTGRES_HOST") is not None else "localhost"
postgres_password = os.getenv("POSTGRES_PASSWORD") if os.getenv("POSTGRES_PASSWORD") is not None else ""


csv_latitude_column = "(latitude|latitud|lat)"
csv_longitude_column = "(longitude|longitud|lng|long)"
csv_delimiter = ","

geometry_column = "the_geom"
geometry_srid = "4326"

csvFileList = []
shpFileList = []
geojsonFileList = []
jsonList = []
kmlKmzList = []

try:
    conn = psycopg2.connect("dbname='{dbname}' user='{user}' host='{host}' password='{password}'".format(dbname=postgres_dbname, user=postgres_user, host=postgres_host, password=postgres_password))
    conn.autocommit = True
except:
    print ("Error en la conexión con postgres.")
    sys.exit()

def analyzeTable(datasetName):
    conn.cursor().execute("ANALYZE " + datasetName)


def processSHP(file, datasetName):
    print("**SHP**")
    print("Procesando... " + file)

    conn.cursor().execute("DROP TABLE IF EXISTS " + datasetName)

    p1 = subprocess.Popen(["shp2pgsql", "-c", "-s", geometry_srid, "-g", geometry_column, file, "public."+datasetName], stdout=subprocess.PIPE)
    p2 = subprocess.Popen(["psql", "-h", postgres_host, "-d", postgres_dbname, "-U", postgres_user], stdin=p1.stdout, stdout=subprocess.PIPE)

    p1.stdout.close()
    output,err=p2.communicate()

    if(err):
        print("Error", err)


def processKML(file, datasetName):
    print("**KML**")

    sqlInsert = "INSERT INTO {dataset}(name, description, {geometry_column}{suffixColumn}) VALUES ('{name}','{description}',{geometry})"

    print("Procesando... " + file)

    with open(file) as f:
        try:
            kml = objectify.parse(f)
            root = kml.getroot()

            ## remove namespaces
            for elem in root.getiterator():
                if not hasattr(elem.tag, 'find'): continue  # (1)
                i = elem.tag.find('}')
                if i >= 0:
                    elem.tag = elem.tag[i+1:]
            objectify.deannotate(root, cleanup_namespaces=True)
        except:
            print("Error parseando el archivo. --omitiendo\n")
            return

        #flags
        pointColumn = False
        linestringColumn = False
        polygonColumn = False

        cur = conn.cursor()

        sql = "DROP TABLE IF EXISTS " + datasetName + ";CREATE TABLE " + datasetName + "(id serial, name text, description text)"

        try:
            cur.execute(sql)
        except:
            print("Error creando la tabla " + datasetName, sql)
            return

        counter_points = 0
        counter_linestring = 0
        counter_polygon = 0

        for pm in root.findall(".//Placemark"):
            name=pm.name.text.encode("utf-8")

            try:
                description=pm.description.text.encode("utf-8").replace("\n","").strip()
            except:
                description=""

            if hasattr(pm, 'Point'):
                if not pointColumn:
                    if createGeometryColumn(cur, datasetName, "POINT", "_point"):
                        pointColumn = True
                    else: continue

                sql = sqlInsert.format(dataset=datasetName,geometry_column=geometry_column, suffixColumn="_point", name=name, description=description, geometry=createGeomFromKML(point=pm.Point.coordinates.text))
                try:
                    cur.execute(sql)
                    counter_points += 1
                except:
                    print("Error inserting in the table, skipping: " + sql)
                    continue


            elif hasattr(pm, 'LineString'):
                if not linestringColumn:
                    if createGeometryColumn(cur, datasetName, "LINESTRING", "_linestring"):
                        linestringColumn = True
                    else: continue

                sql = sqlInsert.format(dataset=datasetName,geometry_column=geometry_column, suffixColumn="_linestring", name=name, description=description, geometry=createGeomFromKML(linestring=pm.LineString.coordinates.text))
                try:
                    cur.execute(sql)
                    counter_linestring += 1
                except:
                    print("Error inserting in the table, skipping: " + sql)
                    continue

            elif hasattr(pm, 'Polygon'):
                if not polygonColumn:
                    if createGeometryColumn(cur, datasetName, "POLYGON", "_polygon"):
                        polygonColumn = True
                    else: continue

                sql = sqlInsert.format(dataset=datasetName,geometry_column=geometry_column, suffixColumn="_polygon", name=name, description=description, geometry=createGeomFromKML(polygon=pm.Polygon.outerBoundaryIs.LinearRing.coordinates.text))
                try:
                    cur.execute(sql)
                    counter_polygon += 1
                except:
                    print("Error inserting in the table, skipping: " + sql)
                    continue

        analyzeTable(datasetName)
        print("Registros creados\n\tPuntos: {}\n\tLinestring: {}\n\tPolygon: {}\n".format(counter_points, counter_linestring, counter_polygon))

def buildPointSQL(lon, lat):
    return "ST_SetSRID(ST_MakePoint(" + str(lon) + "," + str(lat) + ")," + geometry_srid + ")"


def createGeomFromKML(point=None, linestring=None, polygon=None):
    if point is not None:
        coords = point.split(",")
        return buildPointSQL(coords[0].strip(), coords[1].strip())
    elif linestring is not None:
        query ="ST_GeomFromText('LINESTRING("
        for line in linestring.split("\n"):
            coords = line.split(",")
            query += coords[0].strip() + " " + coords[1].strip() + ","

        return query[0:-1] + ")', " + geometry_srid + ")"
    elif polygon is not None:
        query ="ST_GeomFromText('POLYGON(("
        for line in polygon.split("\n"):
            coords = line.split(",")
            query += coords[0].strip() + " " + coords[1].strip() + ","

        return query[0:-1] + "))', " + geometry_srid + ")"


def buildDatasetNameFromURI(uri):
    return uri.split("/")[-1].split(".")[0].replace("-","_").replace(" ","_").strip().lower()


def createGeometryColumn(cur, datasetName, type, suffixColumn=""):
    sql = "SELECT AddGeometryColumn('{dataset}' , '{geometry_column}{suffixColumn}', {srid},'{geometry}',2)"

    try:
        cur.execute(sql.format(dataset=datasetName, geometry_column=geometry_column,suffixColumn=suffixColumn, srid=geometry_srid, geometry=type))
        return True
    except:
        print("Error creando la geometria, esta postgis disponible en {} ? ".format(postgres_dbname), "Try: create extension postgis;")
        return False


def processCSV(file, datasetName):
    print("**CSV**")
    print("Procesando... " + file)

    sqlInsert = "INSERT INTO {dataset}{inputValues} VALUES ({data})"

    dataset = pd.read_csv(file)
    dataset.columns = map(str.lower, dataset.columns) # headers to lowercase

    latitudColumn = None
    longitudColumn = None
    for column in dataset.columns:
        if re.match(csv_latitude_column, column) is not None:
            latitudColumn = column
        elif re.match(csv_longitude_column, column) is not None:
            longitudColumn = column


    # if csv_latitude_column in dataset.columns and csv_longitude_column in dataset.columns:
    if latitudColumn is not None and longitudColumn is not None:
        print("Procesando... " + file)
        print("Using " + latitudColumn + " / " + longitudColumn)

        cur = conn.cursor()

        sql = "DROP TABLE IF EXISTS " + datasetName + ";"
        sql += "CREATE TABLE " + datasetName + "("
        inputValues = "("
        for header in dataset.columns:
            sql += header + " text,"
            inputValues += header + ","

        inputValues = inputValues + geometry_column + ")"
        sql = sql[0:-1] + ");"

        try:
            cur.execute(sql)
            print("Nueva tabla creada " + datasetName)
        except:
            print("Error creando la tabla " + datasetName, sql)
            return

        if not createGeometryColumn(cur, datasetName, "POINT"):
            return

        print("Cargando tabla...")
        counter = 0
        for index, row in dataset.iterrows():
            if math.isnan(row[longitudColumn]) or math.isnan(row[latitudColumn]):
                print("Error en la linea " + str(index+2) + ". --omitiendo")
                continue

            data = ""
            for header in dataset.columns:
                data += "'" + str(row[header]).rstrip() + "',"
            data += buildPointSQL(row[longitudColumn], row[latitudColumn])

            sql = sqlInsert.format(dataset=datasetName, inputValues=inputValues, data=data)
            cur.execute(sql)
            counter += 1

        analyzeTable(datasetName)
        print("Registros creados: {}\n".format(counter))
    else:
        print("No se encontro información geografica.", filePath)


if fileType=="csv":
    processCSV(filePath, newTableName)
elif fileType=="shp":
    processSHP(filePath, newTableName)
elif fileType=="kml":
    processKML(filePath, newTableName)

print("Done")
