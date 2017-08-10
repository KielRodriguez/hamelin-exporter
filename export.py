#!/usr/bin/python
#coding=utf-8

import fnmatch
import os
import re
import math
import sys

import pandas as pd

from pykml import parser
from pykml.factory import nsmap

import psycopg2

from kml2geojson import main

if len(sys.argv) < 2:
    print("Necesitas especificar un directorio")
    print("python export.py data/")
    sys.exit()

rootDirectory = sys.argv[1]

#TODO read variables from config file
csv_latitude_column = "latitud"
csv_longitude_column = "longitud"
csv_delimiter = ","

geometry_column = "the_geom"
geometry_srid = "4326"


postgres_dbname = "import"
postgres_user = "postgres"
postgres_host = "localhost"

csvFileList = []
shpFileList = []
geojsonFileList = []
jsonList = []
kmlKmzList = []

try:
    conn = psycopg2.connect("dbname='" + postgres_dbname + "' user='" + postgres_user + "' host='" + postgres_host + "'")
    conn.autocommit = True
except:
    print ("Error en la conexión con postgres.")
    sys.exit()


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
    return uri.split("/")[-1].split(".")[0].replace("-","_").replace(" ","_").lower()


def getDataFromNode(node):
    #TODO iterate all the nested childs to get all the data
    pass

namespace = {"ns": nsmap[None]}
def processKML():
    print("**KML**")
    for file in kmlKmzList:
        print("Procesando... " + file)

        with open(file) as f:
            try:
                kml = parser.parse(f)
            except:
                print("Error parseando el archivo. --omitiendo\n")
                continue

            cur = conn.cursor()
            root = kml.getroot()

            # flags for columns created
            pointColumn = False
            polygonColumn = False
            linestringColumn = False

            datasetName = buildDatasetNameFromURI(file)

            pms = root.findall(".//ns:Placemark", namespaces=namespace)

            # Get all values to create the table
            # getDataFromNode(pms[0])

            sql = "DROP TABLE IF EXISTS " + datasetName + ";CREATE TABLE " + datasetName + "(id serial, name text)"

            try:
                cur.execute(sql)
            except:
                print("Error creando la tabla " + datasetName + " - omitiendo dataset.")
                print(sql)
                continue

            counter_points = 0
            counter_linestring = 0
            counter_polygon = 0
            for pm in pms:
                if hasattr(pm, 'Point'):
                    if not pointColumn:
                        if createGeometryColumn(cur, datasetName, "POINT", "_point"):
                            pointColumn = True
                        else: continue

                    sql = "INSERT INTO " + datasetName + "(name, " + geometry_column + "_point) VALUES ('"
                    sql += pm.name + "'," + createGeomFromKML(point=pm.Point.coordinates.text) + ");"

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

                    sql = "INSERT INTO " + datasetName + "(name, " + geometry_column + "_linestring) VALUES ('"
                    sql += pm.name + "'," + createGeomFromKML(linestring=pm.LineString.coordinates.text) + ");"

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

                    sql = "INSERT INTO " + datasetName + "(name, " + geometry_column + "_polygon) VALUES ('"
                    sql += pm.name + "'," + createGeomFromKML(polygon=pm.Polygon.outerBoundaryIs.LinearRing.coordinates.text) + ");"

                    try:
                        cur.execute(sql)
                        counter_polygon += 1
                    except:
                        print("Error inserting in the table, skipping: " + sql)
                        continue

            print("Registros creados\n\tPuntos: {}\n\tLinestring: {}\n\tPolygon: {}\n".format(counter_points, counter_linestring, counter_polygon))

def createGeometryColumn(cur, datasetName, geometry, suffixColumn=""):
    sql = "SELECT AddGeometryColumn('{dataset}' , '{geometry_column}{suffixColumn}', {srid},'{geometry}',2)"

    try:
        cur.execute(sql.format(dataset=datasetName, geometry_column=geometry_column,suffixColumn=suffixColumn, srid=geometry_srid, geometry=geometry))
        return True
    except:
        print("Error creando la geometria, esta postgis disponible en {} ? ".format(postgres_dbname), "Try: create extension postgis;")
        return False


def processCSV():
    print("")
    print("**CSV**")

    sqlInsert = "INSERT INTO {dataset}{inputValues} VALUES ({data})"
    for file in csvFileList:
        with open(file) as f:
            headers = f.readline().rstrip().lower().split(csv_delimiter)
            f.close()

            if csv_latitude_column in headers and csv_longitude_column in headers:
                print("Procesando... " + file)
                cur = conn.cursor()

                datasetName = buildDatasetNameFromURI(file)

                dataset = pd.read_csv(file)
                dataset.columns = map(str.lower, dataset.columns) # headers to lowercase


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
                    print("Error creando la tabla " + datasetName + " - omitiendo dataset.", sql)
                    continue

                if not createGeometryColumn(cur, datasetName, "POINT"):
                    continue

                counter = 0
                for index, row in dataset.iterrows():
                    if math.isnan(row[csv_latitude_column]) or math.isnan(row[csv_longitude_column]):
                        continue

                    data = ""
                    for header in dataset.columns:
                        data += "'" + str(row[header]).rstrip() + "',"
                    data += buildPointSQL(row[csv_longitude_column], row[csv_latitude_column])

                    cur.execute(sqlInsert.format(dataset=datasetName, inputValues=inputValues, data=data))
                    counter += 1

                print("Registros creados: {}\n".format(counter))


for root, dirnames, filenames in os.walk(rootDirectory):
    for filename in filenames:
        resource = os.path.join(root, filename)

        if fnmatch.fnmatch(filename, '*.shp'):
            shpFileList.append(resource)
        elif fnmatch.fnmatch(filename, '*.csv'):
            csvFileList.append(resource)
        elif fnmatch.fnmatch(filename, '*.geojson'):
            geojsonFileList.append(resource)
        elif fnmatch.fnmatch(filename, '*.json'):
            jsonList.append(resource)
        elif fnmatch.fnmatch(filename, '*.kml') or fnmatch.fnmatch(filename, '*.kmz'):
            kmlKmzList.append(resource)


print("Se encontraron " + str(len(csvFileList)) + " archivos .csv")
print("Se encontraron " + str(len(shpFileList)) + " archivos .shp")
print("Se encontraron " + str(len(geojsonFileList)) + " archivos .geojson")
print("Se encontraron " + str(len(jsonList)) + " archivos .json")
print("Se encontraron " + str(len(kmlKmzList)) + " archivos .kml/kmz")
print("")

processCSV()
processKML()

print("Done")

#TODO create server and map view
