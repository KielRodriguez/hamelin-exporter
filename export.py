import fnmatch
import os
import re
import math
import sys

import pandas as pd
import numpy as np

from pykml import parser

import psycopg2

if len(sys.argv) < 2:
    print("Necesitas especificar un directorio")
    print("python export.py data/")
    sys.exit()

#TODO read variables from config file

#rootDirectory = "downloadData"
rootDirectory = sys.argv[1]

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
    print ("Error: postgres connection")
    sys.exit()


def buildPointSQL(lat="", lon="", coordinates=""):
    query = "ST_SetSRID(ST_MakePoint("
    if(coordinates!=""):
        query += coordinates
    elif(lat!="" and lon!=""):
        query += str(lon) + "," + str(lat)

    query += ")," + geometry_srid + ")"
    return query

def buildLinestringSQL(coordinates):
    #TODO
    pass

def buildPolygonSQL(coordinates):
    #TODO
    pass

def buildDatasetNameFromURI(uri):
    return uri.split("/")[-1].split(".")[0].replace("-","_").replace(" ","_")


def processKML():
    print("")
    print("**KML**")
    for file in kmlKmzList:
        print("Working... " + file)
        with open(file) as f:
            try:
                kml = parser.parse(f)
            except:
                print("Error parseando el archivo: " + file + " --omitiendo")
                continue

            cur = conn.cursor()

            root = kml.getroot()

            datasetName = buildDatasetNameFromURI(file)
            sql = "CREATE TABLE " + datasetName + "(id serial, name text)"

            try:
                cur.execute(sql)
            except:
                print("Error creando la tabla " + datasetName + " - omitiendo dataset.")
                print(sql)
                continue

            pointColumn = False
            polygonColumn = False
            linestringColumn = False

            counter = 0
            for pm in root.Document.Folder.iterchildren():
                if hasattr(pm, 'Point'):
                    if not pointColumn:
                        sql = "SELECT AddGeometryColumn('" + datasetName + "','" + geometry_column + "_point'," + geometry_srid + ",'POINT',2);"

                        try:
                            cur.execute(sql)
                        except:
                            print("Error creating the geometry column, postgis is enabled in the database ? ")
                            print("Try: create extension postgis;")
                            continue

                        pointColumn = True

                    sql = "INSERT INTO " + datasetName + "(name, " + geometry_column + "_point) VALUES ('"
                    sql += pm.name + "'," + buildPointSQL(coordinates=pm.Point.coordinates) + ");"

                    try:
                        cur.execute(sql)
                    except:
                        print("Error inserting in the table, skipping: " + sql)
                        continue

                elif hasattr(pm, 'LineString'):
                    if not linestringColumn:
                        sql = "SELECT AddGeometryColumn('" + datasetName + "','" + geometry_column + "_linestring'," + geometry_srid + ",'POINT',2);"

                        try:
                            cur.execute(sql)
                        except:
                            print("Error creating the geometry column, postgis is enabled in the database ? ")
                            print("Try: create extension postgis;")
                            continue

                        linestringColumn = True

                    # TODO

                elif hasattr(pm, 'Polygon'):
                    if not polygonColumn:
                        sql = "SELECT AddGeometryColumn('" + datasetName + "','" + geometry_column + "_polygon'," + geometry_srid + ",'POINT',2);"

                        try:
                            cur.execute(sql)
                        except:
                            print("Error creating the geometry column, postgis is enabled in the database ? ")
                            print("Try: create extension postgis;")
                            continue

                        polygonColumn = True

                    #TODO

                counter += 1

            print("Registros creados: " + str(counter))
            print("")

def processCSV():
    print("")
    print("**CSV**")
    for file in csvFileList:
        with open(file) as f:
            headers = f.readline().rstrip().lower().split(csv_delimiter)
            f.close()

            if csv_latitude_column in headers and csv_longitude_column in headers:
                print("Working csv: " + file)
                cur = conn.cursor()

                datasetName = file.split("/")[-1].split(".")[0].replace("-","_")

                dataset = pd.read_csv(file)
                dataset.columns = map(str.lower, dataset.columns) # headers to lowercase


                sql = "CREATE TABLE " + datasetName + "("
                inputValues = "("
                for header in dataset.columns:
                    sql += header + " text,"
                    inputValues += header + ","

                inputValues = inputValues + geometry_column + ")"
                sql = sql[0:-1] + ");"

                try:
                    cur.execute(sql)
                except:
                    print("Error creando la tabla " + datasetName + " - omitiendo dataset.")
                    print(sql)
                    continue

                sql = "SELECT AddGeometryColumn('" + datasetName + "','" + geometry_column+ "'," + geometry_srid + ",'POINT',2);"

                try:
                    cur.execute(sql)
                except:
                    print("Error creating the geometry column, postgis is enabled in the database ? ")
                    print("Try: create extension postgis;")
                    continue

                print("Table created " + datasetName)

                counter = 0
                for index, row in dataset.iterrows():
                    if math.isnan(row[csv_latitude_column]) or math.isnan(row[csv_longitude_column]):
                        continue

                    sql = "INSERT INTO " + datasetName + inputValues + " VALUES ("
                    for header in dataset.columns:
                        sql += "'" + str(row[header]).rstrip() + "',"

                    sql += buildPointSQL(lon=row[csv_longitude_column], lat=row[csv_latitude_column]) + ");"

                    cur.execute(sql)
                    counter += 1

                print("Registros creados: " + str(counter))
                print("")


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

print("")
print("Done")
