#!/usr/bin/python
#coding=utf-8

import fnmatch
import os
import math
import sys
import subprocess

from lxml import etree
from lxml import objectify
import pandas as pd
import psycopg2


if len(sys.argv) < 2:
    print("Necesitas especificar un directorio")
    print("python export.py data/")
    sys.exit()

rootDirectory = sys.argv[1]

postgres_dbname = os.getenv("POSTGRES_DBNAME") if os.getenv("POSTGRES_DBNAME") is not None else "import"
postgres_user = os.getenv("POSTGRES_USER") if os.getenv("POSTGRES_USER") is not None else "postgres"
postgres_host = os.getenv("POSTGRES_HOST") if os.getenv("POSTGRES_HOST") is not None else "localhost"
postgres_password = os.getenv("POSTGRES_PASSWORD") if os.getenv("POSTGRES_PASSWORD") is not None else ""


csv_latitude_column = "latitud"
csv_longitude_column = "longitud"
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


def processSHP():
    print("**SHP**")
    for file in shpFileList:
        print("Procesando... " + file)

        datasetName = buildDatasetNameFromURI(file)

        conn.cursor().execute("DROP TABLE IF EXISTS " + datasetName)

        p1 = subprocess.Popen(["shp2pgsql", "-c", "-s", geometry_srid, file, "public."+datasetName], stdout=subprocess.PIPE)
        p2 = subprocess.Popen(["psql", "-h", postgres_host, "-d", postgres_dbname, "-U", postgres_user], stdin=p1.stdout)

        p1.stdout.close()
        output,err=p2.communicate()

        if(err):
            print("Error", err)
        else: print(output)


def processKML():
    print("**KML**")

    sqlInsert = "INSERT INTO {dataset}(name, description, {geometry_column}{suffixColumn}) VALUES ('{name}','{description}',{geometry})"
    for file in kmlKmzList:
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
                continue

            #flags
            pointColumn = False
            linestringColumn = False
            polygonColumn = False

            cur = conn.cursor()
            datasetName = buildDatasetNameFromURI(file)

            sql = "DROP TABLE IF EXISTS " + datasetName + ";CREATE TABLE " + datasetName + "(id serial, name text, description text)"

            try:
                cur.execute(sql)
            except:
                print("Error creando la tabla " + datasetName + " - omitiendo dataset.")
                print(sql)
                continue

            counter_points = 0
            counter_linestring = 0
            counter_polygon = 0

            for pm in root.findall(".//Placemark"):
                name=pm.name.text.encode("utf-8")
                description=pm.description.text.encode("utf-8").replace("\n","").strip()

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

                    sql = sqlInsert.format(dataset=datasetName,geometry_column=geometry_column, suffixColumn="_linestring", name=name, description=description, geometry=createGeomFromKML(polygon=pm.Polygon.outerBoundaryIs.LinearRing.coordinates.text))
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


def processCSV():
    print("**CSV**")

    sqlInsert = "INSERT INTO {dataset}{inputValues} VALUES ({data})"
    for file in csvFileList:
        with open(file) as f:
            headers = f.readline().strip().lower().split(csv_delimiter)
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

                analyzeTable(datasetName)
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
# print("Se encontraron " + str(len(geojsonFileList)) + " archivos .geojson")
# print("Se encontraron " + str(len(jsonList)) + " archivos .json")
print("Se encontraron " + str(len(kmlKmzList)) + " archivos .kml/kmz\n")

processCSV()
processKML()
processSHP()

print("Done")

#TODO create server and map view
