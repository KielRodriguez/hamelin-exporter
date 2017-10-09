#!/usr/bin/python3
#coding=utf-8

import os
import math
import sys
import subprocess
import re

from lxml import etree
from lxml import objectify
import xml.dom.minidom as md

import pandas as pd
import psycopg2

import kml2geojson
import json

from zipfile import ZipFile

import shutil

if len(sys.argv) < 3:
    print("Uso: python3 export.py file table_name")
    sys.exit()


filePath = sys.argv[1]
newTableName = sys.argv[2]
fileType = filePath.split("/")[-1].split(".")[1].strip().lower()

print("Procesando " + fileType + "... " + filePath + " - " + newTableName)

# check if the file is empty
if os.stat(filePath).st_size < 5: #size in bytes
    print("Error -- Archivo vacio\n")
    sys.exit()


POSTGRES_DBNAME = os.getenv("POSTGRES_DBNAME") if os.getenv("POSTGRES_DBNAME") is not None else "import"
POSTGRES_USER = os.getenv("POSTGRES_USER") if os.getenv("POSTGRES_USER") is not None else "postgres"
POSTGRES_HOST = os.getenv("POSTGRES_HOST") if os.getenv("POSTGRES_HOST") is not None else "172.17.0.1"
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD") if os.getenv("POSTGRES_PASSWORD") is not None else "postgres"
POSTGRES_PORT = os.getenv("POSTGRES_PORT") if os.getenv("POSTGRES_PORT") is not None else "5433"


csv_latitude_column = "(latitude|latitud|lat)"
csv_longitude_column = "(longitude|longitud|lon|lng|long)"
csv_delimiter = ","

geometry_column = "the_geom"
geometry_srid = "4326"

try:
    conn = psycopg2.connect(dbname=POSTGRES_DBNAME, user=POSTGRES_USER, host=POSTGRES_HOST, password=POSTGRES_PASSWORD, port=POSTGRES_PORT)
    conn.autocommit = True
except:
    print("Error en la conexión con postgis.")
    sys.exit()

def main():
    if fileType=="csv":
        processCSV(filePath, newTableName)
    elif fileType=="shp":
        processSHP(filePath, newTableName)
    elif fileType=="kml":
        processKML(filePath, newTableName)
    elif fileType=="kmz":
        processKMZ(filePath, newTableName)
    elif fileType=="geojson":
        processGeojson(filePath, newTableName)


def processCSV(file, datasetName):
    sqlInsert = "INSERT INTO {dataset}{inputValues} VALUES ({data})"

    try:
        dataset = pd.read_csv(file, keep_default_na=False, low_memory=False, encoding="utf-8", sep=csv_delimiter, error_bad_lines=False, warn_bad_lines=False)
    except:
        try:
            dataset = pd.read_csv(file, keep_default_na=False, low_memory=False, encoding="latin-1", sep=csv_delimiter, error_bad_lines=False, warn_bad_lines=False)
        except:
            print("Error parseando el archivo\n")
            return

    rep = {" ": "_", ",": "_", "!":"", "(":"", ")":"", "ñ":"n", ".":"_"}
    rep = dict((re.escape(k), v) for k, v in rep.items()) # rep.iteritems() -> python 2.7
    pattern = re.compile("|".join(rep.keys()))

    # numeric columns that should be treated as str (catalogs)
    for column in ["id", "cve_ent", "cve_mun", "cve_loc"]:
        if column in dataset.columns:
            dataset[column] = dataset[column].astype(str)

    latitudColumn = None
    longitudColumn = None

    columns = []
    columnsType = {}
    for column in dataset.columns:
        newColumnName = pattern.sub(lambda m: rep[re.escape(m.group(0))], column.strip()).lower()

        # geographic columns
        if re.match(csv_latitude_column, newColumnName) is not None:
            latitudColumn = newColumnName
        elif re.match(csv_longitude_column, newColumnName) is not None:
            longitudColumn = newColumnName

        # data types
        if dataset[column].dtype == "int64":
            columnsType[newColumnName] = "integer"
        elif dataset[column].dtype == "float64":
            columnsType[newColumnName] = "real"
        else:
            columnsType[newColumnName] = "text"

        columns.append(newColumnName)

    dataset.columns = columns

    if latitudColumn is not None and longitudColumn is not None:
        print("Usando " + latitudColumn + " / " + longitudColumn)

        cur = conn.cursor()

        # create table
        try:
            createTable(datasetName, columns, columnsType)
            createGeometryColumn(cur, datasetName, "POINT")
        except psycopg2.Error as err:
            print("Error creando la tabla --- ", err)
            return


        print("Cargando tabla...")
        counter = 0
        for index, row in dataset.iterrows():
            if math.isnan(float(row[longitudColumn])) or math.isnan(float(row[latitudColumn])):
                print("Error en la linea " + str(index+2) + ". --omitiendo")
                continue

            data = "nextval('"  + datasetName + "_gid_seq'),"
            for header in dataset.columns:
                if columnsType[header] == "text":
                    data += "'" + row[header].strip().replace("'","''") + "',"
                else:
                    data += str(row[header]) + ","

            data += buildPointSQL(row[longitudColumn], row[latitudColumn])

            sql = sqlInsert.format(dataset=datasetName, inputValues="", data=data)
            cur.execute(sql)
            counter += 1

        createIndex(datasetName, geometry_column)
        analyzeTable(datasetName)
        print("Registros creados: {}\n".format(counter))
    else:
        print("No se encontro información geografica.\n")


def processGeojson(file, datasetName, data=None):
    sqlInsert = "INSERT INTO {dataset}({columns}{geometry_column}{suffix}) VALUES ({values})"

    if data is None:
        with open(file) as geojsonFile:
            processGeojson(file, datasetName, data=json.load(geojsonFile))
    else:
        # table columns
        columns = []
        geometryColumns = []
        columnsType = {}
        columnString = ""
        skip = ["styleUrl"]
        for key in data["features"][0]["properties"]:
            if(key not in skip):
                columns.append(key)
                columnsType[key] = getObjType(data["features"][0]["properties"][key])
                columnString += getValidColumnName(key) + ","

        # create table
        try:
            createTable(datasetName, columns, columnsType)
        except psycopg2.Error as err:
            print("Error creando la tabla --- ", err)
            return

        cur = conn.cursor()

        columnsCreated = {}
        counter = 0
        for element in data["features"]:
            geometryType = element["geometry"]["type"]
            properties = element["properties"]

            featuresToProcess = []
            if geometryType == "GeometryCollection":
                for geometry in element["geometry"]["geometries"]:
                    featuresToProcess.append({
                        "geometry": geometry
                    })
            else:
                featuresToProcess.append(element)


            for feature in featuresToProcess:
                geometryType = feature["geometry"]["type"]

                if not geometryType in columnsCreated:
                    # create geometry column for geometry type
                    try:
                        columnName = createGeometryColumn(cur, datasetName, geometryType.upper(), "_{}".format(geometryType.lower()))
                        geometryColumns.append(columnName)

                        columnsCreated[geometryType] = True

                    except psycopg2.Error as err:
                        print("Error creando columna geografica -- ", err)
                        return


                values = []
                for column in columns:
                    if columnsType[column] == "text":
                        values.append("'" + properties[column] + "'")
                    else:
                        values.append(str(properties[column]))

                if geometryType == "Point":
                    feature["geometry"]["coordinates"] = feature["geometry"]["coordinates"][0:2]

                values.append("ST_SetSRID(ST_GeomFromGeoJSON('" + json.dumps(feature["geometry"]) + "'),4326)")

                sql = sqlInsert.format(dataset=datasetName, columns=columnString, values=",".join(values), geometry_column=geometry_column, suffix="_"+geometryType.lower())
                try:
                    cur.execute(sql)
                    counter += 1
                except:
                    print("Error inserting in the table, skipping: " + sql)

        if len(geometryColumns) == 1:
            # try to rename to default name
            try:
                cur.execute("ALTER TABLE {table_name} RENAME COLUMN {column_current_name} TO {geometry_column}".format(table_name=datasetName, column_current_name=geometryColumns[0], geometry_column=geometry_column))
                geometryColumns[0] = geometry_column
            except:
                pass

        # spatial index
        for column in geometryColumns:
            createIndex(datasetName, column)

        # optimize table
        analyzeTable(datasetName)
        print("Registros creados: {}\n".format(counter))



def processKMZ(file, datasetName):
    try:
        zip=ZipFile(filePath)
        for z in zip.filelist:
            if z.filename[-4:] == '.kml':
                suffix = "_" + z.filename.split(".")[-2]
                processKML(None, newTableName+suffix, data=zip.read(z))
    except error:
        print(error)



def analyzeTable(datasetName):
    conn.cursor().execute("ANALYZE " + datasetName)


def createIndex(datasetName, geomColumn, indexNameSuffix=""):
    sql = "CREATE INDEX " + datasetName + "_gix" + indexNameSuffix + " ON " + datasetName + " USING GIST (" + geomColumn + ");"

    try:
        conn.cursor().execute(sql)
    except:
        print("Error creando el índice espacial.", sql)


def buildPointSQL(lon, lat):
    return "ST_SetSRID(ST_MakePoint(" + str(lon) + "," + str(lat) + ")," + geometry_srid + ")"


def createGeometryColumn(cur, datasetName, type, suffixColumn=""):
    sql = "SELECT AddGeometryColumn('{dataset}','{geometry_column}',{srid},'{geometry}',2)"
    name = "{geometry_column}{suffixColumn}".format(geometry_column=geometry_column,suffixColumn=suffixColumn);

    cur.execute(sql.format(dataset=datasetName, geometry_column=name, srid=geometry_srid, geometry=type))
    return name


def processKML(file, datasetName, data=None):
    if data is None:
        with open(file) as f:
            processKML(file, datasetName, data=f.read())
    else:
        kml2geojson.main.GEOTYPES = ['Polygon', 'LineString', 'Point']

        print("parsing to geojson")
        geojson = kml2geojson.main.build_feature_collection(md.parseString(data))
        processGeojson(None, datasetName, data=geojson)


def processSHP(file, datasetName):
    conn.cursor().execute("DROP TABLE IF EXISTS " + datasetName)

    p1 = subprocess.Popen(["shp2pgsql", "-c", "-s", geometry_srid, "-g", geometry_column, "-I", file, "public."+datasetName], stdout=subprocess.PIPE)
    p2 = subprocess.Popen(["psql", "-h", POSTGRES_HOST, "-p", POSTGRES_PORT,"-d", POSTGRES_DBNAME, "-U", POSTGRES_USER], stdin=p1.stdout, stdout=subprocess.PIPE)

    p1.stdout.close()
    output,err=p2.communicate()

    if(err):
        print("Error", err)
    else:
        print("\n")


def getObjType(obj):
    try:
        int(obj)
        return "integer"
    except:
        try:
            float(obj)
            return "real"
        except:
            return "text"


def getValidColumnName(column):
    return column.lower().replace("-","_").replace(" ","_")

def createTable(datasetName, columns, columnsType):
    sql = "DROP TABLE IF EXISTS " + datasetName + ";CREATE TABLE " + datasetName + "(gid serial PRIMARY KEY"
    for header in columns:
        sql += "," + getValidColumnName(header) + " {}".format(columnsType[header])
    sql += ");"

    print("creating table {}".format(sql))
    conn.cursor().execute(sql)

main()
