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

tempDirectory = "temp"

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

    try:
        geometryColumns.append(name)
        cur.execute(sql.format(dataset=datasetName, geometry_column=name, srid=geometry_srid, geometry=type))
        return name
    except:
        print("Error creando la geometria, esta postgis disponible en {} ? ".format(postgres_dbname), "Try: create extension postgis;")
        return None


def processKML(file, datasetName, data=None):
    if data is None:
        with open(file) as f:
            processKML(file, datasetName, data=f.read())
    else:
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

        sql = "DROP TABLE IF EXISTS " + datasetName + ";"
        sql += "CREATE TABLE " + datasetName + "(gid serial PRIMARY KEY,"
        inputValues = "("
        for header in dataset.columns:
            sql += header + " {},".format(columnsType[header])
            inputValues += header + ","

        inputValues = inputValues + geometry_column + ")"
        sql = sql[0:-1] + ");"

        try:
            cur.execute(sql)
            print("Nueva tabla creada " + datasetName)
        except:
            print("Error creando la tabla " + datasetName, sql + "\n")
            return

        if createGeometryColumn(cur, datasetName, "POINT") is not None:
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


def createTableSQL(datasetName, columns, columnsType):
    sql = "DROP TABLE IF EXISTS " + datasetName + ";CREATE TABLE " + datasetName + "(gid serial PRIMARY KEY"
    for header in columns:
        sql += "," + header.lower() + " {}".format(columnsType[header])
    sql += ");"

    print("creating table {}\n".format(sql))
    conn.cursor().execute(sql)


def isValidGeojson(geojson):
    coordinates = geojson["geometry"]["coordinates"]
    if coordinates[0] == 0 or coordinates[1] == 0:
        return False
    return True


def processGeojson(file, datasetName, data=None):
    sqlInsert = "INSERT INTO {dataset}({columns}) VALUES ({values})"

    if data is None:
        with open(file) as geojsonFile:
            processGeojson(file, datasetName, data=json.load(geojsonFile))
    else:

        # build table columns
        columns = []
        geometryColumns = []
        columnsType = {}
        skip = ["styleUrl"]
        for key in data["features"][0]["properties"]:
            if(key not in skip):
                columns.append(key)
                columnsType[key] = getObjType(data["features"][0]["properties"][key])

        # create table
        try:
            createTableSQL(datasetName, columns, columnsType)
        except psycopg2.Error as e:
            print("Error creando la tabla --- ", e)
            return

        cur = conn.cursor()

        columnsCreatedFlags = {}
        counter = 0
        for feature in data["features"]:
            if isValidGeojson(feature):
                geometryType = feature["geometry"]["type"]
                columnString = ','.join(columns).lower()

                if not hasattr(columnsCreatedFlags, geometryType):
                    # create geometry column for geometry type
                    columnsCreatedFlags[geometryType] = True
                    geometryColumns.append(createGeometryColumn(cur, datasetName, geometryType.upper(), "_{type}".format(geometryType.lower())))

                columnString += ",{geometry_column}_{suffix}".format(geometry_column=geometry_column, suffix=geometryType.lower())

                values = []
                for column in columns:
                    if columnsType[column] == "text":
                        values.append("'" + feature["properties"][column] + "'")
                    else:
                        values.append(feature["properties"][column])

                coordinates = [feature["geometry"]["coordinates"][0], feature["geometry"]["coordinates"][1]]
                feature["geometry"]["coordinates"] = coordinates
                values.append("ST_SetSRID(ST_GeomFromGeoJSON('" + json.dumps(feature["geometry"]) + "'),4326)")

                sql = sqlInsert.format(dataset=datasetName, columns=columnString, values=",".join(values))
                try:
                    counter += 1
                    cur.execute(sql)
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

def clean():
    shutil.rmtree(tempDirectory)

main()
