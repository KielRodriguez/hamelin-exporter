#!/usr/bin/python3
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

from zipfile import ZipFile


if len(sys.argv) < 3:
    print("Correct usage: python3 export.py file table_name")
    sys.exit()


filePath = sys.argv[1]
newTableName = sys.argv[2]
fileType = filePath.split("/")[-1].split(".")[1].strip().lower()

print("Procesando " + fileType + "... " + filePath + " - " + newTableName)

# check if file is empty
if os.stat(filePath).st_size < 5: #size in bytes
    print("Error -- Archivo vacio\n")
    sys.exit()


POSTGRES_DBNAME = os.getenv("POSTGRES_DBNAME") if os.getenv("POSTGRES_DBNAME") is not None else "import"
POSTGRES_USER = os.getenv("POSTGRES_USER") if os.getenv("POSTGRES_USER") is not None else "postgres"
POSTGRES_HOST = os.getenv("POSTGRES_HOST") if os.getenv("POSTGRES_HOST") is not None else "localhost"
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD") if os.getenv("POSTGRES_PASSWORD") is not None else "postgres"
POSTGRES_PORT = os.getenv("POSTGRES_PORT") if os.getenv("POSTGRES_PORT") is not None else "5432"


csv_latitude_column = "(latitude|latitud|lat)"
csv_longitude_column = "(longitude|longitud|lon|lng|long)"
csv_delimiter = ","

geometry_column = "the_geom"
geometry_srid = "4326"

try:
    conn = psycopg2.connect(dbname=POSTGRES_DBNAME, user=POSTGRES_USER, host=POSTGRES_HOST, password=POSTGRES_PASSWORD, port=POSTGRES_PORT)
    conn.autocommit = True
except:
    print("Error en la conexión con postgres.")
    sys.exit()

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
    sql = "SELECT AddGeometryColumn('{dataset}','{geometry_column}{suffixColumn}',{srid},'{geometry}',2)"

    try:
        cur.execute(sql.format(dataset=datasetName, geometry_column=geometry_column,suffixColumn=suffixColumn, srid=geometry_srid, geometry=type))
        return True
    except:
        print("Error creando la geometria, esta postgis disponible en {} ? ".format(postgres_dbname), "Try: create extension postgis;")
        return False


def createGeomFromKML(coordinates, point=False, linestring=False, polygon=False):
    coordinates = coordinates.strip().replace("\n","")

    if point:
        coords = coordinates.split(",")
        return buildPointSQL(coords[0].strip(), coords[1].strip())
    elif linestring:
        query = "ST_GeomFromText('LINESTRING("
        for coords_row in coordinates.split(" "):
            coords = coords_row.split(",")
            if(len(coords)>=2):
                query += coords[0].strip() + " " + coords[1].strip() + ","

        return query[0:-1] + ")', " + geometry_srid + ")"
    elif polygon:
        query ="ST_GeomFromText('POLYGON(("
        for coords_row in coordinates.split(" "):
            coords = coords_row.split(",")
            if(len(coords)>=2):
                query += coords[0].strip() + " " + coords[1].strip() + ","

        return query[0:-1] + "))', " + geometry_srid + ")"



def processKML(file, datasetName, raw_data=None):
    sqlInsert = "INSERT INTO {dataset}(name, description, {geometry_column}{suffixColumn}) VALUES ('{name}','{description}',{geometry})"

    if raw_data is None:
        with open(file, "rb") as f:
            raw_data = f.read()
            f.close()

    try:
        root = objectify.fromstring(raw_data)

        ## remove namespaces
        for elem in root.getiterator():
            if not hasattr(elem.tag, 'find'): continue  # (1)
            i = elem.tag.find('}')
            if i >= 0:
                elem.tag = elem.tag[i+1:]
        objectify.deannotate(root, cleanup_namespaces=True)
    except:
        print("Error parseando el archivo.\n")
        return

    #flags
    pointColumn = False
    linestringColumn = False
    polygonColumn = False

    cur = conn.cursor()

    sql = "DROP TABLE IF EXISTS " + datasetName + ";CREATE TABLE " + datasetName + "(gid serial PRIMARY KEY, name text, description text)"

    try:
        cur.execute(sql)
    except:
        print("Error creando la tabla " + datasetName, sql)
        return

    counter_points = 0
    counter_linestring = 0
    counter_polygon = 0
    for pm in root.findall(".//Placemark"):
        try:
            name=pm.name.text.strip().replace("'","''")
        except:
            print("No name found, skipping row - ")# + etree.tostring(pm))
            continue

        try:
            description=pm.description.text.replace("\n","").strip().replace("'","''")
        except:
            description=""

        if hasattr(pm, 'Point'):
            if not pointColumn:
                if createGeometryColumn(cur, datasetName, "POINT", "_point"):
                    pointColumn = True
                else: continue

            sql = sqlInsert.format(dataset=datasetName,geometry_column=geometry_column, suffixColumn="_point", name=name, description=description, geometry=createGeomFromKML(pm.Point.coordinates.text, point=True))
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

            sql = sqlInsert.format(dataset=datasetName,geometry_column=geometry_column, suffixColumn="_linestring", name=name, description=description, geometry=createGeomFromKML(pm.LineString.coordinates.text, linestring=True))
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

            sql = sqlInsert.format(dataset=datasetName,geometry_column=geometry_column, suffixColumn="_polygon", name=name, description=description, geometry=createGeomFromKML(pm.Polygon.outerBoundaryIs.LinearRing.coordinates.text, polygon=True))
            try:
                cur.execute(sql)
                counter_polygon += 1
            except:
                print("Error inserting in the table, skipping: " + sql)
                continue

    analyzeTable(datasetName)
    print("Registros creados\n\tPuntos: {}\n\tLinestring: {}\n\tPolygon: {}\n".format(counter_points, counter_linestring, counter_polygon))


def processSHP(file, datasetName):
    print("Procesando shp..." + file + " - " + datasetName)

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

        if not createGeometryColumn(cur, datasetName, "POINT"):
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

if fileType=="csv":
    processCSV(filePath, newTableName)
elif fileType=="shp":
    processSHP(filePath, newTableName)
elif fileType=="kml":
    processKML(filePath, newTableName)
elif fileType=="kmz":
    print("Procesando kmz..." + filePath + " - " + newTableName)

    try:
        zip=ZipFile(filePath)
        for z in zip.filelist:
            if z.filename[-4:] == '.kml':
                suffix = "_" + z.filename.split(".")[-2]
                processKML(filePath, newTableName+suffix, raw_data=zip.read(z))
    except error:
        print(error)
