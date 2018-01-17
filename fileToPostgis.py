#!/usr/bin/python3
#coding=utf-8

import os
import math
import sys
import subprocess
import re
import shutil

import xml
import xml.dom.minidom as md

import lxml.html as htmlParser

import pandas as pd
import psycopg2
import kml2geojson
import json
import zipfile
import unidecode

# check inputs
if len(sys.argv) < 3:
    print("Uso: python3 fileToPostgis.py file table_name")
    sys.exit()


# setup
POSTGRES_DBNAME = os.getenv("POSTGRES_DBNAME") if os.getenv("POSTGRES_DBNAME") is not None else ""
POSTGRES_USER = os.getenv("POSTGRES_USER") if os.getenv("POSTGRES_USER") is not None else ""
POSTGRES_HOST = os.getenv("POSTGRES_HOST") if os.getenv("POSTGRES_HOST") is not None else ""
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD") if os.getenv("POSTGRES_PASSWORD") is not None else ""
POSTGRES_PORT = os.getenv("POSTGRES_PORT") if os.getenv("POSTGRES_PORT") is not None else ""

CSV_LATITUDE_COLUMN = "(latitude|latitud|lat)"
CSV_LONGITUDE_COLUMN = "(longitude|longitud|lon|lng|long)"
CSV_DELIMITER = ","

GEOMETRY_COLUMN_NAME = "the_geom"
GEOMETRY_COLUMN_SRID = "4326"

TEMP_FOLDER = "./tmp"

WRITE_LOG = True

# check connection with db
try:
    conn = psycopg2.connect(dbname=POSTGRES_DBNAME, user=POSTGRES_USER, host=POSTGRES_HOST, password=POSTGRES_PASSWORD, port=POSTGRES_PORT)
    conn.autocommit = True
except:
    print("---------- Error en la conexión con postgis.")
    sys.exit()


def main():
    filePath = sys.argv[1]
    newTableName = getValidName(sys.argv[2])
    fileType = re.search('(^\w+)', filePath.split(".")[-1].strip()).group(0)

    printMessage("Procesando " + fileType + "... " + filePath + " - " + newTableName)

    try:
        # check if the file is empty
        if os.stat(filePath).st_size < 5: # size in bytes
            printMessage("---------- Error: Archivo vacio")
            return
    except:
        printMessage("---------- Error: Archivo no encontrado")
        return

    if os.path.exists(TEMP_FOLDER):
        shutil.rmtree(TEMP_FOLDER)

    if fileType=="csv":
        processCSV(filePath, newTableName)
    elif fileType=="shp":
        processSHP(filePath, newTableName)
    elif fileType=="kml":
        processKML(filePath, newTableName)
    elif fileType=="kmz":
        processKMZ(filePath, newTableName)
    elif fileType=="json":
        processJSON(filePath, newTableName)
    elif fileType=="zip":
        processZip(filePath, newTableName)
    elif fileType=="geojson":
        processGeojson(filePath, newTableName)
    else:
        printMessage("Formato no soportado: " + fileType)

    if os.path.exists(TEMP_FOLDER):
        shutil.rmtree(TEMP_FOLDER)

def processZip(file, datasetName):
    #TODO: Nested search ?
    try:
        zip = zipfile.ZipFile(file)
        validFile = False
        for z in zip.filelist:
            if z.filename[-4:] == '.shp':
                # verify tmp file exists
                if not os.path.exists(TEMP_FOLDER):
                    os.makedirs(TEMP_FOLDER)

                # extract zip to tmp folder
                subprocess.run(["unzip", file, "-d", TEMP_FOLDER])

                processSHP(TEMP_FOLDER + "/" + z.filename, datasetName)
                validFile = True
                break

        if not validFile:
            printMessage("---------- Error: No se encontro archivo shp")
    except zipfile.BadZipFile as error:
        printMessage("----------Error: El archivo no es tipo zip")



def processGeojson(file, datasetName, data=None):
    sqlInsert = "INSERT INTO \"{dataset}\"({columns}{geometry_column}{suffix}) VALUES ({values})"

    if data is None:
        with open(file) as geojsonFile:
            processGeojson(file, datasetName, data=json.load(geojsonFile))
    else:
        # table columns
        columns = []
        validColumns = []
        columnsType = {}
        columnString = ""

        textColumns = ["cve_ent", "cve_mun", "cve_loc", "cvegeo"] # numeric columns that should be treated as str (catalogs)
        for column in data["features"][0]["properties"]:
            validColumnName = getValidName(column)

            columns.append(column)
            validColumns.append(validColumnName)
            columnsType[validColumnName] = "text" if validColumnName in textColumns else getObjType(data["features"][0]["properties"][column])
            columnString += validColumnName + ","


        # create table
        try:
            createTable(datasetName, validColumns, columnsType)
        except psycopg2.Error as err:
            printMessage("---------- Error creando la tabla: " + str(err))
            return

        cur = conn.cursor()

        geometryColumns = []
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

                # validate geometry coordinates
                if geometryType == "Point":
                    feature["geometry"]["coordinates"] = feature["geometry"]["coordinates"][0:2]
                elif geometryType == "Polygon":
                    for batchCoordinates in feature["geometry"]["coordinates"]:
                        for index in range(0, len(batchCoordinates)):
                            batchCoordinates[index] = batchCoordinates[index][0:2]
                elif geometryType == "LineString":
                    for index in range(0, len(feature["geometry"]["coordinates"])):
                        feature["geometry"]["coordinates"][index] = feature["geometry"]["coordinates"][index][0:2]


                if not geometryType in columnsCreated:
                    # create geometry column for geometry type
                    try:
                        columnName = createGeometryColumn(cur, datasetName, geometryType.upper(), "_{}".format(geometryType.lower()))
                        geometryColumns.append(columnName)

                        columnsCreated[geometryType] = True

                    except psycopg2.Error as err:
                        printMessage("---------- Error creando columna geografica" + str(err))
                        return


                values = []
                for index in range(0, len(columns)):
                    try:
                        value = "" if properties[ columns[index] ] is None else properties[ columns[index] ]
                    except:
                        value = ""
                        pass

                    values.append( getValidSQLValue(value, columnsType[validColumns[index]]) )

                values.append("ST_SetSRID(ST_GeomFromGeoJSON('" + json.dumps(feature["geometry"]) + "'),4326)")

                sql = sqlInsert.format(dataset=datasetName, columns=columnString, values=",".join(values), geometry_column=GEOMETRY_COLUMN_NAME, suffix="_"+geometryType.lower())
                try:
                    cur.execute(sql)
                    counter += 1
                except:
                    printMessage("---------- Error inserting in the table, skipping: " + sql)

        if len(geometryColumns) == 1:
            # try to rename to default name
            try:
                cur.execute("ALTER TABLE \"{table_name}\" RENAME COLUMN {column_current_name} TO {geometry_column}".format(table_name=datasetName, column_current_name=geometryColumns[0], geometry_column=GEOMETRY_COLUMN_NAME))
                geometryColumns[0] = GEOMETRY_COLUMN_NAME
            except:
                pass

        # spatial index
        for column in geometryColumns:
            createIndex(datasetName, column)

        # optimize table
        analyzeTable(datasetName)
        printMessage("Registros creados: {}".format(counter))


def processJSON(file, datasetName, data=None):
    if data is None:
        with open(file) as jsonFile:
            try:
                processJSON(file, datasetName, data=json.load(jsonFile))
            except:
                printMessage("---------- Error: Archivo no valido")
    else:
        if(len(data)==0):
            printMessage("---------- Error: Archivo sin información")
            return

        geojson = {
            "type": "FeatureCollection",
            "features": []
        }

        latitudColumn = None
        longitudColumn = None

        for column in list(data[0].keys()):
            # geographic columns
            if re.match(CSV_LATITUDE_COLUMN, column) is not None:
                latitudColumn = column
            elif re.match(CSV_LONGITUDE_COLUMN, column) is not None:
                longitudColumn = column

        if latitudColumn is None or longitudColumn is None:
            printMessage("---------- Error: No se encontro información geografica.")
            return

        for row in data:
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [ row.pop(longitudColumn, "0") , row.pop(latitudColumn, "0") ]
                },
                "properties": row
            }

            geojson["features"].append(feature)

        processGeojson(file, datasetName, data=geojson)



def processCSV(file, datasetName, encoding="utf-8"):
    try:
        dataset = pd.read_csv(file, keep_default_na=False, low_memory=False, encoding=encoding, sep=CSV_DELIMITER, error_bad_lines=False, warn_bad_lines=False, dtype=object)
    except:
        encodingAux = "latin-1"

        if( encodingAux != encoding ):
            # try to parse file with another encoding
            processCSV(file, datasetName, encoding=encodingAux)
        else:
            printMessage("---------- Error parseando el archivo")

        return

    json = []
    headers = list(dataset)
    for index, row in dataset.iterrows():
        feature = {}
        for header in headers:
            feature[header] = row[header]
        json.append(feature)

    processJSON(None, datasetName, data=json)


def processSHP(file, datasetName):
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS \"" + datasetName + "\"")

    if not os.path.exists(TEMP_FOLDER):
        os.makedirs(TEMP_FOLDER)

    printMessage("Creando instrucciones sql")

    sqlFilePath = TEMP_FOLDER + "/commands.sql"
    sqlWriter = open(sqlFilePath, "w")
    # subprocess.call(["shp2pgsql", "-c", "-s", GEOMETRY_COLUMN_SRID, "-g", GEOMETRY_COLUMN_NAME, "-I", "-S", file, "public."+datasetName], stdout=sqlWriter)

    # omit -S
    subprocess.call(["shp2pgsql", "-c", "-s", GEOMETRY_COLUMN_SRID, "-g", GEOMETRY_COLUMN_NAME, "-I", file, "public."+datasetName], stdout=sqlWriter)


    sqlWriter.close()

    with open(sqlFilePath, "r") as fp:
        instruccions = []

        instruction = ""
        for line in fp:
            instruction += line[0:-1]

            if line[-2:-1] == ";":
                instruccions.append(instruction)
                instruction = ""

        printMessage("Ejecutando...")

        for instruccion in instruccions:
            cursor.execute(instruccion)

        printMessage("Tabla creada correctamente")


def processKML(file, datasetName, data=None, removeInvalidProperties=False):
    if data is None:
        with open(file) as f:
            processKML(file, datasetName, data=f.read())
    else:
        kml2geojson.main.GEOTYPES = ['Polygon', 'LineString', 'Point']

        try:
            # remove all Document properties
            if removeInvalidProperties:
                data = re.compile("<Documen[^>]*>").sub("<Document>", data)

            geojson = kml2geojson.main.build_feature_collection(md.parseString(data))

            for element in geojson["features"]:
                # extract data from nested tables
                properties = {}
                for prop in element["properties"]:
                    value = str(element["properties"][prop])

                    if "<table" in value:
                        page = htmlParser.document_fromstring(value)

                        for row in page.xpath("body/table")[0].findall("tr"):
                            childs = row.findall("td")
                            if len(childs) == 2:
                                variableName = getValidName(childs[0].text)
                                properties[getValidName(variableName)] = getValidTextValue(childs[1].text)
                    else:
                        if(prop != "styleUrl"):
                            properties[getValidName(prop)] = getValidTextValue(value)
                element["properties"] = properties

            processGeojson(None, datasetName, data=geojson)
        except xml.parsers.expat.ExpatError as err:
            if removeInvalidProperties:
                printMessage("---------- Error parseando el archivo. " + str(err))
            else:
                processKML(file, datasetName, data=data, removeInvalidProperties=True)
            pass
        except:
            print("---------- Error parseando el archivo.")


def processKMZ(file, datasetName):
    try:
        zip = zipfile.ZipFile(file)
        for z in zip.filelist:
            if z.filename[-4:] == '.kml':
                suffix = "_" + z.filename.split(".")[-2]
                processKML(None, datasetName+suffix, data=zip.read(z))
    except zipfile.BadZipFile as error:
        printMessage("---------- Error: El archivo no es tipo zip")


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


def getValidSQLValue(value, columnType):
    auxValue = "NULL"
    try:
        if(columnType == "integer"):
            auxValue = int(obj)
        elif(columnType == "real"):
            auxValue = float(obj)
        else:
            auxValue = "'" + value + "'"
    except: pass

    return str(auxValue)


def getValidTextValue(text):
    return "" if text is None else text.replace("'","''")


def getValidName(currentName):
    return re.compile('[^a-z0-9_]').sub("_", unidecode.unidecode(currentName).lower().strip())


def createTable(datasetName, columns, columnsType):
    sql = "DROP TABLE IF EXISTS \"" + datasetName + "\";CREATE TABLE \"" + datasetName + "\"(gid serial PRIMARY KEY"
    for header in columns:
        sql += ", {column} {column_type}".format(column=header, column_type=columnsType[header])
    sql += ");"

    printMessage("creating table - {}".format(sql))
    conn.cursor().execute(sql)


def analyzeTable(datasetName):
    try:
        conn.cursor().execute("VACUUM ANALYZE \"{}\"".format(datasetName))
        printMessage("tabla optimizada")
    except:
        printMessage("---------- Error optimizando tabla. " + sql)



def createIndex(datasetName, geomColumn):
    indexName = "{dataset}_{geomColumn}_gix".format(dataset=datasetName, geomColumn=geomColumn)
    sql = "CREATE INDEX \"{indexName}\" ON \"{dataset}\" USING GIST ({geomColumn})".format(dataset=datasetName, geomColumn=geomColumn, indexName=indexName)

    try:
        conn.cursor().execute(sql)
        printMessage("índice espacial " + indexName + " creado")
    except:
        printMessage("---------- Error creando el índice espacial. " + sql)


def createGeometryColumn(cur, datasetName, type, suffixColumn=""):
    sql = "SELECT AddGeometryColumn('{dataset}','{geometry_column}',{srid},'{geometry}',2)"
    name = "{geometry_column}{suffixColumn}".format(geometry_column=GEOMETRY_COLUMN_NAME,suffixColumn=suffixColumn);

    cur.execute(sql.format(dataset=datasetName, geometry_column=name, srid=GEOMETRY_COLUMN_SRID, geometry=type))
    return name

def printMessage(text):
    print(text)

    if WRITE_LOG:
        with open('output_script', 'a') as the_file:
            the_file.write(text + "\n")

main()
