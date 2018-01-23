#!/usr/bin/python3
#coding=utf-8

import os
import re
import sys
import subprocess

if len(sys.argv) < 2:
    print("Error: Necesitas especificar un directorio")
    sys.exit()

def getTableName(filename):
    return filename.split(".")[0]

rootDirectory = sys.argv[1]

processedFiles = open("processed_files", "w+").read().splitlines()

supportedTypes = ".*\.(shp|csv|kml|kmz|geojson|json|zip)"
for root, dirnames, filenames in os.walk(rootDirectory):
    for filename in filenames:
        resource = os.path.join(root, filename)

        if filename in processedFiles:
            print("Omitiendo archivo ya procesado: " + filename)
        elif re.match(supportedTypes, filename):
            call = os.path.abspath("fileToPostgis.py") + " " + os.path.abspath(resource) + " " + getTableName(filename)
            subprocess.call("python3 " + call, shell=True, stderr=subprocess.STDOUT)
            print("")

            with open("processed_files", "a") as the_file:
                the_file.write(filename + "\n")

print("Done")
