#!/usr/bin/python
#coding=utf-8

import os
import re
import sys
import subprocess

if len(sys.argv) < 2:
    print("\nError: Necesitas especificar un directorio")
    sys.exit()

def getTableName(filename):
    return filename.split(".")[0].lower().replace(" ","_").strip()

rootDirectory = sys.argv[1]

supportedTypes = ".*\.(shp|csv|kml|kmz)"
for root, dirnames, filenames in os.walk(rootDirectory):
    for filename in filenames:
        resource = os.path.join(root, filename)

        if re.match(supportedTypes, filename):
            call = os.path.abspath("../export.py") + " " + os.path.abspath(resource) + " " + getTableName(filename)
            subprocess.call("python3 " + call, shell=True, stderr=subprocess.STDOUT)

print("\nDone")
