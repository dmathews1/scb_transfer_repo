#!/usr/bin/python

import os
import argparse
import re
from sys import exit
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument('-p','--path',dest='base_path', action='store', default = None)
parser.add_argument('-f','--hdfs_path',dest='hdfs_path', action='store', default = None)
parser.add_argument('-s','--source_system', dest='source_system', action='store', default = False)
parser.add_argument('-c','--country', dest='country', action='store', default = False)
parser.add_argument('-d','--database', dest='database', action='store', default = False)
parser.add_argument('-x','--partition', dest='partition', action='store', default = False)
parser.add_argument('-t','--tables', dest='tables', action='store', default = "")
parser.add_argument('-r','--replay', dest='replay', action='store_true', default = False)
args = parser.parse_args()

args.partition = args.partition.replace("-","_")

base_command = "/usr/hdp/current/hadoop-client/bin/hdfs dfs -ls {}/global/".format(args.hdfs_path)
process = subprocess.Popen(base_command.split(), stdout=subprocess.PIPE)
output, error = process.communicate()
hdfs_files = [ x.split()[-1] for x in output.split("\n")[1:-1]]

files = [ x for x in hdfs_files if re.search(r'_tokenization_success', x) and re.search(r'{}'.format(args.partition), x) ]
dbs = [ x for x in args.database.split(",") if x ]

## grab tables
if args.tables == "":
    table_path = "{}/{}/{}/configs".format(args.base_path, args.source_system, args.country)
    tableslist = [ x for x in os.listdir(table_path) if re.search(r'{}'.format(args.source_system), x) and re.search(r'{}'.format(args.country), x) and x.endswith(".xml") and not x.endswith("_param.xml") ]
    tables = [ "_".join(x.split("_")[:-2]) for x in tableslist]
    tables = [ x for x in tables if x ]
else:
    tables = [ x for x in args.tables.split(",") if x ]

if len(tables) < 0:
    print("no tables found")
    exit(1)

## check files
fail_files = [ x for x in hdfs_files if re.search(r'_tokenization_failure_details', x) and re.search(r'{}'.format(args.partition), x) ]
if len(fail_files) > 0:
    base_command = "/usr/hdp/current/hadoop-client/bin/hdfs dfs -cat {}".format(fail_files[0])
    process = subprocess.Popen(base_command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    fail_files_lines = [ x.split(",") for x in output.split("\n") if x ]
    fail_files = [ "{}.{}".format(x[0].split('(')[1], x[1]) for x in fail_files_lines if re.search(r'\.*Failure*',x[2]) ]

db_tables = []
for database in dbs:
    db_tables.append([ "{}_{}_{}.{}".format(args.source_system,args.country,database.lower(), x) for x in tables ])
db_tables = [x for y in db_tables for x in y]
db_tables = [ "msck repair table {}".format(x) for x in db_tables if x not in fail_files ]

if not args.replay:
    print("\n".join(db_tables))
else:
    print("\n".join(fail_files))

