#!/usr/bin/python

import os
import argparse
import re
from sys import exit
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument('-p','--path',dest='base_path', action='store', default = None) ## only used if table mapping not provided
parser.add_argument('-f','--hdfs_path',dest='hdfs_path', action='store', default = None)
parser.add_argument('-s','--source_system', dest='source_system', action='store', default = False)
parser.add_argument('-c','--country', dest='country', action='store', default = False)
parser.add_argument('-d','--database', dest='database', action='store', default = False)
parser.add_argument('-x','--partition', dest='partition', action='store', default = False)
parser.add_argument('-t','--tables', dest='tables', action='store', default = "")
parser.add_argument('-m','--mapping', dest='mapping', action='store', default = "")
args = parser.parse_args()

partition = args.partition.replace("-","_")

mapping = args.mapping.split(",")

base_command = "/usr/bin/hdfs dfs -ls {}/global/".format(args.hdfs_path)
process = subprocess.Popen(base_command.split(), stdout=subprocess.PIPE)
output, error = process.communicate()
hdfs_files = [ x.split()[-1] for x in output.split("\n")[1:-1]]

files = [ x for x in hdfs_files if re.search(r'_tokenization_success', x) and re.search(r'{}'.format(partition), x) ]
dbs = [ x for x in args.database.split(",") if x ]

## grab tables
if args.tables == "":
    table_path = "{}/{}/{}/configs".format(args.base_path, args.source_system, args.country)
    tableslist = [ x for x in os.listdir(table_path) if re.search(r'{}'.format(args.source_system), x) and re.search(r'{}'.format(args.country), x) and x.endswith("_tables_config.xml") ]
    tables = [ "_".join(x.split("_")[:-2]) for x in tableslist]
    tables = [ x for x in tables if x ]
else:
    tables = [ x for x in args.tables.split(",") if x ]
    if tables[0][-2] == "|": 
        tables = list(set([ x[:-2] for x in tables ]))

if len(tables) < 0:
    print("no tables found")
    exit(1)

## check files
token_fail_command = "/usr/bin/hdfs dfs -ls {}/{}_{}_tokenization_failure_details_{}/".format(args.hdfs_path, args.source_system, args.country, partition)
token_fail_process = subprocess.Popen(token_fail_command.split(), stdout=subprocess.PIPE)
token_fail_output, token_fail_error = process.communicate()
token_fail_files = [ x.split()[-1] for x in token_fail_output.split("\n")[1:-1]]

fail_files = []
if len(token_fail_files) > 0:
    for file in token_fail_files:
        base_command = "/usr/bin/hdfs dfs -cat {}".format(token_fail_files[0])
        process = subprocess.Popen(base_command.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()
        fail_files_lines = [ x.split(",") for x in output.split("\n") if x ]
        fail_files.append([ "{}.{}".format(x[0].split('(')[1], x[1]) for x in fail_files_lines if re.search(r'\.*Failure*',x[2]) ][0])

db_tables = []
for database in dbs:
    db_tables.append([ "{}.{}".format(database.lower(), x) for x in tables ])
db_tables = [x for y in db_tables for x in y if x not in fail_files ]

if args.mapping is not "":
    for i in db_tables:
        index = db_tables.index(i)
        for j in mapping:
            if i.split(".")[0] == j.split(":")[0]:
                db_tables[index] = (i, j.split(":")[1])
    db_tables = [ "alter table {} add partition ({} = '{}');".format(x[0], x[1], args.partition) for x in db_tables ]
else:
    db_tables = [ "msck repair table {};".format(x) for x in db_tables ]

print("\n".join(db_tables))

