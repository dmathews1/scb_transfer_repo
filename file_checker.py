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
args = parser.parse_args()

base_command = "hdfs dfs -ls {}".format(args.hdfs_path)
process = subprocess.Popen(base_command.split(), stdout=subprocess.PIPE)
output, error = process.communicate()
hdfs_files = [ x.split()[-1] for x in output.split("\n")[1:-1]]

##fail files
check_fail_path = "hdfs dfs -ls {}/global/".format(args.hdfs_path)
process_fail_files = subprocess.Popen(check_fail_path.split(), stdout=subprocess.PIPE)
fail_output, fail_error = process_fail_files.communicate()
fail_files = [ x.split()[-1] for x in fail_output.split("\n")[1:-1]]

files = [ x for x in hdfs_files if re.search(r'\.*success*', x) ]

if len(files) == 0:
    print("failure noted at a global level, not continuing")
    exit(1)

dbs = [ x for x in args.database.split(",") if x ]

## grab tables

table_path = "{}/{}/{}/configs".format(args.base_path, args.source_system, args.country)
tableslist = [ x for x in os.listdir(table_path) if re.search(r'\.*{}*'.format(args.source_system), x) and re.search(r'\.*{}*'.format(args.country), x) and x.endswith(".xml") ]
tables = [ "_".join(x.split("_")[2:-2]) for x in tableslist]
tables = [ x for x in tables if x ]

if len(tables) < 0:
    print("no tables found")
    exit(1)

## check files

if len(fail_files) == 1:
    base_command = "hdfs dfs -cat {}/global/{}_{}_tokenization_failure_details".format(args.hdfs_path, args.source_system, args.country)
    process = subprocess.Popen(base_command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    fail_files = [ "_".join(x.split("_")[2:]) for x in output.split("\n") if x ]
    tables = [ x for x in tables if x not in fail_files ]
else:
    print("no failed tables")

db_tables = []
for database in dbs:
    db_tables.append([ "msck repair table {}.{}".format(database, x) for x in tables ])
db_tables = [x for y in db_tables for x in y]

print("\n".join(db_tables))
