#!/usr/bin/python

import os
import argparse
import re
from sys import exit

parser = argparse.ArgumentParser()
parser.add_argument('-p','--path',dest='base_path', action='store', default = None)
parser.add_argument('-s','--source_system', dest='source_system', action='store', default = False)
parser.add_argument('-c','--country', dest='country', action='store', default = False)
parser.add_argument('-d','--database', dest='database', action='store', default = False)
args = parser.parse_args()

full_path = "{}/{}/{}/status".format(args.base_path, args.source_system, args.country)
files = [ x for x in os.listdir(full_path) if re.search(r'\.*success*', x) ]

if len(files) == 0:
    print("failure noted at a global level, not continuing")
    exit(1)

dbs = [ x for x in args.database.split(",") if x ]

## grab tables

table_path = "{}/{}/{}/configs".format(args.base_path, args.source_system, args.country)
tableslist = [ x for x in os.listdir(table_path) if re.search(r'\.*{}*'.format(args.source_system), x) and re.search(r'\.*{}*'.format(args.country), x) and x.endswith(".xml") ]  
tables = [ "_".join(x.split("_")[2:-2]) for x in tableslist ]

if len(tables) < 0:
    print("no tables found")
    exit(1)

## check files

check_fail_path = "{}/global/{}_{}_tokenization_failure_details".format(full_path, args.source_system, args.country)

if os.path.exists(check_fail_path):
    with open(check_fail_path) as f:
        failed_tables_list = [ x for x in f.read().split("\n") if x ]
    failed_tables = [ "_".join(x.split("_")[2:]) for x in failed_tables_list ]
#    print(failed_tables)
    if len(failed_tables) > 0:
        tables = [ x for x in tables if x not in failed_tables ]
else:
    print("no failed tables")

db_tables = []
for database in dbs:
    db_tables.append([ "{}.{}".format(database, x) for x in tables ])
db_tables = [x for y in db_tables for x in y]

print("\n".join(db_tables))

