#!/usr/bin/env python

import os
import argparse
import re
from sys import exit
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument('-b','--base_path',dest='base_path', action='store', default = None)
parser.add_argument('-d','--databases',dest='databases', action='store', default = None)
parser.add_argument('-t','--tables', dest='tables', action='store', default = None)
parser.add_argument('-m','--mapping', dest='mapping', action='store', default = None)
parser.add_argument('-p','--partition', dest='partition', action='store', default = None)
parser.add_argument('-u','--user_principal', dest='user_principal', action='store', default = None)
parser.add_argument('-k','--keytab', dest='keytab', action='store', default = None)
parser.add_argument('-f','--function', dest='function', action='store', default = None)
args = parser.parse_args()

os.environ["KRB5CCNAME"] = "FILE:/tmp/krb5cc_%{uid}"
base_command = "kinit -kt {} {}".format(args.keytab, args.user_principal)
process = subprocess.Popen(base_command.split(), stdout=subprocess.PIPE)
output, error = process.communicate()

table_list = [ x[:-2] for x in args.tables.split(",") if x[-1] == args.function.strip() ]
mapping = args.mapping.split(",")
for db in args.databases.split(","):
    col_name = [ x.split(":")[1] for x in mapping if db in x ][0]
    for tbl in table_list:
        hdfs_command = "hdfs dfs -ls {}/{}/{}/{}={}".format(args.base_path, db, tbl, col_name, args.partition)
        hdfs_process = subprocess.Popen(hdfs_command.split(), stdout=subprocess.PIPE)
        output, error = hdfs_process.communicate()
        if output:
            print("\n".join([x.split(" ")[-1] for x in output.split("\n") if x][1:]))
