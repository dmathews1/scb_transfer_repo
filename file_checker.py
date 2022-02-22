#!/usr/bin/python

import os
import argparse
import re
from sys import exit
import subprocess


def plain_run(base_path, hdfs_path, source_system, country, databases, partition, tables_raw, mapping):
    dbs = [ x for x in databases.split(",") if x ]
    tables = [ x[:-2] for x in tables_raw.split(",") if x[-1] == "p" ]
    db_tables = []
    for database in dbs:
        db_tables.append([ "{}_{}_{}.{}".format(source_system, country, database.lower(), x) for x in tables ])
    db_tables = [x for y in db_tables for x in y ]
    if mapping is not "":
        for i in db_tables:
            index = db_tables.index(i)
            for j in mapping:
                if i.split(".")[0] in j.split(":")[0]:
                    db_tables[index] = (i, j.split(":")[1])
        db_tables = [ "alter table {} add partition ({} = '{}');".format(x[0], x[1], args.partition) for x in db_tables ]
    else:
        db_tables = [ "msck repair table {};".format(x) for x in db_tables ]

    print("\n".join(db_tables))


def detokenise_run(base_path, hdfs_path, source_system, country, databases, partition, tables, mapping):

    base_command = "/usr/hdp/current/hadoop-client/bin/hdfs dfs -ls {}/global/".format(hdfs_path)
    process = subprocess.Popen(base_command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    hdfs_files = [ x.split()[-1] for x in output.split("\n")[1:-1]]

    files = [ x for x in hdfs_files if re.search(r'_tokenization_success', x) and re.search(r'{}'.format(partition), x) ]
    dbs = [ x for x in databases.split(",") if x ]

    ## grab tables
    if args.tables == "":
        table_path = "{}/{}/{}/configs".format(base_path, source_system, country)
        tableslist = [ x for x in os.listdir(table_path) if re.search(r'{}'.format(source_system), x) and re.search(r'{}'.format(country), x) and x.endswith("_tables_config.xml") ]
        tables = [ "_".join(x.split("_")[:-2]) for x in tableslist]
        tables = [ x for x in tables if x ]
    else:
        tables = [ x for x in tables.split(",") if x ]

    if len(tables) < 0:
        print("no tables found")
        exit(1)

    ## check files
    fail_files = [ x for x in hdfs_files if re.search(r'_tokenization_failure_details', x) and re.search(r'{}'.format(partition), x) ]
    if len(fail_files) > 0:
        base_command = "/usr/hdp/current/hadoop-client/bin/hdfs dfs -cat {}".format(fail_files[0])
        process = subprocess.Popen(base_command.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()
        fail_files_lines = [ x.split(",") for x in output.split("\n") if x ]
        fail_files = [ "{}.{}".format(x[0].split('(')[1], x[1]) for x in fail_files_lines if re.search(r'\.*Failure*',x[2]) ]

    db_tables = []
    for database in dbs:
        db_tables.append([ "{}.{}".format(database.lower(), x) for x in tables ])
    db_tables = [x for y in db_tables for x in y if x not in fail_files ]

    if mapping is not "":
        for i in db_tables:
            index = db_tables.index(i)
            for j in mapping:
                if i.split(".")[0] == j.split(":")[0]:
                    db_tables[index] = (i, j.split(":")[1])
        db_tables = [ "alter table {} add partition ({} = '{}');".format(x[0], x[1], partition) for x in db_tables ]
    else:
        db_tables = [ "msck repair table {};".format(x) for x in db_tables ]

    print("\n".join(db_tables))

if __name__=="__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-p','--path',dest='base_path', action='store', default = None)
    parser.add_argument('-f','--hdfs_path',dest='hdfs_path', action='store', default = None)
    parser.add_argument('-s','--source_system', dest='source_system', action='store', default = False)
    parser.add_argument('-c','--country', dest='country', action='store', default = False)
    parser.add_argument('-d','--database', dest='database', action='store', default = False)
    parser.add_argument('-x','--partition', dest='partition', action='store', default = False)
    parser.add_argument('-t','--tables', dest='tables', action='store', default = "")
    parser.add_argument('-m','--mapping', dest='mapping', action='store', default = "")
    parser.add_argument('-r','--runtype', dest='runtype', action='store', default = False)
    args = parser.parse_args()

    partition = args.partition.replace("-","_")
    mapping = args.mapping.split(",")

    if args.runtype in ["plain", "vanilla"]:
        plain_run(args.base_path, args.hdfs_path, args.source_system, args.country, args.database, partition, args.tables, mapping)
    else:
        detokenise_run(args.base_path, args.hdfs_path, args.source_system, args.country, args.database, partition, args.tables, mapping)
