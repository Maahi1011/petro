import pandas as pd
import pyodbc as odbc
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from petrodb.common.userDefinedFunctions import check_if_file_exists
from petrodb.config.readConfig import get_config_dict

spark = SparkSession.builder.getOrCreate()


def analyticsandboxlayer(modname, libname, configpath, temppath, filepath, filename, deltalake_root,
                         delta_database_name, delta_schema_name,
                         deltaLake_table_name,sql_database_name,sql_schema_name,sql_table_name,sql_view_name, dataset_name):
    print('Processing Sandbox layer for file : ' + filename)
    from datetime import datetime
    import importlib
    importlib.import_module(modname)
    mod = importlib.import_module(modname)
    details_dict = get_config_dict(modname, libname, configpath, temppath)
    # aligning manifest path and entity/table from busniess metadata
    #     manifestpath=tech_mf_path+'/'+deltalake_root+'/'+delta_database_name+'/'+delta_schema_name+'/'+deltaLake_table_name
    #     entity=deltaLake_table_name

    # fetching technical metadata info given file
    #     readDf = (spark.read.format("com.microsoft.cdm")
    #         .option("storage", storeageDevAccountName)
    #         .option("manifestPath", metadata_ct + manifestpath+"/default.manifest.cdm.json")
    #         .option("entity", entity)
    #         .load())

    odbcConnectionString = 'Driver={{ODBC Driver 17 for SQL Server}};Server={0};Database={1};UID={2};PWD={3};MARS_Connection=yes;'.format(
        details_dict.get("DEFAULTS", {}).get("dedsqlpoolname"),
        details_dict.get("DEFAULTS", {}).get("dedsqlpooldbname"),
        details_dict.get("DEFAULTS", {}).get("dedsqlpoolusername"), details_dict.get("DEFAULTS", {}).get("password"))
    sqlCon = odbc.connect(odbcConnectionString)
    sqlCon.autocommit = True
    sqlCursor = sqlCon.cursor()
    selectst = "select * from " + details_dict.get("DEFAULTS", {}).get(
        "tech_schema_name") + "." + details_dict.get("DEFAULTS", {}).get(
        "tech_table_name") + " where filename='" + dataset_name + "';"
    filecheck = pd.read_sql(selectst, sqlCon)
    readDf = spark.createDataFrame(filecheck)
    readDf.show()

    # fetch the column names and delta datatypes from technical metadata
    mv_dataset = readDf.select(concat_ws(' : ', readDf["columnname"], readDf["delta"]).alias("col")).sort(
        "colseq").collect()
    mv_list = [i.col for i in mv_dataset]

    md_col_list = [i.col.split(":")[0].strip() for i in mv_dataset]

    # cast column names as per the data types define in technical metadata and store it as a string
    val = ''
    for i in mv_list:
        val += str('cast(' + str(i).replace(":", "as") + ') as ' + i.split(":")[0] + ',')

    # align the conform delta path according to busniess metadata
    conform_delta_path = details_dict.get("DEFAULTS", {}).get(
        "conformed_path") + deltalake_root + '/' + delta_database_name + '/' + delta_schema_name + '/' + deltaLake_table_name

    # align the sandbox delta path according to busniess metadata
    sandbox_delta_path = details_dict.get("DEFAULTS", {}).get(
        "sandbox_path") + deltalake_root + '/' + delta_database_name + '/' + delta_schema_name + '/' + deltaLake_table_name

    # align the backup delta path according to busniess metadata
    backup_delta_path = details_dict.get("DEFAULTS", {}).get(
        "sandbox_backup_path") + deltalake_root + '/' + delta_database_name + '/' + delta_schema_name + '/' + deltaLake_table_name + '_' + datetime.today().strftime(
        '%Y%m%d%H%M%S')

    # fetch the column names from the existing schema to list
    df = spark.sql("select * from delta.`{}`".format(conform_delta_path))
    col_val = df.schema.names

    # fetching the extra columns from existing delta table which are not in technical metadata
    extra_col = list(set(col_val) - set(md_col_list))

    rem_col = ''
    for i in extra_col:
        rem_col += str(i + ',')

    # adding all the columns to a string
    all_col = val + rem_col[:-1]

    # fetching the partition column
    partitionkey = readDf.select("columnname").filter("upper(partitionkeycheck)=='Y'").collect()
    partitionkey_list = [i.columnname for i in partitionkey]

    partitioncol = ''
    for i in partitionkey_list:
        partitioncol += str(i + ',')

    partitioncol = partitioncol[:-1]

    # fetching the date list
    #     dateval=df.select(partitioncol).distinct().collect()
    #     disdatecol_list=[i.pdate for i in dateval]
    #     print(disdatecol_list)
    disdatecol_list = df.select(partitioncol).distinct().rdd.flatMap(lambda x: x).collect()
    print(disdatecol_list)

    # reading data from conform delta table after casting the column names to sandbox datatypes
    dfs = spark.sql("select {} from delta.`{}`".format(all_col, conform_delta_path))

    # checking if sandbox delta table already exists
    if check_if_file_exists(modname, libname,sandbox_delta_path):

        # read data from sandbox to dataframe
        edf = spark.read.format("delta").load(sandbox_delta_path)
        print("existing count of the processing file : " + str(edf.count()))

        # temp view for existing delta file
        edf.filter(F.col(partitioncol).isin(disdatecol_list)).registerTempTable("sandbox_table")

        # create temp table for conform delta table
        dfs.registerTempTable("conform_delta")

        # compare hashkeys and copy the data which does not exists in sandbox to dataframe
        newdf = spark.sql("Select distinct n.* from conform_delta n where n.calhashkey not in " +
                          "(select calhashkey from sandbox_table)")
        print("count of new records : " + str(newdf.count()))

        # copy new records to sandbox delta table
        if (newdf.count() != 0):
            # checking if the back up schema path exists
            # if check_if_file_exists(sandbox_backup_path+deltalake_root+'/'+delta_database_name+'/'+delta_schema_name+'/'):

            #     bkpfiles=mssparkutils.fs.ls(sandbox_backup_path+deltalake_root+'/'+delta_database_name+'/'+delta_schema_name+'/')

            #     #for file names/tables in backup schema
            #     for f in bkpfiles:
            #         #if table exists in backup schema then delete and copy new data from sandbox to backup
            #         if deltaLake_table_name in f.name:
            #             mssparkutils.fs.rm(f.path, True)
            #             print('conformed_path: '+sandbox_delta_path)
            #             print('backup_path: '+backup_delta_path)
            #             mssparkutils.fs.cp(sandbox_delta_path,backup_delta_path, True)

            #         #if delta table does not exists then copy data from sandbox to backup
            #         else:
            #             mssparkutils.fs.cp(sandbox_delta_path,backup_delta_path, True)

            # #if backup schema does not exists them copy directly to back path from sandbox
            # else:
            #     print('backup_path: '+backup_delta_path)
            #     mssparkutils.fs.cp(sandbox_delta_path,backup_delta_path, True)

            # df.join(edf, df.hashrow!=edf.hashrow,'inner').select(df["*"]).show(10)
            print("new records found")
            newdf.write.format("delta").partitionBy(partitioncol).mode("append").option("mergeSchema", "true").save(
                sandbox_delta_path)


    # if sandbox path does not exists then copy the conform data to sandbox
    else:

        print("New file loading to sandbox layer with the count : " + str(dfs.count()))
        # write the data to new enrich delta path if its a delta table does not exists
        dfs.write.format("delta").partitionBy(partitioncol).option("mergeSchema", "true").save(sandbox_delta_path)


    #create databricks tables
    dblist=spark.sql("show databases").rdd.flatMap(lambda x: x).collect()
    dbname=details_dict.get("DEFAULTS", {}).get("dbsandboxdbname")
    if dbname in dblist:
        print("db exists")
    else:
        spark.sql("CREATE DATABASE {}".format(dbname))

    # spark.sql("show databases").show()

    tbllist=spark.sql("show tables in {}".format(dbname)).rdd.flatMap(lambda x: x).collect()
    tblname=sql_schema_name+'_'+sql_table_name
    print(tblname)
    if tblname in tbllist:
        print("tbl exists")
        spark.sql("DROP TABLE {}.{}".format(dbname,tblname))
        spark.sql("CREATE TABLE {}.{} USING DELTA LOCATION '{}'".format(dbname,tblname,sandbox_delta_path))
    else:
        print("tbl does not exists")
        spark.sql("CREATE TABLE {}.{} USING DELTA LOCATION '{}'".format(dbname,tblname,sandbox_delta_path))