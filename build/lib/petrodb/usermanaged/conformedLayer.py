import pandas as pd
import pyodbc as odbc
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


from petrodb.common.userDefinedFunctions import clean_column_name, check_if_file_exists, read_file
from petrodb.config.readConfig import get_config_dict

spark = SparkSession.builder.getOrCreate()


def conformlayer(modname, libname, configpath, temppath, filepath, filename, deltalake_root, delta_database_name,
                 delta_schema_name, deltaLake_table_name, sql_database_name,sql_schema_name,sql_table_name,sql_view_name, dataset_name):
    from datetime import datetime
    import importlib
    importlib.import_module(modname)
    mod = importlib.import_module(modname)
    details_dict = get_config_dict(modname, libname, configpath, temppath)
    print('Process conform layer for : ' + filename)

    # read landing file to dataframe
    # df = spark.read.option("header",True).csv(filepath)

    df = read_file(filepath)

    print("read csv file to df")

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

    # aligning the tech metadata path & table/entity for the file

    #     manifestpath=tech_mf_path+'/'+deltalake_root+'/'+delta_database_name+'/'+delta_schema_name+'/'+deltaLake_table_name
    #     entity=deltaLake_table_name
    #     readDf = (spark.read.format("com.microsoft.cdm")
    #         .option("storage", storeageDevAccountName)
    #         .option("manifestPath", metadata_ct + manifestpath+"/default.manifest.cdm.json")
    #         .option("entity", entity)
    #         .load())

    #     print("metadata loaded")

    # fetching the columnames from tech metadata for which hashkey has to be generated
    hashkey_ds = readDf.select("columnname").filter("upper(hashkeycheck)=='Y'").collect()
    hashkey_list = [i.columnname for i in hashkey_ds]

    print(hashkey_list)
    print("hash key generation completed")

    # fetching the partition column
    partitionkey = readDf.select("columnname").filter("upper(partitionkeycheck)=='Y'").collect()
    partitionkey_list = [i.columnname for i in partitionkey]

    partitioncol = ''
    for i in partitionkey_list:
        partitioncol += str(i + ',')

    partitioncol = partitioncol[:-1]

    # clean up column names for the processing file
    for col in df.columns:
        new_name = clean_column_name(column=col)
        df = df.withColumnRenamed(col, new_name)

    # exclude duplicate values
    ndf = df.distinct()

    # create additional column calhashkey to generate hashkey for the columns mentioned in tech metadata for a give file
    hashdf = ndf.withColumn("calhashkey", upper(sha2(concat_ws("||", *hashkey_list), 256)))

    # create additional column calinsertdate, calupdatedate
    hdf = hashdf.withColumn("calinsertdate", current_timestamp()).withColumn("calupdatedate",
                                                                             lit(None).cast('timestamp'))

    # cast pdate to date
    pdf = hdf.withColumn(partitioncol, F.date_format(partitioncol, 'yyyy-MM-dd').cast(StringType()))

    # fetching the date list
    #     slist = pdf.schema.names
    #     print(slist)
    #     dateval=pdf.select(partitioncol).distinct().collect()
    #     print(dateval)
    disdatecol_list = pdf.select(partitioncol).distinct().rdd.flatMap(lambda x: x).collect()
    print(disdatecol_list)
    #     disdatecol_list=[i.pdate for i in dateval]
    #     print(disdatecol_list)

    # defining the conform path from busniess metadata
    conform_delta_path = details_dict.get("DEFAULTS", {}).get(
        "conformed_path") + deltalake_root + '/' + delta_database_name + '/' + delta_schema_name + '/' + deltaLake_table_name
    print("Conformed Delta Path : " + conform_delta_path)
    # defining the backup paath from busniess metadata
    backup_delta_path = details_dict.get("DEFAULTS", {}).get(
        "backup_path") + deltalake_root + '/' + delta_database_name + '/' + delta_schema_name + '/' + deltaLake_table_name + '_' + datetime.today().strftime(
        '%Y%m%d%H%M%S')

    # check if conform delta path exists for the processing file
    if check_if_file_exists(modname, libname,conform_delta_path):

        # reading the existing conform delta file
        edf = spark.read.format("delta").load(conform_delta_path)
        print("count of existing delta table : " + str(edf.count()))

        # temp view for existing delta file
        edf.filter(F.col(partitioncol).isin(disdatecol_list)).createOrReplaceTempView("old_oil_mobility_apple")

        print("count of new file : " + str(pdf.count()))

        # temp view for the new records in the file in landing path
        pdf.createOrReplaceTempView("new_oil_mobility_apple")

        # comparing the existing data with new data and fetching the records which are not present in the existing delta table
        newdf = spark.sql("Select distinct n.* from new_oil_mobility_apple n where n.calhashkey not in " +
                          "(select calhashkey from old_oil_mobility_apple)")

        print("count of new records : " + str(newdf.count()))

        # if no records found then exit else append to the existing data
        if (newdf.count() != 0):
            # check if backup schema path exists for the processing file schema
            if check_if_file_exists(modname, libname,details_dict.get("DEFAULTS", {}).get(
                    "backup_path") + deltalake_root + '/' + delta_database_name + '/' + delta_schema_name + '/'):
                if "dbutils" in modname:

                    dbutils = getattr(mod, libname)(spark)
                    bkpfiles = dbutils.fs.ls(details_dict.get("DEFAULTS", {}).get(
                        "backup_path") + deltalake_root + '/' + delta_database_name + '/' + delta_schema_name + '/')

                elif "notebookutils" in modname:

                    bkpfiles = getattr(mod, libname).fs.ls(details_dict.get("DEFAULTS", {}).get(
                        "backup_path") + deltalake_root + '/' + delta_database_name + '/' + delta_schema_name + '/')

                # checking the file name in the backup folder
                for f in bkpfiles:

                    # if the file name exists in the backup folder delete the existing one and create a new file
                    if deltaLake_table_name in f.name:
                        if "dbutils" in modname:
                            dbutils = getattr(mod, libname)(spark)
                            dbutils.fs.rm(f.path, True)
                            print('conformed_path: ' + conform_delta_path)
                            print('backup_path: ' + backup_delta_path)
                            dbutils.fs.cp(conform_delta_path, backup_delta_path, True)

                        elif "notebookutils" in modname:
                            getattr(mod, libname).fs.rm(f.path, True)
                            print('conformed_path: ' + conform_delta_path)
                            print('backup_path: ' + backup_delta_path)
                            getattr(mod, libname).fs.cp(conform_delta_path, backup_delta_path, True)

                    # if the file does not exists in backup folder then copy the data form conform to backup
                    else:
                        if "dbutils" in modname:
                            dbutils = getattr(mod, libname)(spark)
                            dbutils.fs.cp(conform_delta_path, backup_delta_path, True)
                        elif "notebookutils" in modname:
                            getattr(mod, libname).fs.cp(conform_delta_path, backup_delta_path, True)

            # if the backup schema path does not exists then create a backup path
            else:
                print('backup_path: ' + backup_delta_path)
                if "dbutils" in modname:
                    dbutils = getattr(mod, libname)(spark)
                    dbutils.fs.cp(conform_delta_path, backup_delta_path, True)
                elif "notebookutils" in modname:
                    getattr(mod, libname).fs.cp(conform_delta_path, backup_delta_path, True)

            # df.join(edf, df.hashrow!=edf.hashrow,'inner').select(df["*"]).show(10)
            print("new records found")
            newdf.write.format("delta").mode("append").partitionBy(partitioncol).option("mergeSchema", "true").save(
                conform_delta_path)



    else:

        print("New file loading to conform layer with the count : " + str(df.count()))

        # write the data to new conform delta path if its a delta table does not exists
        pdf.write.format("delta").option("mergeSchema", "true").partitionBy(partitioncol).save(conform_delta_path)


    # create databricks tables
    dblist = spark.sql("show databases").rdd.flatMap(lambda x: x).collect()
    dbname = details_dict.get("DEFAULTS", {}).get("dbconformeddbname")
    if dbname in dblist:
        print("db exists")
    else:
        spark.sql("CREATE DATABASE {}".format(dbname))

    # spark.sql("show databases").show()

    tbllist = spark.sql("show tables in {}".format(dbname)).rdd.flatMap(lambda x: x).collect()
    tblname = sql_schema_name + '_' + sql_table_name
    print(tblname)
    if tblname in tbllist:
        print("tbl exists")
        spark.sql("DROP TABLE {}.{}".format(dbname, tblname))
        spark.sql("CREATE TABLE {}.{} USING DELTA LOCATION '{}'".format(dbname, tblname, conform_delta_path))
    else:
        print("tbl does not exists")
        spark.sql("CREATE TABLE {}.{} USING DELTA LOCATION '{}'".format(dbname, tblname, conform_delta_path))