import importlib
import re
import pyodbc as odbc
import pandas as pd
from pyspark.sql import SparkSession
from petrodb.config.readConfig import get_config_dict

spark = SparkSession.builder.appName("petro.com").getOrCreate()


def check_if_file_exists(modname, libname, path):
    import importlib
    importlib.import_module(modname)
    mod = importlib.import_module(modname)
    """ Check if a file exists in blob store using spark """

    exists = False
    try:
        if "dbutils" in modname:

            dbutils = getattr(mod, libname)(spark)
            dbutils.fs.ls(path)
            exists = True

        elif "notebookutils" in modname:

            getattr(mod, libname).fs.ls(path)
            exists = True

    except Exception as e:
        pass
    return exists


def clean_column_name(column):
    """
    Clean up column names to conform with our data standards
    :param column:
    :return:
    """
    column = column.lower()
    column = column.replace('%', "percent")
    column = re.sub(r'(?<=\[)[^]]+(?=\])', '', column)
    column = column.rstrip()
    column = column.lstrip()
    column = column.replace(']', "")
    column = column.replace('[', "")
    column = column.replace('(', "")
    column = column.replace(';', "")
    column = column.replace(')', "")
    column = column.replace(' ', "_")
    column = column.replace(',', "")
    column = column.replace('/', "_")
    column = column.replace(':', "_")

    return column


def buss_metadata(modname, libname, configpath, temppath, path):
    import importlib
    importlib.import_module(modname)
    mod = importlib.import_module(modname)
    details_dict = get_config_dict(modname, libname, configpath, temppath)

    file_name = []

    odbcConnectionString = 'Driver={{ODBC Driver 17 for SQL Server}};Server={0};Database={1};UID={2};PWD={3};MARS_Connection=yes;'.format(
        details_dict.get("DEFAULTS",{}).get("dedsqlpoolname"), details_dict.get("DEFAULTS",{}).get("dedsqlpooldbname"), details_dict.get("DEFAULTS",{}).get("dedsqlpoolusername"), details_dict.get("DEFAULTS",{}).get("password"))

    sqlCon = odbc.connect(odbcConnectionString)
    sqlCon.autocommit = True
    sqlCursor = sqlCon.cursor()

    # copying the files from landing path to a list
    if "dbutils" in modname:
        dbutils = getattr(mod, libname)(spark)
        dbutils.fs.cp(configpath, temppath)
        file_info = dbutils.fs.ls(path)
        for f in file_info:
            d_name = f.name.replace("__", "_")
            file_name.append([f.path, f.name, d_name])
    elif "notebookutils" in modname:

        file_info = getattr(mod, libname).fs.ls(path)
        for f in file_info:
            d_name = f.name.replace("__", "_")
            file_name.append([f.path, f.name, d_name])

    # print(file_name)
    metadatalist = []
    for i in file_name:
        selectst = "select distinct cast('" + i[0] + "' as varchar(500)) as path, cast('" + i[
            1] + "' as varchar(100)) as name, b.deltaLake_root,b.delta_database_name,b.delta_schema_name,b.deltaLake_table_name,b.sql_database_name, b.sql_schema_name,b.sql_table_name,b.sql_view_name, b.dataset ,b.raw_layer,b.conformed_layer,b.enrich_layer,b.sandbox_layer,b.edw_layer from " + details_dict.get(
            "DEFAULTS", {}).get("bus_schema_name") + "." + details_dict.get("DEFAULTS", {}).get(
            "bus_table_name") + " b where '" + i[2] + "' LIKE CONCAT('%', lower(trim(b.dataset)), '%') ;"
        #     print(selectst)
        filecheck = pd.read_sql(selectst, sqlCon)
        #     print(filecheck.head())
        metadatalist.append(filecheck.values.tolist())

    return metadatalist


def checkschema(schema, dedsqlpoolname, dedsqlpooldbname, dedsqlpoolusername, password):
    # print(dedsqlpoolname+','+dedsqlpooldbname+','+dedsqlpoolusername+','+password)

    # Establishing the connection
    odbcConnectionString = 'Driver={{ODBC Driver 17 for SQL Server}};Server={0};Database={1};UID={2};PWD={3}'.format(
        dedsqlpoolname, dedsqlpooldbname, dedsqlpoolusername, password)

    sqlCon = odbc.connect(odbcConnectionString)
    sqlCon.autocommit = True
    sqlCursor = sqlCon.cursor()

    checkschemaexists = "SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME ='{0}' ".format(schema)
    print(checkschemaexists)
    schemacheck = pd.read_sql(checkschemaexists, sqlCon)
    schema
    if (schemacheck.shape[0] > 0):
        print("schema exists")
    else:
        ctschema = "create schema {0}".format(schema)
        print(ctschema)
        sqlCursor.execute(ctschema)
    sqlCon.close()


def read_file(path):
    if ".csv" in path:

        df = spark.read.option("header", True).csv(path)

    else:
        print(path + " file is not supported in the framework")

    return df
