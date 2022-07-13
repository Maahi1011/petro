import importlib
import re
import pyodbc as odbc
import pandas as pd
from pyspark.sql import SparkSession
from mstest.config.readConfig import get_config_dict

spark = SparkSession.builder.appName("petro.com").getOrCreate()


def check_if_file_exists(modname, libname, path):
    """ Check if a file exists in blob store using spark """
    importlib.import_module(modname)
    mod = importlib.import_module(modname)

    exists = False
    try:
        getattr(mod, libname).fs.ls(path)
        exists = True
    except Exception as e:
        pass
    return exists


# def split_name(name, delimiter='__'):
#         #""" Split file name and return back components """
#         name_structure_split = name.split(delimiter)
#         database = name_structure_split[0]
#         schema = name_structure_split[1]
#         table = name_structure_split[2]
#         #file_name = "_".join(name_structure_split[2:])
#         file_name = name.replace('__', '_')


#         return database, schema, table, file_name


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


def buss_metadata(modname, libname, config_path,path):
    print()
    importlib.import_module(modname)
    mod = importlib.import_module(modname)
    file_name = []
    details_dict = get_config_dict(config_path)
    print(str(details_dict.get("bus_metadata_delta_path")).strip())
    # copying the files from landing path to a list
    file_info = getattr(mod, libname).fs.ls(path)
    for f in file_info:
        file_name.append([f.path, f.name])

    file_col = ["path", "name"]
    fileDF = spark.createDataFrame(data=file_name, schema=file_col)
    fileDF.registerTempTable("files")

    # fetching the business metadata for the files in landing folder
    metadatadf = spark.sql(
        "select f.path,f.name,b.deltaLake_root,b.delta_database_name,b.delta_schema_name,b.deltaLake_table_name,b.sql_database_name,"
        + "b.sql_schema_name,b.sql_table_name,b.sql_view_name from files f inner join "
        + "delta.`{}` b ON lower(regexp_replace(f.name,'__','_')) LIKE CONCAT('%', lower(trim(b.dataset)), '%') ".format(
            str(details_dict.get("bus_metadata_delta_path")).strip()))

    metadatalist = metadatadf.collect()

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
