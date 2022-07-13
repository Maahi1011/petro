from pyspark.sql import SparkSession

from mstest.common.userDefinedFunctions import buss_metadata
from mstest.usermanaged.conformedLayer import conformlayer
from mstest.usermanaged.enrichedLayer import enrichlayer
from mstest.usermanaged.rawLayer import *
from mstest.config.readConfig import *
from mstest.usermanaged.sandboxLayer import analyticsandboxlayer

spark = SparkSession.builder.appName("petro.com").getOrCreate()


# def print_name(modname, libname, path):
#     importlib.import_module(modname)
#     mod = importlib.import_module(modname)
#     file_name = []
#     details_dict = get_config_dict(path)
#     print(details_dict)
#     input_path = details_dict.get("incoming_path")
#     # copying the files from landing path to a list
#     file_info = getattr(mod, libname).fs.ls(str(input_path).strip())
#     for f in file_info:
#         file_name.append([f.path, f.name])
#
#     print(file_name)
# raw layer

def usermanaged(modname, libname, path):
    details_dict = get_config_dict(path)
    print(details_dict)
    # fetching business metadata information for the incoming path
    mdl = buss_metadata(modname, libname, path, str(details_dict.get("incoming_path")).strip())

    # copy the files from incoming path to landed & archived path
    for m in mdl:
        print(m[0])
        filepath = m[0]
        filename = str(m[1])
        deltalake_root = str(m[2])
        delta_database_name = str(m[3])
        delta_schema_name = str(m[4])
        deltaLake_table_name = str(m[5])

        rawlayer(modname, libname, path, filepath, filename, deltalake_root, delta_database_name, delta_schema_name,
                 deltaLake_table_name)

    mdl = buss_metadata(modname, libname, path, str(details_dict.get("landing_path")).strip())
    manifestpath = ''
    entity = ''
    # copy the files from incoming path to landed & archived path
    for m in mdl:
        print(m[0])
        filepath = m[0]
        filename = str(m[1])
        deltalake_root = str(m[2])
        delta_database_name = str(m[3])
        delta_schema_name = str(m[4])
        deltaLake_table_name = str(m[5])
        sql_database_name = str(m[6])
        sql_schema_name = str(m[7])
        sql_table_name = str(m[8])
        sql_view_name = str(m[9])

        # count of records existing delta table
        conformpath = str(details_dict.get("conformed_path")).strip() + deltalake_root + '/' + delta_database_name + '/' + delta_schema_name + '/' + deltaLake_table_name
        if check_if_file_exists(modname, libname, conformpath):
            countofexistingdata = spark.read.format("delta").load(conformpath).count()
            print(countofexistingdata)
        else:
            countofexistingdata = 0

        print("********** Processing Conform Layer **************")
        conformlayer(modname, libname, path, filepath, filename, deltalake_root, delta_database_name, delta_schema_name, deltaLake_table_name)

        # count of records after the conform process
        countafterconformprocess = spark.read.format("delta").load(conformpath).count()
        print(countafterconformprocess)

        # get the count of new records
        newcount = countafterconformprocess - countofexistingdata
        print(newcount)

        if (newcount > 0):
            print("********** Processing Enrich Layer **************")
            enrichlayer(modname, libname, path, filepath, filename, deltalake_root, delta_database_name, delta_schema_name,
                        deltaLake_table_name)

            print("********** Processing Sandbox Layer **************")
            analyticsandboxlayer(modname, libname, path, filepath, filename, deltalake_root, delta_database_name, delta_schema_name,
                                 deltaLake_table_name)


