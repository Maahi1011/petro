from petrodb.common.userDefinedFunctions import check_if_file_exists, buss_metadata, checkschema
from petrodb.config.readConfig import get_config_dict
from petrodb.usermanaged.conformedLayer import conformlayer
from petrodb.usermanaged.edwLayer import sqllayer
from petrodb.usermanaged.enrichedLayer import enrichlayer
from petrodb.usermanaged.rawLayer import rawlayer
from petrodb.usermanaged.sandboxLayer import analyticsandboxlayer

from pyspark.sql import SparkSession


def usermanaged(modname, libname, path, temppath):
    details_dict = get_config_dict(modname, libname, path, temppath)
    # fetching business metadata information for the incoming path
    mdi = buss_metadata(modname, libname, path, temppath, details_dict.get("DEFAULTS", {}).get("incoming_path").strip())
    #     buss_metadata(incoming_path)
    spark = SparkSession.builder.getOrCreate()

    print(mdi)
    manifestpath = ''
    entity = ''

    # copy the files from incoming path to landed & archived path
    for flist in mdi:
        for m in flist:
            print(m[0])
            filepath = m[0]
            filename = str(m[1])
            deltalake_root = str(m[2])
            delta_database_name = str(m[3])
            delta_schema_name = str(m[4])
            deltaLake_table_name = str(m[5])
            raw_layer = str(m[11])

            if (raw_layer == 'Y'):
                rawlayer(modname, libname, path, temppath, filepath, filename, deltalake_root, delta_database_name,
                         delta_schema_name, deltaLake_table_name)

    mdl = buss_metadata(modname, libname, path, temppath, details_dict.get("DEFAULTS", {}).get("landing_path").strip())

    manifestpath = ''
    entity = ''

    # for each filein landing path execute conform, enriched, sandbox & edw layer
    for flist in mdl:
        for m in flist:

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
            dataset_name = str(m[10])
            conformed_layer = str(m[12])
            enrich_layer = str(m[13])
            sandbox_layer = str(m[14])
            edw_layer = str(m[15])

            print("count of records existing delta table")
            countofexistingdata = int()
            conformpath = details_dict.get("DEFAULTS", {}).get(
                "conformed_path") + deltalake_root + '/' + delta_database_name + '/' + delta_schema_name + '/' + deltaLake_table_name
            if check_if_file_exists(modname, libname, conformpath):
                countofexistingdata = spark.read.format("delta").load(conformpath).count()
                print(countofexistingdata)
            else:
                countofexistingdata = 0

            if (conformed_layer == 'Y'):
                print("********** Processing Conform Layer **************")
                conformlayer(modname, libname, path, temppath, filepath, filename, deltalake_root, delta_database_name,
                             delta_schema_name, deltaLake_table_name, sql_database_name, sql_schema_name,
                             sql_table_name, sql_view_name, dataset_name)

            # count of records after the conform process
            countafterconformprocess = spark.read.format("delta").load(conformpath).count()
            print(countafterconformprocess)

            # get the count of new records
            newcount = countafterconformprocess - countofexistingdata
            print(newcount)
            if (enrich_layer == 'Y' and newcount > 0):
                print("********** Processing Enrich Layer **************")
                enrichlayer(modname, libname, path, temppath, filepath, filename, deltalake_root, delta_database_name,
                            delta_schema_name, deltaLake_table_name, sql_database_name, sql_schema_name, sql_table_name,
                            sql_view_name, dataset_name)

            if (sandbox_layer == 'Y' and newcount > 0):
                print("********** Processing Sandbox Layer **************")
                analyticsandboxlayer(modname, libname, path, temppath, filepath, filename, deltalake_root,
                                     delta_database_name, delta_schema_name, deltaLake_table_name, sql_database_name,
                                     sql_schema_name, sql_table_name, sql_view_name, dataset_name)

            if (edw_layer == 'Y' and newcount > 0):
                print("********** Processing EDW Layer **************")
                sqllayer(modname, libname, path, temppath, filepath, filename, deltalake_root, delta_database_name,
                         delta_schema_name, deltaLake_table_name, sql_database_name, sql_schema_name, sql_table_name,
                         sql_view_name, dataset_name)




def businessmetadata(modname, libname, configpath, temppath):
    import importlib
    importlib.import_module(modname)
    mod = importlib.import_module(modname)
    details_dict = get_config_dict(modname, libname, configpath, temppath)
    from pyspark.sql import SparkSession
    import pyodbc as odbc
    import pandas as pd
    spark = SparkSession.builder.getOrCreate()

    print(details_dict.get("DEFAULTS", {}).get("bus_metadata_path"))
    sdf = spark.read.option("header", True).csv(details_dict.get("DEFAULTS", {}).get("bus_metadata_path"))
    df = sdf.toPandas()
    # df.createOrReplaceTempView("business_metadata")
    # df = pd.read_csv(bus_metadata_path)

    odbcConnectionString = 'Driver={{ODBC Driver 17 for SQL Server}};Server={0};Database={1};UID={2};PWD={3};MARS_Connection=yes;'.format(
        details_dict.get("DEFAULTS", {}).get("dedsqlpoolname"),
        details_dict.get("DEFAULTS", {}).get("dedsqlpooldbname"),
        details_dict.get("DEFAULTS", {}).get("dedsqlpoolusername"), details_dict.get("DEFAULTS", {}).get("password"))

    sqlCon = odbc.connect(odbcConnectionString)
    sqlCon.autocommit = True
    sqlCursor = sqlCon.cursor()
    checkschema(details_dict.get("DEFAULTS", {}).get("bus_schema_name"),
                details_dict.get("DEFAULTS", {}).get("dedsqlpoolname"),
                details_dict.get("DEFAULTS", {}).get("dedsqlpooldbname"),
                details_dict.get("DEFAULTS", {}).get("dedsqlpoolusername"),
                details_dict.get("DEFAULTS", {}).get("password"))
    checkfileexists = "SELECT * FROM INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = '" + details_dict.get("DEFAULTS",
                                                                                                          {}).get(
        "bus_schema_name") + "' AND  TABLE_NAME = '" + details_dict.get("DEFAULTS", {}).get("bus_table_name") + "'"
    filecheck = pd.read_sql(checkfileexists, sqlCon)
    print(filecheck.head())
    tbl_name = details_dict.get("DEFAULTS", {}).get("bus_schema_name") + "." + details_dict.get("DEFAULTS", {}).get(
        "bus_table_name")

    spark.conf.set(
        "fs.azure.account.key.{}.dfs.core.windows.net".format(details_dict.get("DEFAULTS", {}).get("rawstname")),
        details_dict.get("DEFAULTS", {}).get("rawaccesskey"))

    # Azure Synapse Connection Configuration
    dwDatabase = details_dict.get("DEFAULTS", {}).get("dedsqlpooldbname")
    dwServer = details_dict.get("DEFAULTS", {}).get("dedsqlpoolname")
    dwUser = details_dict.get("DEFAULTS", {}).get("dedsqlpoolusername")
    dwPass = details_dict.get("DEFAULTS", {}).get("password")
    dwJdbcPort = "1433"
    dwJdbcExtraOptions = "encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
    sqlDwUrl = f"jdbc:sqlserver://{dwServer}:{dwJdbcPort};database={dwDatabase};user={dwUser};password={dwPass};${dwJdbcExtraOptions}"

    # Azure Data Lake Gen 2
    tempDir = "abfss://temp@{}.dfs.core.windows.net/sql_temp".format(details_dict.get("DEFAULTS", {}).get("rawstname"))

    if (filecheck.shape[0] > 0):
        dropst = "drop table " + details_dict.get("DEFAULTS", {}).get("bus_schema_name") + "." + details_dict.get(
            "DEFAULTS", {}).get("bus_table_name") + ";"
        print(dropst)
        sdf.write.mode('overwrite').format("com.databricks.spark.sqldw").option("url", sqlDwUrl).option("tempDir",
                                                                                                        tempDir).option(
            "forwardSparkAzureStorageCredentials", "true").option("dbTable", tbl_name).save()
    else:
        sdf.write.format("com.databricks.spark.sqldw").option("url", sqlDwUrl).option("tempDir", tempDir).option(
            "forwardSparkAzureStorageCredentials", "true").option("dbTable", tbl_name).save()

def technicalmetadata(modname, libname, configpath, temppath):
    from pyspark.sql import SparkSession
    import importlib
    importlib.import_module(modname)
    mod = importlib.import_module(modname)
    details_dict = get_config_dict(modname, libname, configpath, temppath)
    import pyodbc as odbc
    import pandas as pd
    spark = SparkSession.builder.getOrCreate()

    print(details_dict.get("DEFAULTS", {}).get("tech_metadata_path"))
    sdf = spark.read.option("header", True).csv(details_dict.get("DEFAULTS", {}).get("tech_metadata_path"))


    odbcConnectionString = 'Driver={{ODBC Driver 17 for SQL Server}};Server={0};Database={1};UID={2};PWD={3};MARS_Connection=yes;'.format(
        details_dict.get("DEFAULTS", {}).get("dedsqlpoolname"), details_dict.get("DEFAULTS", {}).get("dedsqlpooldbname"), details_dict.get("DEFAULTS", {}).get("dedsqlpoolusername"), details_dict.get("DEFAULTS", {}).get("password"))

    sqlCon = odbc.connect(odbcConnectionString)
    sqlCon.autocommit = True
    sqlCursor = sqlCon.cursor()
    checkschema(details_dict.get("DEFAULTS", {}).get("tech_schema_name"), details_dict.get("DEFAULTS", {}).get("dedsqlpoolname"), details_dict.get("DEFAULTS", {}).get("dedsqlpooldbname"), details_dict.get("DEFAULTS", {}).get("dedsqlpoolusername"), details_dict.get("DEFAULTS", {}).get("password"))
    checkfileexists = "SELECT * FROM INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = '" + details_dict.get("DEFAULTS", {}).get("tech_schema_name") + "' AND  TABLE_NAME = '" + details_dict.get("DEFAULTS", {}).get("tech_table_name") + "'"
    filecheck = pd.read_sql(checkfileexists, sqlCon)
    print(filecheck.head())

    tbl_name = details_dict.get("DEFAULTS", {}).get("tech_schema_name") + "." + details_dict.get("DEFAULTS", {}).get(
        "tech_table_name")

    spark.conf.set(
        "fs.azure.account.key.{}.dfs.core.windows.net".format(details_dict.get("DEFAULTS", {}).get("rawstname")),
        details_dict.get("DEFAULTS", {}).get("rawaccesskey"))

    # Azure Synapse Connection Configuration
    dwDatabase = details_dict.get("DEFAULTS", {}).get("dedsqlpooldbname")
    dwServer = details_dict.get("DEFAULTS", {}).get("dedsqlpoolname")
    dwUser = details_dict.get("DEFAULTS", {}).get("dedsqlpoolusername")
    dwPass = details_dict.get("DEFAULTS", {}).get("password")
    dwJdbcPort = "1433"
    dwJdbcExtraOptions = "encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
    sqlDwUrl = f"jdbc:sqlserver://{dwServer}:{dwJdbcPort};database={dwDatabase};user={dwUser};password={dwPass};${dwJdbcExtraOptions}"

    # Azure Data Lake Gen 2
    tempDir = "abfss://temp@{}.dfs.core.windows.net/sql_tech_temp".format(details_dict.get("DEFAULTS", {}).get("rawstname"))

    if (filecheck.shape[0] > 0):
        dropst = "drop table " + details_dict.get("DEFAULTS", {}).get("tech_schema_name") + "." + details_dict.get(
            "DEFAULTS", {}).get("tech_table_name") + ";"
        print(dropst)
        sdf.write.mode('overwrite').format("com.databricks.spark.sqldw").option("url", sqlDwUrl).option("tempDir",
                                                                                                        tempDir).option(
            "forwardSparkAzureStorageCredentials", "true").option("dbTable", tbl_name).save()


    else:
        sdf.write.format("com.databricks.spark.sqldw").option("url", sqlDwUrl).option("tempDir", tempDir).option(
            "forwardSparkAzureStorageCredentials", "true").option("dbTable", tbl_name).save()

