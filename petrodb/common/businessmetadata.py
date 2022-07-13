from petrodb.common.userDefinedFunctions import checkschema
from petrodb.config.readConfig import get_config_dict


def businessmetadatalayer(modname, libname, configpath, temppath):
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



