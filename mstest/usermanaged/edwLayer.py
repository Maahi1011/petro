from pyspark.sql import *
from pyspark.sql.functions import *
import re
from datetime import datetime
from com.microsoft.spark.sqlanalytics.Constants import Constants
import pyodbc as odbc
import pandas as pd

ctallcolval = ''
all_col = ''


def sqllayer(filepath, filename, deltalake_root, delta_database_name, delta_schema_name, deltaLake_table_name,
             sql_database_name, sql_schema_name, sql_table_name, sql_view_name):
    print('Processing SQLpool layer : ' + filename)

    # fetching technical metadata for the give file
    mfpath = tech_mf_path + '/' + deltalake_root + '/' + delta_database_name + '/' + delta_schema_name + '/' + deltaLake_table_name
    entity = deltaLake_table_name

    readDf = (spark.read.format("com.microsoft.cdm")
              .option("storage", storeageDevAccountName)
              .option("manifestPath", metadata_ct + mfpath + "/default.manifest.cdm.json")
              .option("entity", entity)
              .load())

    clusterindexkey = readDf.select("columnname").filter("upper(clusterindexcol)=='Y'").collect()
    clusterindex_list = [i.columnname for i in clusterindexkey]
    clusterindex_col = ''
    for i in clusterindex_list:
        clusterindex_col += i

    # read column names with data types delimited with :
    mv_dataset = readDf.select(concat_ws(' : ', readDf["columnname"], readDf["sql"]).alias("col")).sort(
        "colseq").collect()
    mv_list = [i.col for i in mv_dataset]

    # reading only column names to variable from tech metadata
    md_col_list = [i.col.split(":")[0].strip() for i in mv_dataset]

    # read col names and data type to variable which is passed as casting columns to queries
    val = ''
    for i in mv_list:
        val += str('cast(' + str(i).replace(":", "as") + ') as ' + i.split(":")[0] + ',')

    # read col names and data type to variable which is passed to create table syntax
    ctcolval = ''
    for i in mv_list:
        ctcolval += str(str(i).replace(":", " ") + ',')
    # print(ctcolval)

    # adding derived columns to create table syntax variable
    ctallcolval = ctcolval + 'calhashkey  varchar(300),calinsertdate  date,calupdatedate  date'

    # aliging enrich file name as per business metadata
    enrich_delta_path = enrich_path + deltalake_root + '/' + delta_database_name + '/' + delta_schema_name + '/' + deltaLake_table_name

    # read enrich data to dataframe
    df = spark.sql("select * from delta.`{}`".format(enrich_delta_path))

    col_val = df.schema.names

    # fetching derived columns i.e calhaskey, calinsertdate, calupdatedate
    extra_col = list(set(col_val) - set(md_col_list))

    rem_col = ''
    for i in extra_col:
        rem_col += str(i + ',')

    all_col = val + rem_col[:-1]

    # ****** add where calinsertdate*****#
    dfs = spark.sql("select {} from delta.`{}`".format(all_col, enrich_delta_path))

    # aligning database.schema.tablename for given file and staging
    dbval = sql_database_name + '.' + sql_schema_name + '.' + sql_table_name
    stagingtable = sql_database_name + '.staging_' + sql_schema_name + '.' + sql_table_name

    # Open Sql Server Conn & cursor
    odbcConnectionString = 'Driver={{ODBC Driver 17 for SQL Server}};Server={0};Database={1};UID={2};PWD={3};MARS_Connection=yes;'.format(
        dedsqlpoolname, dedsqlpooldbname, dedsqlpoolusername, password)

    sqlCon = odbc.connect(odbcConnectionString)
    sqlCon.autocommit = True
    sqlCursor = sqlCon.cursor()

    print("established sql pool connection")

    # checks if file schema & staging file schema exists else create
    checkschema(sql_schema_name, dedsqlpoolname, dedsqlpooldbname, dedsqlpoolusername, password)
    checkschema('staging_' + sql_schema_name, dedsqlpoolname, dedsqlpooldbname, dedsqlpoolusername, password)

    # query to check if table and staging table exists in sqlpool
    checkfileexists = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + sql_schema_name + "' AND  TABLE_NAME = '" + sql_table_name + "'"
    checkstagingfileexists = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'staging_" + sql_schema_name + "' AND  TABLE_NAME = '" + sql_table_name + "'"

    # execute table check query and check if table exists
    filecheck = pd.read_sql(checkfileexists, sqlCon)

    # if table exists
    if (filecheck.shape[0] > 0):

        # read sqlpool table data to dataframe
        sqldf = spark.read.synapsesql(dbval).registerTempTable("sqlpooltable")
        # read enrich table data to dataframe
        dfs.registerTempTable("enrichtable")

        # fetch new records to dataframe
        newdf = spark.sql(
            "Select {0} from enrichtable n where n.calhashkey not in (select calhashkey from sqlpooltable)".format(
                all_col))
        print("count of new records : " + str(newdf.count()))

        # if new records found
        if (newdf.count() != 0):

            # run staging table check query and check if staging table exists
            stagingfilecheck = pd.read_sql(checkstagingfileexists, sqlCon)

            # if staging table exists
            if (stagingfilecheck.shape[0] > 0):

                # drop the existing staging table
                dropst = "DROP TABLE staging_{0}.{1};".format(sql_schema_name, sql_table_name)
                sqlCursor.execute(dropst)
                print(dropst)
                # copy data from enrich to staging
                newdf.write.synapsesql(stagingtable, Constants.INTERNAL)
            # if staging table doesnot exists copy the data from enrich to staging
            else:
                newdf.write.synapsesql(stagingtable, Constants.INTERNAL)

            print("Copy data from enrich to staging completed")

            # insert new records from staging table to main table and cast to its defined datatypes
            writetotable = "INSERT INTO {0} select distinct {1} FROM {2} ".format(dbval, all_col, stagingtable)
            sqlCursor.execute(writetotable)
            print("Copy data from staging to main table completed")
            # print(writetotable)

    # if table does not exists
    else:

        # generate create table syntax
        ctval = "CREATE TABLE " + sql_schema_name + "." + sql_table_name + "( " + ctallcolval + ") WITH (CLUSTERED INDEX (" + clusterindex_col[
                                                                                                                              :-1] + "desc))"
        print(ctval)

        # execute create table st
        sqlCursor.execute(ctval)
        print("Table created")

        # run staging table check query and check if staging table exists
        stagingfilecheck = pd.read_sql(checkstagingfileexists, sqlCon)

        # if staging table exists
        if (stagingfilecheck.shape[0] > 0):

            # drop the existing staging table
            dropst = "DROP TABLE staging_{0}.{1}".format(sql_schema_name, sql_table_name)
            sqlCursor.execute(dropst)
            print(dropst)

            # copy data from enrich to staging
            dfs.write.synapsesql(stagingtable, Constants.INTERNAL)
        # if staging table doesnot exists copy the data from enrich to staging
        else:
            dfs.write.synapsesql(stagingtable, Constants.INTERNAL)

        print("Copy data from enrich to staging completed")

        # insert new records from staging table to main table and cast to its defined datatypes
        writetotable = "INSERT INTO {0} select distinct {1} FROM {2};".format(dbval, all_col, stagingtable)
        print(writetotable)
        sqlCursor.execute(writetotable)
        print("Copy data from staging to main table completed")

        # print(writetotable)

    checkviewexists = "SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = '" + sql_schema_name + "' AND  TABLE_NAME = '" + sql_view_name + "'"
    print(checkviewexists)
    viewexists = pd.read_sql(checkviewexists, sqlCon)
    # print(viewexists.head())
    # print(viewexists.shape[0])

    if (viewexists.shape[0] > 0):
        print("view exists")
    else:
        # generate create view syntax
        ctval = "CREATE VIEW " + sql_schema_name + "." + sql_view_name + " AS SELECT * FROM " + sql_schema_name + "." + sql_table_name
        print(ctval)
        # execute create table st
        sqlCursor.execute(ctval)
        print("Table created")

    sqlCon.close()





