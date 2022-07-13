import pyspark.sql.functions as F
from pyspark.sql.functions import *
from mstest.common.userDefinedFunctions import *
from mstest.config.readConfig import *

spark = SparkSession.builder.appName("petro.com").getOrCreate()


def conformlayer(modname, libname, configpath, filepath, filename, deltalake_root, delta_database_name,
                 delta_schema_name, deltalake_table_name):
    import importlib
    from datetime import datetime
    importlib.import_module(modname)
    mod = importlib.import_module(modname)
    details_dict = get_config_dict(configpath)
    print('Process conform layer for : ' + filename)

    # read landing file to dataframe
    df = spark.read.option("header", True).csv(filepath)

    print("read csv file to df")

    # aligning the tech metadata path & table/entity for the file

    manifestpath = str(details_dict.get("tech_mf_path")).strip() + '/' + deltalake_root + '/' + delta_database_name + '/' + delta_schema_name + '/' + deltalake_table_name
    entity = deltalake_table_name
    readDf = (spark.read.format("com.microsoft.cdm")
              .option("storage", str(details_dict.get("storeageDevAccountName")).strip())
              .option("manifestPath", str(details_dict.get("metadata_ct")).strip() + manifestpath + "/default.manifest.cdm.json")
              .option("entity", entity)
              .load())

    print("metadata loaded")

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
    dateval = pdf.select(partitioncol).distinct().collect()
    disdatecol_list = [i.pdate for i in dateval]
    print(disdatecol_list)

    # defining the conform path from busniess metadata
    conform_delta_path = str(details_dict.get("conformed_path")).strip() + deltalake_root + '/' + delta_database_name + '/' + delta_schema_name + '/' + deltalake_table_name

    # defining the backup paath from busniess metadata
    backup_delta_path = str(details_dict.get("backup_path")).strip() + deltalake_root + '/' + delta_database_name + '/' + delta_schema_name + '/' + deltalake_table_name + '_' + datetime.today().strftime(
        '%Y%m%d%H%M%S')

    # check if conform delta path exists for the processing file
    if check_if_file_exists(modname, libname, conform_delta_path):

        # reading the existing conform delta file
        edf = spark.read.format("delta").load(conform_delta_path)
        print("count of existing delta table : " + str(edf.count()))

        # temp view for existing delta file
        edf.filter(F.col(partitioncol).isin(disdatecol_list)).registerTempTable("old_oil_mobility_apple")

        print("count of new file : " + str(pdf.count()))

        # temp view for the new records in the file in landing path
        pdf.registerTempTable("new_oil_mobility_apple")

        # comparing the existing data with new data and fetching the records which are not present in the existing delta table
        newdf = spark.sql("Select distinct n.* from new_oil_mobility_apple n where n.calhashkey not in " +
                          "(select calhashkey from old_oil_mobility_apple)")

        print("count of new records : " + str(newdf.count()))

        # if no records found then exit else append to the existing data
        if (newdf.count() != 0):
            # check if backup schema path exists for the processing file schema
            if check_if_file_exists(modname, libname,
                    str(details_dict.get("backup_path")).strip() + deltalake_root + '/' + delta_database_name + '/' + delta_schema_name + '/'):
                bkpfiles = getattr(mod, libname).fs.ls(
                    str(details_dict.get("backup_path")).strip() + deltalake_root + '/' + delta_database_name + '/' + delta_schema_name + '/')

                # checking the file name in the backup folder
                for f in bkpfiles:

                    # if the file name exists in the backup folder delete the existing one and create a new file
                    if deltalake_table_name in f.name:
                        getattr(mod, libname).fs.rm(f.path, True)
                        print('conformed_path: ' + conform_delta_path)
                        print('backup_path: ' + backup_delta_path)
                        getattr(mod, libname).fs.cp(conform_delta_path, backup_delta_path, True)

                    # if the file does not exists in backup folder then copy the data form conform to backup
                    else:
                        getattr(mod, libname).fs.cp(conform_delta_path, backup_delta_path, True)

            # if the backup schema path does not exists then create a backup path
            else:
                print('backup_path: ' + backup_delta_path)
                getattr(mod, libname).fs.cp(conform_delta_path, backup_delta_path, True)

            # df.join(edf, df.hashrow!=edf.hashrow,'inner').select(df["*"]).show(10)
            print("new records found")
            newdf.write.format("delta").mode("append").partitionBy(partitioncol).option("mergeSchema", "true").save(
                conform_delta_path)



    else:

        print("New file loading to conform layer with the count : " + str(df.count()))

        # write the data to new conform delta path if its a delta table does not exists
        pdf.write.format("delta").option("mergeSchema", "true").partitionBy(partitioncol).save(conform_delta_path)
