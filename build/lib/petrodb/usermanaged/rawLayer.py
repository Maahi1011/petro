
from petrodb.common.userDefinedFunctions import check_if_file_exists
from petrodb.config.readConfig import get_config_dict
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


def rawlayer(modname, libname, configpath, temppath, filepath, filename, deltalake_root, delta_database_name,
             delta_schema_name, deltaLake_table_name):
    from datetime import datetime
    import importlib
    importlib.import_module(modname)
    mod = importlib.import_module(modname)
    details_dict = get_config_dict(modname, libname, configpath, temppath)

    print("Processing raw layer : " + filename)

    # copying files from incoming path to landing path
    if "dbutils" in modname:

        dbutils = getattr(mod, libname)(spark)
        dbutils.fs.cp(filepath, details_dict.get("DEFAULTS", {}).get("landing_path"), False)

    elif "notebookutils" in modname:

        getattr(mod, libname).fs.cp(filepath, details_dict.get("DEFAULTS", {}).get("landing_path"), False)

    # aligning archine path according to folder structure from busniess metadata
    arch_path = details_dict.get("DEFAULTS", {}).get(
        "archive_path") + '/' + deltalake_root + '/' + delta_database_name + '/' + datetime.today().strftime(
        '%Y') + '/' + datetime.today().strftime('%m') + '/' + datetime.today().strftime('%d') + '/'

    print("copying file " + filename + " to archive path")

    # check if the archive path exists
    if check_if_file_exists(modname, libname, arch_path):
        if "dbutils" in modname:
            dbutils = getattr(mod, libname)(spark)
            dbutils.fs.cp(filepath, arch_path, False)

        elif "notebookutils" in modname:
            getattr(mod, libname).fs.cp(filepath, arch_path, False)


    else:
        if "dbutils" in modname:
            dbutils = getattr(mod, libname)(spark)
            dbutils.fs.mkdirs(arch_path)
            dbutils.fs.cp(filepath, arch_path, False)

        elif "notebookutils" in modname:
            getattr(mod, libname).fs.mkdirs(arch_path)
            getattr(mod, libname).fs.cp(filepath, arch_path, False)
