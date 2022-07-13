from configparser import ConfigParser

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
# print("readConfig file")
# def get_config_dict(config_path):
#     config = configparser.RawConfigParser()
#     config.read(config_path)
#
#     # details_dict = dict(config.items('DEFAULT'))
#     return dict(config.items('DEFAULT'))
#
#     # print(details_dict.get("bus_metadata_path"))

# def get_config_dict(config_path, temp_path):
#     dbutils.fs.cp(config_path, temp_path)
#     config = ConfigParser()
#     config.read("/"+temp_path.split("///")[1])
#     print ("Sections : ", config.sections())
#     config_dict = {}
#     config_dict = {sect: dict(config.items(sect)) for sect in config.sections()}
#     print()
#     return config_dict
def get_config_dict(modname, libname, config_path, temp_path):
    config_dict = {}
    if "dbutils" in modname:
        import importlib
        importlib.import_module(modname)
        mod = importlib.import_module(modname)
        dbutils = getattr(mod, libname)(spark)
        dbutils.fs.cp(config_path, temp_path)
        config = ConfigParser()
        config.read("/" + temp_path.split("///")[1])
        print("Sections : ", config.sections())
        config_dict = {sect: dict(config.items(sect)) for sect in config.sections()}

    elif modname == "notebookutils":

        import importlib
        importlib.import_module(modname)
        mod = importlib.import_module(modname)
        getattr(mod, libname).fs.cp(config_path, temp_path)
        config = ConfigParser()
        config.read("/" + temp_path.split("///")[1])
        print("Sections : ", config.sections())
        config_dict = {sect: dict(config.items(sect)) for sect in config.sections()}

    else:
        config = ConfigParser()
        config.read(config_path)
        print("Sections : ", config.sections())
        config_dict = {sect: dict(config.items(sect)) for sect in config.sections()}

    return config_dict



