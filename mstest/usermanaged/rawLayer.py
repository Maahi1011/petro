from mstest.common.userDefinedFunctions import check_if_file_exists
from mstest.config.readConfig import *

# copy file from incoming to archive path
def rawlayer(modname,libname,configpath,filepath, filename, deltalake_root, delta_database_name, delta_schema_name, deltaLake_table_name):
    import importlib
    from datetime import datetime
    importlib.import_module(modname)
    mod = importlib.import_module(modname)
    details_dict = get_config_dict(configpath)
    print("Processing raw layer : " + filename)

    # copying files from incoming path to landing path
    getattr(mod, libname).fs.cp(filepath, details_dict.get("landing_path"), False)

    # aligning archive path according to folder structure from busniess metadata
    arch_path = details_dict.get("archive_path") + '/' + deltalake_root + '/' + delta_database_name + '/' + datetime.today().strftime(
        '%Y') + '/' + datetime.today().strftime('%m') + '/' + datetime.today().strftime('%d') + '/'
    print("copying file " + filename + " to archive path")

    # check if the archive path exists
    if check_if_file_exists(modname,libname,arch_path):
        getattr(mod, libname).fs.cp(filepath, arch_path, False)
    else:
        getattr(mod, libname).fs.mkdirs(arch_path)
        getattr(mod, libname).fs.cp(filepath, arch_path, False)
