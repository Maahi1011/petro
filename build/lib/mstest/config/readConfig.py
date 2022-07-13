import pandas as pd

def init():
    global detailsdict
    detailsdict = {}

# print("readConfig file")
# def get_config_dict(config_path):
#     config = configparser.RawConfigParser()
#     config.read(config_path)
#
#     # details_dict = dict(config.items('DEFAULT'))
#     return dict(config.items('DEFAULT'))
#
#     # print(details_dict.get("bus_metadata_path"))

def get_config_dict(config_path):
    # df = pd.read_csv(config_path, sep="\s+=\s+", engine='python')
    df = pd.read_json(config_path, orient='records')
    print(df.head())
    detailsdict = dict(zip(df.key, df.value))
    return detailsdict
