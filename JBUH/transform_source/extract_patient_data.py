import pandas as pd
import yaml
import os

def load_config(config_path):
    with open(config_path, 'r', encoding="utf-8") as file:
        return yaml.safe_load(file)
    
config_path = './transform_source/extract_patient_data_config.yaml'

config = load_config(config_path)
input_path = config["input_path"]
output_path = config["output_path"]
input_encoding = config["input_encoding"]
output_encoding = config["output_encoding"]
patient_filename = config["patient_filename"]
table_list = config["table_list"]


patient = pd.read_excel(os.path.join(output_path, patient_filename), header = 1)
patient["환자번호"] = patient["환자번호"].astype(str).str.zfill(8)

for table in table_list:
    df = pd.read_csv(os.path.join(input_path, table + '.csv'), dtype = str, encoding=input_encoding)

    if table == "person":
        df = df[df["person_source_value"].isin(patient["환자번호"])]
        print(df.shape)
        df.to_csv(os.path.join(output_path, table + '.csv'), encoding=output_encoding, index = False)
        patient = patient.merge(df, left_on="환자번호", right_on="person_source_value", how = "inner")
    else :
        df = df[df["person_id"].isin(patient["person_id"])]
        print(df.shape)
        df.to_csv(os.path.join(output_path, table + '.csv'), encoding=output_encoding, index = False)