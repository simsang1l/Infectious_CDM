import pandas as pd
import os 
from glob import glob

data_path = 'F:\\01.감염병데이터베이스\\data\\DW'
save_path = 'F:\\01.감염병데이터베이스\\data\\2023_infectious_DW'
data = {}

for filepath in glob(data_path + '/*'):
    filename = filepath.split('\\')[-1]
    if '_' in filename : 
        pre, name = filename.split('_')[:2]
        final_name = pre + '_' + name
    else :
        final_name = filename

    if final_name in data :
        data[final_name].append(filepath)
    else :
        data[final_name] = [filepath]

for key, value in data.items():
    if "ods_" in key and '.csv' not in key:
        length = 0
        for idx, val in enumerate(value) :
            source = pd.read_csv(val, dtype = str, lineterminator='\n')
            length += len(source)
            # if idx == 0 :
            #     source.to_csv(os.path.join(save_path, key+".csv"), index = False, mode = 'w')
            # else :
            #     source.to_csv(os.path.join(save_path, key+".csv"), index = False, mode = 'a', header=False)
            print(f'{key}: {length}')
            source += source
        source = source.drop_duplicates()
        if idx == 0 :
                source.to_csv(os.path.join(save_path, key+".csv"), index = False, mode = 'w')
        else :
            source.to_csv(os.path.join(save_path, key+".csv"), index = False, mode = 'a', header=False)
            