import pandas as pd
import os

def merge_procedure(**kwargs):
    cdmpath = kwargs["CDM_path"]
    source1 = pd.read_csv(os.path.join(cdmpath, kwargs["source_data1"]), dtype=str)
    source2 = pd.read_csv(os.path.join(cdmpath, kwargs["source_data2"]), dtype=str)

    print(f"원천1 데이터 개수 : {len(source1)}, 원천2 데이터 개수 :{len(source2)}")

    # axis = 0을 통해 행으로 데이터 합치기, ignore_index = True를 통해 dataframe index재설정
    cdm = pd.concat([source1, source2], axis = 0, ignore_index=True)

    cdm["procedure_id"] = cdm.index + 1
    
    print(cdm.isnull().sum())
    print(f"병합된 cdm 데이터 수 : {len(cdm)}")
    cdm.to_csv(os.path.join(cdmpath, "procedure_occurrence.csv"), index = False)


merge_procedure(CDM_path = "F:\\01.감염병데이터베이스\\data\\2023_infectious_CDM"
                            , source_data1 = "procedure_order_trt.csv"
                            , source_data2 = "procedure_stresult.csv"
)