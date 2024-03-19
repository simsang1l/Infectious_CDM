import pandas as pd
import os

def merge_measurement(**kwargs):
    cdmpath = kwargs["CDM_path"]
    source1 = pd.read_csv(os.path.join(cdmpath, kwargs["source_data1"]), dtype=str)
    source2 = pd.read_csv(os.path.join(cdmpath, kwargs["source_data2"]), dtype=str)
    source3 = pd.read_csv(os.path.join(cdmpath, kwargs["source_data3"]), dtype=str)

    print(f"원천1 데이터 개수 : {len(source1)}, 원천2 데이터 개수 :{len(source2)}")

    # axis = 0을 통해 행으로 데이터 합치기, ignore_index = True를 통해 dataframe index재설정
    cdm = pd.concat([source1, source2, source3], axis = 0, ignore_index=True)

    cdm["measurement_id"] = cdm.index + 1

    print(f"병합된 cdm 데이터 수 : {len(cdm)}")
    print(cdm.isnull().sum())
    cdm.to_csv(os.path.join(cdmpath, "measurement_.csv"), index = False)


merge_measurement(CDM_path = "F:\\01.감염병데이터베이스\\data\\knuh\\cdm",
                  source_data1 = "measurement_diag.csv",
                  source_data2 = "measurement_pth.csv",
                  source_data3 = "measurement_vs.csv"
)