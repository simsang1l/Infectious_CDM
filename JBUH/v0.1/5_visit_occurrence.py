import os
import pandas as pd
import numpy as np
from datetime import datetime
  
def transform_to_visit_occurrence(**kwargs):
    # kwargs가 모두 입력됐는지 확인 및 변수 정의
    if "source_path" in kwargs :
        source_path = kwargs["source_path"]
    else :
        raise ValueError("source_path가 없습니다.")

    if "CDM_path" in kwargs:
        CDM_path = kwargs["CDM_path"]
    else :
        raise ValueError("CDM_path가 없습니다.")

    if "source_data" in kwargs :
        source = pd.read_csv(os.path.join(source_path, kwargs["source_data"]), dtype=str)
    else :
        raise ValueError("source_data(원천 데이터)가 없습니다.")

    if "source_data2" in kwargs :
        source2 = pd.read_csv(os.path.join(source_path, kwargs["source_data2"]), dtype=str)
    else :
        raise ValueError("source_data2(원천 데이터)가 없습니다.")
    
    if "care_site_data" in kwargs:
        care_site_data = pd.read_csv(os.path.join(CDM_path, kwargs["care_site_data"]), dtype=str)
    else :
        raise ValueError("care_site_data가 없습니다.")
    
    if "provider_data" in kwargs:
        provider_data = pd.read_csv(os.path.join(CDM_path, kwargs["provider_data"]), dtype=str)
    else :
        raise ValueError("provider_data가 없습니다.")

    if "person_data" in kwargs:
        person_data = pd.read_csv(os.path.join(CDM_path, kwargs["person_data"]), dtype=str)
    else :
        raise ValueError("person_data가 없습니다.")
    
    if "admitting_from_source_value" in kwargs :
        admitting_from_source_value = kwargs["admitting_from_source_value"]
    else :
        raise ValueError("admitting_from_source_value 없습니다.")
    
    if "discharge_to_source_value" in kwargs:
        discharge_to_source_value = kwargs["discharge_to_source_value"]
    else :
        raise ValueError("discharge_to_source_value 없습니다.")
    
    if "medtime" in kwargs:
        medtime = kwargs["medtime"]
    else :
        raise ValueError("medtime 없습니다")    
    
    if "admtime" in kwargs:
        admtime = kwargs["admtime"]
    else :
        raise ValueError("admtime 없습니다.")
    
    if "dschtime" in kwargs:
        dschtime = kwargs["dschtime"]
    else :
        raise ValueError("dschtime 없습니다.")
    
    if "meddept" in kwargs:
        meddept = kwargs["meddept"]
    else :
        raise ValueError("meddept 없습니다.")
    
    if "visit_source_value" in kwargs:
        visit_source_value = kwargs["visit_source_value"]
    else :
        raise ValueError("visit_source_value 없습니다.")
    
    print('원천 데이터 개수:', len(source), len(source2))
    print('원천 데이터 개수:', len(source) + len(source2))

    ## 외래데이터 변환하기
    # person table과 병합
    source = pd.merge(source, person_data, left_on="PATNO", right_on="person_source_value", how="inner")
    source = source.drop(columns = ["care_site_id", "provider_id"])
    source = pd.merge(source, care_site_data, left_on=meddept, right_on="care_site_source_value", how="left")
    source = pd.merge(source, provider_data, left_on="MEDDR", right_on="provider_source_value", how="left", suffixes=('', '_y'))
    source.loc[source["care_site_id"].isna(), "care_site_id"] = 0
    # 201903081045같은 데이터가 2019-03-08 10:04:05로 바뀌는 문제 발견 
    source[medtime] = source[medtime].apply(lambda x : x[:4] + "-" + x[4:6] + "-" + x[6:8] + " " + x[8:10] + ":" + x[10:])
    source[medtime] = pd.to_datetime(source[medtime], errors = "coerce")

    cdm_o = pd.DataFrame({
        "person_id": source["person_id"],
        "visit_concept_id": 9202,
        "visit_start_date": source[medtime].dt.date,
        "visit_start_datetime": source[medtime],
        "visit_end_date": source[medtime].dt.date,
        "visit_end_datetime": source[medtime],
        "visit_type_concept_id": np.select([source[meddept] == "CTC"], [44818519], default = 44818518),
        "provider_id": source["provider_id"],
        "care_site_id": source["care_site_id"],
        "visit_source_value": "O",
        "visit_source_concept_id": 9202,
        "admitted_from_concept_id": 0,
        "admitted_from_source_value": None,
        "discharge_to_concept_id": 0,
        "discharge_to_source_value": None
        })


    ## 입원 응급 데이터 변환하기
    ## 외래데이터 변환하기
    # person table과 병합
    source2 = pd.merge(source2, person_data, left_on="PATNO", right_on="person_source_value", how="inner")
    source2 = source2.drop(columns = ["care_site_id", "provider_id"])
    source2 = pd.merge(source2, care_site_data, left_on=meddept, right_on="care_site_source_value", how="left")
    source2 = pd.merge(source2, provider_data, left_on="CHADR", right_on="provider_source_value", how="left", suffixes=('', '_y'))
    source2[admtime] = source2[admtime].apply(lambda x : x[:4] + "-" + x[4:6] + "-" + x[6:8] + " " + x[8:10] + ":" + x[10:])
    print(source2[admtime])
    source2[admtime] = pd.to_datetime(source2[admtime], errors = "coerce")
    print(source2[admtime])

    visit_condition = [
        source2["PATFG"] == "I",
        source2["PATFG"] == "E",
    ]
    visit_concept_id = [9201, 9203]

    admit_condition = [
        source2[admitting_from_source_value].isin(["1", "6"]),
        source2[admitting_from_source_value].isin(["3"]),
        source2[admitting_from_source_value].isin(["7"]),
        source2[admitting_from_source_value].isin(["9"])
        ]
    admit_concept_id = [8765, 8892, 8870, 8844]

    discharge_condition = [
        source2[discharge_to_source_value].isin(["1"]),
        source2[discharge_to_source_value].isin(["2"]),
        source2[discharge_to_source_value].isin(["3"]),
        source2[discharge_to_source_value].isin(["8"]),
        source2[discharge_to_source_value].isin(["9"])
        ]
    discharge_concept_id = [44790567, 4061268, 8536, 44814693, 8844]

    cdm_ie = pd.DataFrame({
        "person_id": source2["person_id"],
        "visit_concept_id": np.select(visit_condition, visit_concept_id, default = 0),
        "visit_start_date": source2[admtime].dt.date ,
        "visit_start_datetime": source2[admtime],
        "visit_end_date": source2[dschtime].apply(lambda x: datetime.strptime(x, '%Y%m%d%H%M%S').strftime('%Y-%m-%d')),
        "visit_end_datetime": source2[dschtime].apply(lambda x: datetime.strptime(x, '%Y%m%d%H%M%S')), #pd.to_datetime(source2[dschtime], format="%Y%m%d%H%M%S"),
        "visit_type_concept_id": np.select([source2[meddept] == "CTC"], [44818519], default = 44818518),
        "provider_id": source2["provider_id"],
        "care_site_id": source2["care_site_id"],
        "visit_source_value": source2[visit_source_value],
        "visit_source_concept_id": np.select(visit_condition, visit_concept_id, default = 0),
        "admitted_from_concept_id": np.select(admit_condition, admit_concept_id, default = 0),
        "admitted_from_source_value": source2[admitting_from_source_value],
        "discharge_to_concept_id": np.select(discharge_condition, discharge_concept_id, default = 0),
        "discharge_to_source_value": source2[discharge_to_source_value]
        })
    cdm = pd.concat([cdm_o, cdm_ie], axis = 0)
    cdm = cdm[pd.to_datetime(cdm["visit_start_date"]) <= "2023-08-31"]
    
    cdm.reset_index(drop=True, inplace = True)
    cdm["visit_occurrence_id"] = cdm.index + 1
    cdm.sort_values(by=["person_id", "visit_start_datetime"], inplace = True)
    cdm["preceding_visit_occurrence_id"] = cdm.groupby("person_id")["visit_occurrence_id"].shift(1)

    columns = ["visit_occurrence_id", "person_id", "visit_concept_id", "visit_start_date", "visit_start_datetime"
               , "visit_end_date", "visit_end_datetime", "visit_type_concept_id", "provider_id"
               , "care_site_id", "visit_source_value", "visit_source_concept_id", "admitted_from_concept_id"
               , "admitted_from_source_value", "discharge_to_source_value", "discharge_to_concept_id"
               , "preceding_visit_occurrence_id"]
    cdm = cdm[columns]

    print(cdm.isnull().sum())
    print(len(cdm_o), len(cdm_ie), len(cdm_o)+len(cdm_ie))
    print(cdm.shape)
    cdm.to_csv(os.path.join(CDM_path, "visit_occurrence.csv"), index=False, float_format = '%.0f')

    print('CDM 데이터 개수', len(cdm_o), len(cdm_ie))
    print('CDM 데이터 개수', len(cdm))
    

start_time = datetime.now()
transform_to_visit_occurrence( source_path = "F:\\01.감염병데이터베이스\\data\\2023_infectious_DW"
                            , CDM_path = "F:\\01.감염병데이터베이스\\data\\2023_infectious_CDM"
                            , source_data = "ods_apopdlst.csv"
                            , source_data2 = "ods_apipdlst.csv"
                            , care_site_data = "care_site.csv"
                            , person_data = "person.csv"
                            , provider_data = "provider.csv"
                            , admitting_from_source_value = "ADMPATH"
                            , discharge_to_source_value = "DSCHRSLT"
                            , medtime = "MEDTIME"
                            , admtime = "ADMTIME"
                            , dschtime = "DSCHTIME"
                            , meddept = "MEDDEPT"
                            , visit_source_value = "PATFG"
)

print(f"transform and Load end, elapsed_time is : {datetime.now() - start_time}")