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

    if "person_source_value" in kwargs:
        person_source_value = kwargs["person_source_value"]
    else :
        raise ValueError("person_source_value 없습니다")

    if "visit_start_datetime" in kwargs:
        visit_start_datetime = kwargs["visit_start_datetime"]
    else :
        raise ValueError("visit_start_datetime 없습니다")    
    
    if "visit_end_datetime" in kwargs:
        visit_end_datetime = kwargs["visit_end_datetime"]
    else :
        raise ValueError("visit_end_datetime 없습니다.")
    
    if "visit_source_value" in kwargs:
        visit_source_value = kwargs["visit_source_value"]
    else :
        raise ValueError("visit_source_value 없습니다.")

    if "visit_source_key" in kwargs:
        visit_source_key = kwargs["visit_source_key"]
    else :
        raise ValueError("visit_source_key 없습니다.")
    
    print('원천 데이터 개수:', len(source))

    source = source[source[visit_start_datetime] <= "2023-08-31"]
    print('조건 적용 후 원천 데이터 개수:', len(source))

    ## 외래데이터 변환하기
    # person table과 병합
    source = pd.merge(source, person_data, left_on=person_source_value, right_on="person_source_value", how="inner")
    source = source.drop(columns = ["care_site_id", "provider_id"])
    # source = pd.merge(source, care_site_data, left_on=meddept, right_on="care_site_source_value", how="left")
    # source = pd.merge(source, provider_data, left_on="MEDDR", right_on="provider_source_value", how="left", suffixes=('', '_y'))

    # source.loc[source["care_site_id"].isna(), "care_site_id"] = 0

    visit_condition = [
        source[visit_source_value] == "외래",
        source[visit_source_value] == "입원",
        source[visit_source_value] == "응급"
    ]
    visit_concept_id = [9202, 9201, 9203]

    cdm = pd.DataFrame({
        "person_id": source["person_id"],
        "visit_concept_id": np.select(visit_condition, visit_concept_id, default = 0),
        "visit_start_date": pd.to_datetime(source[visit_start_datetime]).dt.date,
        "visit_start_datetime": pd.to_datetime(source[visit_start_datetime]),
        "visit_end_date": pd.to_datetime(source[visit_end_datetime]).dt.date,
        "visit_end_datetime": pd.to_datetime(source[visit_end_datetime]),
        "visit_type_concept_id": 44818518,
        "provider_id": 0 ,
        "care_site_id": 0,
        "visit_source_value": source[visit_source_value],
        "visit_source_concept_id": np.select(visit_condition, visit_concept_id, default = 0),
        "admitted_from_concept_id": 0,
        "admitted_from_source_value": source[admitting_from_source_value],
        "discharge_to_concept_id": 0,
        "discharge_to_source_value": source[discharge_to_source_value],
        "visit_source_key": source[visit_source_key]
        })
    cdm.reset_index(drop=True, inplace = True)
    cdm["visit_occurrence_id"] = cdm.index + 1
    cdm.sort_values(by=["person_id", "visit_start_datetime"], inplace = True)
    cdm["preceding_visit_occurrence_id"] = cdm.groupby("person_id")["visit_occurrence_id"].shift(1)

    columns = ["visit_occurrence_id", "person_id", "visit_concept_id", "visit_start_date", "visit_start_datetime"
               , "visit_end_date", "visit_end_datetime", "visit_type_concept_id", "provider_id"
               , "care_site_id", "visit_source_value", "visit_source_concept_id", "admitted_from_concept_id"
               , "admitted_from_source_value", "discharge_to_concept_id", "discharge_to_source_value"
               , "preceding_visit_occurrence_id", "visit_source_key"]
    cdm = cdm[columns]
    
    cdm.to_csv(os.path.join(CDM_path, "visit_occurrence.csv"), index=False, float_format = '%.0f')

    print('CDM 데이터 개수', len(cdm))
    

start_time = datetime.now()
transform_to_visit_occurrence( source_path = "F:\\01.감염병데이터베이스\\data\\dsmc\\emr",
                               CDM_path = "F:\\01.감염병데이터베이스\\data\\dsmc\\cdm",
                               source_data = "visit_occurrence.csv",
                               person_data = "person.csv",
                               person_source_value = "환자번호",
                               admitting_from_source_value = "입원경로",
                               discharge_to_source_value = "퇴원형태",
                               visit_start_datetime = "내원시작일시",
                               visit_end_datetime = "내원종료일시",
                               visit_source_value = "방문유형",
                               visit_source_key = "수진번호"
)

print(f"transform and Load end, elapsed_time is : {datetime.now() - start_time}")