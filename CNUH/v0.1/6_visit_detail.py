import os
import pandas as pd
import numpy as np
from datetime import datetime

def convert_to_datetime(row):
    try :
        return datetime.strptime(row.replace("오전", "AM").replace("오후", "PM"), "%Y-%m-%d %I:%M:%S %p")
    except ValueError:
        return None

def transform_to_visit_detail(**kwargs):
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
    
    if "care_site_data" in kwargs:
        care_site_data = pd.read_csv(os.path.join(CDM_path, kwargs["care_site_data"]), dtype=str)
    else :
        raise ValueError("care_site_data가 없습니다.")

    if "person_data" in kwargs:
        person_data = pd.read_csv(os.path.join(CDM_path, kwargs["person_data"]), dtype=str)
    else :
        raise ValueError("person_data가 없습니다.")
    
    if "visit_data" in kwargs:
        visit_data = pd.read_csv(os.path.join(CDM_path, kwargs["visit_data"]), dtype=str)
    else :
        raise ValueError("visit_data가 없습니다.")
    
    if "meddept" in kwargs:
            meddept = kwargs["meddept"]
    else :
        raise ValueError("meddept 없습니다.")
    
    if "person_source_value" in kwargs:
        person_source_value = kwargs["person_source_value"]
    else :
        raise ValueError("person_source_value 없습니다.")
    
    if "visit_detail_source_value" in kwargs :
        visit_detail_source_value = kwargs["visit_detail_source_value"]
    else :
        raise ValueError("visit_detail_source_value 없습니다.")
    
    if "visit_detail_start_datetime" in kwargs:
        visit_detail_start_datetime = kwargs["visit_detail_start_datetime"]
    else :
        raise ValueError("visit_detail_start_datetime 없습니다.")
    
    if "visit_detail_end_datetime" in kwargs:
        visit_detail_end_datetime = kwargs["visit_detail_end_datetime"]
    else :
        raise ValueError("visit_detail_end_datetime 없습니다.")
    
    if "visit_source_key" in kwargs:
        visit_source_key = kwargs["visit_source_key"]
    else :
        raise ValueError("visit_source_key 없습니다.")
    
    print('원천 데이터 개수:', len(source))

    # 원천에서 조건걸기
    source[visit_detail_start_datetime] = source[visit_detail_start_datetime].str.replace("오전", "AM").str.replace("오후", "PM")
    source[visit_detail_start_datetime] = pd.to_datetime(source[visit_detail_start_datetime])
    source = source[source[visit_detail_start_datetime] <= "2023-08-31"]

    # person table과 병합
    source = pd.merge(source, person_data, left_on=person_source_value, right_on="person_source_value", how="inner")
    source = source.drop(columns = ["care_site_id", "provider_id"])
    # care_site table과 병합
    source = pd.merge(source, care_site_data, left_on=[meddept, "병원구분코드"], right_on=["care_site_source_value", "place_of_service_source_value"], how="left")
    # provider table과 병합
    # source = pd.merge(source, provider_data, left_on=provider, right_on="provider_source_value", how="left", suffixes=('', '_y'))

    # 컬럼을 datetime형태로 변경
    visit_data["visit_start_datetime"] = pd.to_datetime(visit_data["visit_start_datetime"])
    source["입원일자"]  = pd.to_datetime(source["입원일자"])

    # visit_occurrence table과 병합
    source = pd.merge(source, visit_data, left_on=["person_id", "입원일자", visit_source_key], right_on=["person_id", "visit_start_datetime", "visit_source_key"], how="left", suffixes=('', '_y'))
    # source = source[(source[visit_detail_start_datetime] >= source["visit_start_datetime"]) & (source[visit_detail_start_datetime] <= source["visit_end_datetime"])]
    print(len(source))
    source.loc[source["care_site_id"].isna(), "care_site_id"] = 0
    print(source.columns.to_list())

    source[visit_detail_end_datetime] = pd.to_datetime(source[visit_detail_end_datetime], errors="coerce")
    cdm = pd.DataFrame({
        "person_id": source["person_id"],
        "visit_detail_concept_id": 32037,
        "visit_detail_start_date": source[visit_detail_start_datetime].dt.date,
        "visit_detail_start_datetime": source[visit_detail_start_datetime],
        "visit_detail_end_date": source[visit_detail_end_datetime].dt.date,
        "visit_detail_end_datetime": pd.to_datetime(source[visit_detail_end_datetime], errors="coerce"),
        "visit_detail_type_concept_id": 44818518,
        "provider_id": 0, #source["provider_id"],
        "care_site_id": source["care_site_id"],
        "visit_detail_source_value": "ICU",
        "visit_detail_source_concept_id": 0,
        "admitted_from_concept_id": 0,
        "admitted_from_source_value": None,
        "discharge_to_source_value": None,
        "discharge_to_concept_id": 0,
        "visit_detail_parent_id": 0,
        "visit_occurrence_id": source["visit_occurrence_id"]
        })

    # 컬럼 생성
    cdm.reset_index(drop = True, inplace = True)
    cdm["visit_detail_id"] = cdm.index + 1 
    cdm.sort_values(by=["person_id", "visit_detail_start_datetime"], inplace = True)
    cdm["preceding_visit_detail_id"] = cdm.groupby("person_id")["visit_detail_id"].shift(1)

    columns = ["visit_detail_id", "person_id", "visit_detail_concept_id", "visit_detail_start_date"
               , "visit_detail_start_datetime", "visit_detail_end_date", "visit_detail_end_datetime"
               , "visit_detail_type_concept_id", "provider_id", "care_site_id", "visit_detail_source_value"
               , "visit_detail_source_concept_id", "admitted_from_concept_id", "admitted_from_source_value"
               , "discharge_to_source_value", "discharge_to_concept_id", "preceding_visit_detail_id"
               , "visit_detail_parent_id", "visit_occurrence_id"]
    cdm = cdm[columns]

    
    cdm.to_csv(os.path.join(CDM_path, "visit_detail.csv"), index=False, float_format = '%.0f')

    print(cdm.shape)
    print('CDM 데이터 개수', len(cdm))
    

start_time = datetime.now()
transform_to_visit_detail( source_path = "F:\\01.감염병데이터베이스\\data\\cnuh\\emr",
                           CDM_path = "F:\\01.감염병데이터베이스\\data\\cnuh\\cdm",
                           source_data = "MOMNMIOD_중환자실환자정보.csv",
                           care_site_data = "care_site.csv",
                           person_data = "person.csv",
                           visit_data = "visit_occurrence.csv",
                           person_source_value = "등록번호",
                           visit_detail_start_datetime = "입실일시",
                           visit_detail_end_datetime = "퇴실일시",
                           visit_detail_source_value = "입실병동부서코드",
                           meddept = "진료부서코드",
                           visit_source_key = "원무접수ID"
)

print(f"transform and Load end, elapsed_time is : {datetime.now() - start_time}")