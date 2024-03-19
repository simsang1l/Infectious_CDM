import os
import pandas as pd
import numpy as np
from datetime import datetime

def convert_to_datetime(row):
    try :
        return datetime.strptime(row.replace("오전", "AM").replace("오후", "PM"), "%Y-%m-%d %I:%M:%S %p")
    except ValueError:
        return None

def transform_to_condition_occurrence(**kwargs):
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
        source = pd.read_csv(os.path.join(source_path, kwargs["source_data"]), dtype=str, encoding = 'cp949')
    else :
        raise ValueError("source_data(원천 데이터)가 없습니다.")
    
    if "provider_data" in kwargs:
        provider_data = pd.read_csv(os.path.join(CDM_path, kwargs["provider_data"]), dtype=str)
    else :
        raise ValueError("provider_data가 없습니다.")

    if "person_data" in kwargs:
        person_data = pd.read_csv(os.path.join(CDM_path, kwargs["person_data"]), dtype=str)
    else :
        raise ValueError("person_data가 없습니다.")
    
    if "visit_data" in kwargs:
        visit_data = pd.read_csv(os.path.join(CDM_path, kwargs["visit_data"]), dtype=str)
    else :
        raise ValueError("visit_data가 없습니다.")
    
    if "visit_detail" in kwargs:
        visit_detail = pd.read_csv(os.path.join(CDM_path, kwargs["visit_detail"]), dtype=str)
    else :
        raise ValueError("visit_detail가 없습니다.")
    
    if "person_source_value" in kwargs:
            person_source_value = kwargs["person_source_value"]
    else :
        raise ValueError("person_source_value 없습니다.")
    
    if "provider" in kwargs:
        provider = kwargs["provider"]
    else :
        raise ValueError("provider 없습니다.")
    
    if "condition_start_datetime" in kwargs :
        condition_start_datetime = kwargs["condition_start_datetime"]
    else :
        raise ValueError("condition_start_datetime 없습니다.")
    
    if "condition_end_datetime" in kwargs :
        condition_end_datetime = kwargs["condition_end_datetime"]
    else :
        raise ValueError("condition_end_datetime 없습니다.")

    if "condition_source_value" in kwargs:
        condition_source_value = kwargs["condition_source_value"]
    else :
        raise ValueError("condition_source_value 없습니다.")

    if "visit_source_key" in kwargs:
        visit_source_key = kwargs["visit_source_key"]
    else :
        raise ValueError("visit_source_key 없습니다.")

    print('원천 데이터 개수:', len(source))

    # 컬럼 줄이기
    source = source[[person_source_value, condition_start_datetime, condition_end_datetime, condition_source_value, visit_source_key, provider, "최종변경일시"]]
    # 원천에서 조건걸기
    source[condition_start_datetime] = source[condition_start_datetime].str.strip().str.replace("오전", "AM").str.replace("오후", "PM")
    source[condition_start_datetime] = pd.to_datetime(source[condition_start_datetime])

    source["최종변경일시"] = source["최종변경일시"].str.replace("오전", "AM").str.replace("오후", "PM")
    source[condition_end_datetime] = source[condition_end_datetime].str.replace("오전", "AM").str.replace("오후", "PM")
    source[condition_end_datetime] = source[condition_end_datetime].apply(convert_to_datetime)
    source = source[source[condition_start_datetime] <= "2023-08-31"]
    
    print('조건 적용후 원천 데이터 개수:', len(source))

    # 컬럼 줄이기
    person_data = person_data[["person_id", "person_source_value"]]
    provider_data = provider_data[["provider_id", "provider_source_value"]]
    visit_data = visit_data[["visit_occurrence_id", "visit_start_datetime", "care_site_id", "visit_source_value", "person_id", "visit_start_date", "visit_source_key"]]

    # person table과 병합
    source = pd.merge(source, person_data, left_on=person_source_value, right_on="person_source_value", how="inner")
    
    # provider table과 병합
    source = pd.merge(source, provider_data, left_on=provider, right_on="provider_source_value", how="left", suffixes=('', '_y'))

    # visit_occurrence table과 병합
    source = pd.merge(source, visit_data, left_on=["person_id", visit_source_key], right_on=["person_id", "visit_source_key"], how="left", suffixes=('', '_y'))

    # visit_detail table과 병합
    # source = pd.merge(source, visit_detail, left_on=["visit_occurrence_id"], right_on=["visit_occurrence_id"], how="left", suffixes=('', '_y'))

    source["condition_end_date"] = source[condition_end_datetime].apply(lambda x : str(x.year) + '-' + str(x.month).zfill(2) + '-' + str(x.day))
    print(source.columns.to_list())

    cdm = pd.DataFrame({
        "condition_occurrence_id": source.index + 1,
        "person_id": source["person_id"],
        "condition_concept_id": 0,
        "condition_start_date": source[condition_start_datetime].dt.date,
        "condition_start_datetime": source[condition_start_datetime],
        "condition_end_date": source["condition_end_date"],
        "condition_end_datetime": source[condition_end_datetime],
        "condition_type_concept_id": 0,
        "condition_status_concept_id": 0,
        "stop_reason": None,
        "provider_id": source["provider_id"],
        "visit_occurrence_id": source["visit_occurrence_id"],
        "visit_detail_id": source["visit_detail_id"],
        "condition_source_value": source[condition_source_value],
        "condition_source_concept_id": 0,
        "condition_status_source_value": None
        })

    # csv파일로 저장
    cdm.to_csv(os.path.join(CDM_path, "condition_occurrence.csv"), index=False)

    print(cdm.shape)
    print('CDM 데이터 개수', len(cdm))
    

start_time = datetime.now()
transform_to_condition_occurrence( source_path = "F:\\01.감염병데이터베이스\\data\\cnuh\\emr",
                                   CDM_path = "F:\\01.감염병데이터베이스\\data\\cnuh\\cdm",
                                   source_data = "MOODIPAM_환자진단기본.csv",
                                   person_data = "person.csv",
                                   provider_data = "provider.csv",
                                   visit_data = "visit_occurrence.csv",
                                   visit_detail = "visit_detail.csv",
                                   person_source_value = "환자번호",
                                   provider = "최초진단직원번호",
                                   condition_start_datetime = "진단시작일자",
                                   condition_end_datetime = "진단종료일자",
                                   condition_source_value = "ICD10코드",
                                   visit_source_key = "진단ID"
)

print(f"transform and Load end, elapsed_time is : {datetime.now() - start_time}")