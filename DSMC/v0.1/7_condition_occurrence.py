import os
import pandas as pd
import numpy as np
from datetime import datetime

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
        source = pd.read_csv(os.path.join(source_path, kwargs["source_data"]), dtype=str)
    else :
        raise ValueError("source_data(원천 데이터)가 없습니다.")

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
    
    #####
    if "condition_start_datetime" in kwargs :
        condition_start_datetime = kwargs["condition_start_datetime"]
    else :
        raise ValueError("condition_start_datetime 없습니다.")
    
    if "condition_type" in kwargs:
        condition_type = kwargs["condition_type"]
    else :
        raise ValueError("condition_type 없습니다.")
    
    if "condition_source_value" in kwargs:
        condition_source_value = kwargs["condition_source_value"]
    else :
        raise ValueError("condition_source_value 없습니다.")
    
    if "condition_status_source_value" in kwargs:
        condition_status_source_value = kwargs["condition_status_source_value"]
    else :
        raise ValueError("condition_status_source_value 없습니다.")
    
    if "visit_source_key" in kwargs:
        visit_source_key = kwargs["visit_source_key"]
    else :
        raise ValueError("visit_source_key 없습니다.")
   
    print('원천 데이터 개수:', len(source))

    # 원천에서 조건걸기
    source[condition_start_datetime] = pd.to_datetime(source[condition_start_datetime])
    source = source[source[condition_start_datetime] <= "2023-08-31"]
    print('조건 적용후 원천 데이터 개수:', len(source))

    # person table과 병합
    source = pd.merge(source, person_data, left_on=person_source_value, right_on="person_source_value", how="inner")
    source = source.drop(columns = ["care_site_id", "provider_id"])
    print(source.values.tolist())
    # care_site table과 병합
    # source = pd.merge(source, care_site_data, left_on=meddept, right_on="care_site_source_value", how="left")
    # provider table과 병합
    # source = pd.merge(source, provider_data, left_on=provider, right_on="provider_source_value", how="left", suffixes=('', '_y'))

    # visit_start_datetime 형태 변경
    visit_data["visit_start_datetime"] = pd.to_datetime(visit_data["visit_start_datetime"]).dt.strftime("%Y%m%d%H%M%S").str[:12]

    # visit_occurrence table과 병합
    source = pd.merge(source, visit_data, left_on=["person_id"], right_on=["person_id"], how="left", suffixes=('', '_y'))

    # visit_detail table과 병합
    source = pd.merge(source, visit_detail, left_on=["visit_occurrence_id"], right_on=["visit_occurrence_id"], how="left", suffixes=('', '_y'))
    
    print(source.columns.to_list())

    type_condition = [
        source[condition_type] == "Y",
        source[condition_type] == "N"
    ]
    type_id = [44786627, 44786629]

    status_condition = [
        source[condition_status_source_value] == "Y",
        source[condition_status_source_value] == "N"
    ]
    status_id = [4230359, 4033240] 

    cdm = pd.DataFrame({
        "condition_occurrence_id": source.index + 1,
        "person_id": source["person_id"],
        "condition_concept_id": 0,
        "condition_start_date": source[condition_start_datetime].dt.date,
        "condition_start_datetime": source[condition_start_datetime],
        "condition_end_date": source["visit_end_date"],
        "condition_end_datetime": source["visit_end_datetime"],
        "condition_type_concept_id": np.select(type_condition, type_id, default = 0),
        "condition_status_concept_id": np.select(status_condition, status_id, default = 0),
        "stop_reason": None,
        "provider_id": None,
        "visit_occurrence_id": source["visit_occurrence_id"],
        "visit_detail_id": source["visit_detail_id"],
        "condition_source_value": source[condition_source_value],
        "condition_source_concept_id": 0,
        "condition_status_source_value": source[condition_status_source_value],
        "visit_source_key": source[visit_source_key]
        })

    # datetime format 형식 맞춰주기, ns로 표기하는 값이 들어갈 수 있어서 처리함
    # cdm["condition_end_date"] = pd.to_datetime(cdm["condition_end_date"])
    # cdm["condition_end_datetime"] = pd.to_datetime(cdm["condition_end_datetime"])

    # csv파일로 저장
    cdm.to_csv(os.path.join(CDM_path, "condition_occurrence.csv"), index=False)
    
    print(cdm)
    print('CDM 데이터 개수', len(cdm))
    

start_time = datetime.now()
transform_to_condition_occurrence(  source_path = "F:\\01.감염병데이터베이스\\data\\dsmc\\emr",
                                    CDM_path = "F:\\01.감염병데이터베이스\\data\\dsmc\\cdm",
                                    source_data = "condition_occurrence.csv",
                                    person_data = "person.csv",
                                    visit_data = "visit_occurrence.csv",
                                    visit_detail = "visit_detail.csv",
                                    person_source_value = "환자번호", 
                                    condition_start_datetime = "진단일",
                                    condition_type = "주상병여부",
                                    condition_source_value = "진단코드",
                                    condition_status_source_value = "확정상병여부",
                                    visit_source_key = "수진번호"                       
)

print(f"transform and Load end, elapsed_time is : {datetime.now() - start_time}")