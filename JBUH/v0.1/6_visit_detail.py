import os
import pandas as pd
import numpy as np
from datetime import datetime

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
    
    if "meddept" in kwargs:
            meddept = kwargs["meddept"]
    else :
        raise ValueError("meddept 없습니다.")
    
    if "provider" in kwargs:
        provider = kwargs["provider"]
    else :
        raise ValueError("provider 없습니다.")
    
    if "admitted_from_source_value" in kwargs :
        admitted_from_source_value = kwargs["admitted_from_source_value"]
    else :
        raise ValueError("admitted_from_source_value 없습니다.")
    
    if "discharged_to_source_value" in kwargs:
        discharged_to_source_value = kwargs["discharged_to_source_value"]
    else :
        raise ValueError("discharged_to_source_value 없습니다.")
    
    if "visit_detail_start_datetime" in kwargs:
        visit_detail_start_datetime = kwargs["visit_detail_start_datetime"]
    else :
        raise ValueError("visit_detail_start_datetime 없습니다.")
    
    if "visit_detail_end_datetime" in kwargs:
        visit_detail_end_datetime = kwargs["visit_detail_end_datetime"]
    else :
        raise ValueError("visit_detail_end_datetime 없습니다.")
    
    
    print('원천 데이터 개수:', len(source))

    # 원천에서 조건걸기
    source = source[source["DELYN"]== "N"]
    # person table과 병합
    source = pd.merge(source, person_data, left_on="PATNO", right_on="person_source_value", how="inner")
    source = source.drop(columns = ["care_site_id", "provider_id"])
    # care_site table과 병합
    source = pd.merge(source, care_site_data, left_on=meddept, right_on="care_site_source_value", how="left")
    # provider table과 병합
    source = pd.merge(source, provider_data, left_on=provider, right_on="provider_source_value", how="left", suffixes=('', '_y'))

    # visit_occurrence테이블에서 I, E에 해당하는 데이터만 추출
    visit_data = visit_data[visit_data["visit_source_value"].isin(["I", "E"])]
    # visit_occurrence table과 병합
    source = pd.merge(source, visit_data, left_on=["person_id", "care_site_id"], right_on=["person_id", "care_site_id"], how="left", suffixes=('', '_y'))
    
    # 컬럼을 datetime형태로 변경
    source[visit_detail_start_datetime] = pd.to_datetime(source[visit_detail_start_datetime], format = "%Y%m%d%H%M%S")
    source["visit_start_datetime"] = pd.to_datetime(source["visit_start_datetime"])
    
    # pandas가 약 2262년 까지만 지원함.., errors="coerce" 하면 에러발생하는 부분은 NaT()처리
    source["visit_end_datetime"] = pd.to_datetime(source["visit_end_datetime"], errors="coerce")
    # 에러 발생하는 부분을 최대값으로 처리
    # 최대 Timestamp 값
    max_timestamp = pd.Timestamp.max

    # NaT 값을 최대 Timestamp 값으로 대체
    # 원본에 256건의 NaT값이 있지만 감안하고 하자.. 12건 늘어남
    source["visit_end_datetime"] = source["visit_end_datetime"].fillna(max_timestamp)

    source = source[(source[visit_detail_start_datetime] >= source["visit_start_datetime"]) & (source[visit_detail_start_datetime] <= source["visit_end_datetime"])]
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
        "provider_id": source["provider_id"],
        "care_site_id": source["care_site_id"],
        "visit_detail_source_value": "ICU",
        "visit_detail_source_concept_id": 0,
        "admitted_from_concept_id": 0,
        "admitted_from_source_value": source[admitted_from_source_value],
        "discharge_to_source_value": source[discharged_to_source_value],
        "discharge_to_concept_id": 0,
        "visit_detail_parent_id": 0,
        "visit_occurrence_id": source["visit_occurrence_id"]
        })

    cdm = cdm[cdm["visit_detail_start_datetime"] <= "2023-08-31"]
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
transform_to_visit_detail( source_path = "F:\\01.감염병데이터베이스\\data\\2023_infectious_DW"
                            , CDM_path = "F:\\01.감염병데이터베이스\\data\\2023_infectious_CDM"
                            , source_data = "ods_mnicupat.csv"
                            , care_site_data = "care_site.csv"
                            , person_data = "person.csv"
                            , provider_data = "provider.csv"
                            , visit_data = "visit_occurrence.csv"
                            , visit_detail_start_datetime = "ENTRTIME"
                            , visit_detail_end_datetime = "OUTRMTIME"
                            , admitted_from_source_value = "ENTRPATH"
                            , discharged_to_source_value = "DSCHSTATE"
                            , meddept = "MEDDEPT"
                            , provider = "CHADR"
)

print(f"transform and Load end, elapsed_time is : {datetime.now() - start_time}")