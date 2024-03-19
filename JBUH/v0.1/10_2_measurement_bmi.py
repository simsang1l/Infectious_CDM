import os
import pandas as pd
import numpy as np
from datetime import datetime

def transform_to_measurement_bmi(**kwargs):
    # kwargs가 모두 입력됐는지 확인 및 변수 정의
    if "source_path" in kwargs :
        source_path = kwargs["source_path"]
    else :
        raise ValueError("source_path가 없습니다.")

    if "CDM_path" in kwargs:
        CDM_path = kwargs["CDM_path"]
    else :
        raise ValueError("CDM_path가 없습니다.")

    if "source_data1" in kwargs :
        source = pd.read_csv(os.path.join(source_path, kwargs["source_data1"]), dtype=str)
    else :
        raise ValueError("source_data1(원천 데이터)가 없습니다.")
    
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
    
    if "visit_detail" in kwargs:
        visit_detail = pd.read_csv(os.path.join(CDM_path, kwargs["visit_detail"]), dtype=str)
    else :
        raise ValueError("visit_detail가 없습니다.")
    
    if "meddept" in kwargs:
            meddept = kwargs["meddept"]
    else :
        raise ValueError("meddept 없습니다.")
    
    if "provider" in kwargs:
        provider = kwargs["provider"]
    else :
        raise ValueError("provider 없습니다.")
    
    #####
    if "admtime" in kwargs :
        admtime = kwargs["admtime"]
    else :
        raise ValueError("admtime 없습니다.")
    
    if "height" in kwargs :
        height = kwargs["height"]
    else :
        raise ValueError("height 없습니다.")
    
    if "weight" in kwargs:
        weight = kwargs["weight"]
    else :
        raise ValueError("weight 없습니다.")
    
    print('원천 데이터 개수:', len(source))

    # 원천에서 조건걸기
    source = source[["PATNO", admtime, meddept, provider, "HEIGHT", "WEIGHT"]]
    source[admtime] = source[admtime].apply(lambda x : x[:4] + "-" + x[4:6] + "-" + x[6:8] + " " + x[8:10] + ":" + x[10:])
    source[admtime] = pd.to_datetime(source[admtime], errors = "coerce")
    source = source[(source[admtime] <= "2023-08-31")]
    print('조건 적용후 원천 데이터 개수:', len(source))

    source = source[(source["HEIGHT"].notna()) | source["WEIGHT"].notna()]
    source[weight] = source[weight].str.extract(r'(-?\d+\.\d+|\d+)').copy()
    source[height] = source[height].str.extract(r'(-?\d+\.\d+|\d+)').copy()
    source['bmi'] = round(source["WEIGHT"].astype(float) / (source["HEIGHT"].astype(float)*0.01)**2, 1)
    print(source.shape)
    print(f"조건 적용후 원천 데이터 개수: weight: {len(source)}")
    

    # CDM 데이터 컬럼 줄이기
    person_data = person_data[["person_id", "person_source_value"]]
    care_site_data = care_site_data[["care_site_id", "care_site_source_value"]]
    provider_data = provider_data[["provider_id", "provider_source_value"]]
    visit_data = visit_data[["visit_occurrence_id", "visit_start_datetime", "care_site_id", "visit_source_value", "person_id"]]
    visit_detail = visit_detail[["visit_detail_id", "visit_occurrence_id"]]

    # person table과 병합
    source = pd.merge(source, person_data, left_on="PATNO", right_on="person_source_value", how="inner")
    # care_site table과 병합
    source = pd.merge(source, care_site_data, left_on=meddept, right_on="care_site_source_value", how="left")
    # provider table과 병합
    source = pd.merge(source, provider_data, left_on=provider, right_on="provider_source_value", how="left", suffixes=('', '_y'))

    # visit_start_datetime 형태 변경
    visit_data["visit_start_datetime"] = pd.to_datetime(visit_data["visit_start_datetime"])

    # visit_occurrence table과 병합
    visit_data = visit_data[visit_data["visit_source_value"] == 'I']
    visit_data["visit_start_datetime"] = pd.to_datetime(visit_data["visit_start_datetime"])
    source = pd.merge(source, visit_data, left_on=["person_id", "care_site_id", admtime], right_on=["person_id", "care_site_id", "visit_start_datetime"], how="left", suffixes=('', '_y'))

    # visit_detail table과 병합
    source = pd.merge(source, visit_detail, left_on=["visit_occurrence_id"], right_on=["visit_occurrence_id"], how="left", suffixes=('', '_y'))
    print(len(source))

    # 값이 없는 경우 0으로 값 입력
    source.loc[source["care_site_id"].isna(), "care_site_id"] = 0

    print(source.columns.to_list())

    source_weight = source[source[weight].notna()]
    source_weight[weight] = source_weight[weight].str.extract(r'(-?\d+\.\d+|\d+)').copy()
    source_weight[weight] = source_weight[weight].astype(float)

    source_height = source[source[height].notna()]
    source_height[height] = source_height[height].str.extract(r'(-?\d+\.\d+|\d+)').copy()
    source_height[weight] = source_height[height].astype(float)

    source_bmi = source[source["bmi"].notna()]
    source_bmi["bmi"] = source_bmi["bmi"].astype(float)

    # weight값이 저장된 cdm_bmi생성
    cdm_weight = pd.DataFrame({
        "measurement_id": source_weight.index + 1,
        "person_id": source_weight["person_id"],
        "measurement_concept_id": 4099154,
        "measurement_date": source_weight[admtime].dt.date,
        "measurement_datetime": source_weight[admtime],
        "measurement_time": source_weight[admtime].dt.time, 
        "measurement_type_concept_id": 44818702,
        "operator_concept_id": 0,
        "value_as_number": source_weight[weight],
        "value_as_concept_id": 0,
        "unit_concept_id": 9529,
        "range_low": None,
        "range_high": None,
        "provider_id": source_weight["provider_id"],
        "visit_occurrence_id": source_weight["visit_occurrence_id"],
        "visit_detail_id": source_weight["visit_detail_id"],
        "measurement_source_value": "weight",
        "measurement_source_concept_id": 4099154,
        "unit_source_value": "kg",
        "value_source_value": source_weight[weight],
        "vocabulary_id": "EDI"
        })

    # height값이 저장된 cdm_bmi생성
    cdm_height = pd.DataFrame({
        "measurement_id": source_height.index + 1,
        "person_id": source_height["person_id"],
        "measurement_concept_id": 4177340,
        "measurement_date": source_height[admtime].dt.date,
        "measurement_datetime": source_height[admtime],
        "measurement_time": source_height[admtime].dt.time, 
        "measurement_type_concept_id": 44818702,
        "operator_concept_id": 0,
        "value_as_number": source_height[height],
        "value_as_concept_id": 0,
        "unit_concept_id": 8582,
        "range_low": None,
        "range_high": None,
        "provider_id": source_height["provider_id"],
        "visit_occurrence_id": source_height["visit_occurrence_id"],
        "visit_detail_id": source_height["visit_detail_id"],
        "measurement_source_value": "height",
        "measurement_source_concept_id": 4177340,
        "unit_source_value": "cm",
        "value_source_value": source_height[height],
        "vocabulary_id": "EDI"
        })

    # bmi값이 저장된 cdm_bmi생성
    cdm_bmi = pd.DataFrame({
        "measurement_id": source_bmi.index + 1,
        "person_id": source_bmi["person_id"],
        "measurement_concept_id": 40490382,
        "measurement_date": source_bmi[admtime].dt.date,
        "measurement_datetime": source_bmi[admtime],
        "measurement_time": source_bmi[admtime].dt.time, 
        "measurement_type_concept_id": 44818702,
        "operator_concept_id": 0,
        "value_as_number": source_bmi["bmi"],
        "value_as_concept_id": 0,
        "unit_concept_id": 9531,
        "range_low": None,
        "range_high": None,
        "provider_id": source_bmi["provider_id"],
        "visit_occurrence_id": source_bmi["visit_occurrence_id"],
        "visit_detail_id": source_bmi["visit_detail_id"],
        "measurement_source_value": "BMI",
        "measurement_source_concept_id": 40490382,
        "unit_source_value": "kilogram per square meter",
        "value_source_value": source_bmi["bmi"],
        "vocabulary_id": "EDI"
        })

    cdm = pd.concat([cdm_weight, cdm_height, cdm_bmi], axis = 0, ignore_index=True)

    # csv파일로 저장
    cdm.to_csv(os.path.join(CDM_path, "measurement_bmi.csv"), index=False)

    print(cdm.isnull().sum())
    print(cdm.shape)
    print('CDM 데이터 개수', len(cdm))
    

start_time = datetime.now()
transform_to_measurement_bmi( source_path = "F:\\01.감염병데이터베이스\\data\\2023_infectious_DW"
                            , CDM_path = "F:\\01.감염병데이터베이스\\data\\2023_infectious_CDM"
                            , source_data1 = "ods_mnnisemt.csv"
                            , care_site_data = "care_site.csv"
                            , person_data = "person.csv"
                            , provider_data = "provider.csv"
                            , visit_data = "visit_occurrence.csv"
                            , visit_detail = "visit_detail.csv"
                            , meddept = "MEDDEPT"
                            , provider = "REGID"

                            , admtime = "ADMTIME"
                            , height = "HEIGHT"
                            , weight = "WEIGHT"
)

print(f"transform and Load end, elapsed_time is : {datetime.now() - start_time}")