import os
import pandas as pd
import numpy as np
from datetime import datetime

def transform_to_measurement_vs(**kwargs):
    # concept_id 정의, {key: [measurement_concept_id, measurement_concept_name, unit_concept_id, unit_concept_name]}
    measurement_concept = {
        "height": [4177340, "body height", 8582, "cm"],
        "weight": [4099154, "body weight", 9529, "kg"],
        "bmi": [40490382, "BMI", 9531, "kilogram per square meter"],
        "sbp": [4152194, "systolic blood pressure (SBP)", 4118323, "mmHg"],
        "dbp": [4154790, "diastolic blood pressure (DBP)", 4118323, "mmHg"],
        "pulse": [4224504, "pulse rate (PR)", 4118124, "beats/min"],
        "breth": [4313591, "respiratory rate(RR)", 8541, "respiratory rate(RR)"],
        "bdtp": [4302666, "body temperature", 586323, "degree Celsius"],
        "spo2": [4020553, "SPO2", 8554, "%"]
    }
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
        source = pd.read_csv(os.path.join(source_path, kwargs["source_data1"]), dtype=str, encoding="cp949")
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
    
    # if "visit_detail" in kwargs:
    #     visit_detail = pd.read_csv(os.path.join(CDM_path, kwargs["visit_detail"]), dtype=str)
    # else :
    #     raise ValueError("visit_detail가 없습니다.")
    
    if "person_source_value" in kwargs:
            person_source_value = kwargs["person_source_value"]
    else :
        raise ValueError("person_source_value 없습니다.")
    
    # if "meddept" in kwargs:
    #         meddept = kwargs["meddept"]
    # else :
    #     raise ValueError("meddept 없습니다.")
    
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
    
    if "sbp" in kwargs:
        sbp = kwargs["sbp"]
    else :
        raise ValueError("sbp 없습니다.")

    if "dbp" in kwargs:
        dbp = kwargs["dbp"]
    else :
        raise ValueError("dbp 없습니다.")

    if "pulse" in kwargs:
        pulse = kwargs["pulse"]
    else :
        raise ValueError("pulse 없습니다.")

    if "breth" in kwargs:
        breth = kwargs["breth"]
    else :
        raise ValueError("breth 없습니다.")

    if "bdtp" in kwargs:
        bdtp = kwargs["bdtp"]
    else :
        raise ValueError("bdtp 없습니다.")

    if "spo2" in kwargs:
        spo2 = kwargs["spo2"]
    else :
        raise ValueError("spo2 없습니다.")


    print('원천 데이터 개수:', len(source))

    # 원천에서 조건걸기
    source = source[[person_source_value, admtime, provider, height, weight, sbp, dbp, pulse, breth, bdtp, spo2]]
    source[admtime] = pd.to_datetime(source[admtime], format="%Y%m%d")
    source = source[(source[admtime] <= "2023-08-31")]
    print('조건 적용후 원천 데이터 개수:', len(source))
    
    # CDM 데이터 컬럼 줄이기
    person_data = person_data[["person_id", "person_source_value"]]
    # care_site_data = care_site_data[["care_site_id", "care_site_source_value"]]
    provider_data = provider_data[["provider_id", "provider_source_value"]]
    visit_data = visit_data[["visit_occurrence_id", "visit_start_date", "care_site_id", "visit_source_value", "person_id"]]
    # visit_detail = visit_detail[["visit_detail_id", "visit_occurrence_id"]]

    # person table과 병합
    source = pd.merge(source, person_data, left_on=person_source_value, right_on="person_source_value", how="inner")

    # care_site table과 병합
    # source = pd.merge(source, care_site_data, left_on=meddept, right_on="care_site_source_value", how="left")

    # provider table과 병합
    source = pd.merge(source, provider_data, left_on=provider, right_on="provider_source_value", how="left", suffixes=('', '_y'))


    # visit_occurrence table과 병합
    visit_data = visit_data[visit_data["visit_source_value"] == 'I']
    visit_data["visit_start_date"] = pd.to_datetime(visit_data["visit_start_date"])
    source = pd.merge(source, visit_data, left_on=["person_id", admtime], right_on=["person_id", "visit_start_date"], how="left", suffixes=('', '_y'))

    # visit_detail table과 병합
    # source = pd.merge(source, visit_detail, left_on=["visit_occurrence_id"], right_on=["visit_occurrence_id"], how="left", suffixes=('', '_y'))
    print(len(source))

    # 값이 없는 경우 0으로 값 입력
    source.loc[source["care_site_id"].isna(), "care_site_id"] = 0

    print(source.columns.to_list())

    source_weight = source[source[weight].notna()]
    source_height = source[source[height].notna()]

    source_bmi = source[source[height].notna() | source[weight].notna()]

    source_sbp = source[source[sbp].notna()]
    source_dbp = source[source[dbp].notna()]
    source_pulse = source[source[pulse].notna()]
    source_breth = source[source[breth].notna()]
    source_bdtp = source[source[bdtp].notna()]
    source_spo2 = source[source[spo2].notna()]

    # 숫자가 아닌 value 값 수정
    source_weight["value_as_number"] = source_weight[weight].str.extract(r'(-?\d+\.\d+|\d+)').copy()
    source_weight["value_as_number"].astype(float)

    source_height["value_as_number"] = source_height[height].str.extract(r'(-?\d+\.\d+|\d+)').copy()
    source_height["value_as_number"].astype(float)

    source_bmi[weight] = source_bmi[height].str.extract(r'(-?\d+\.\d+|\d+)').copy()
    source_bmi[weight] = source_bmi[weight].astype(float)
    source_bmi[height] = source_bmi[height].str.extract(r'(-?\d+\.\d+|\d+)').copy()
    source_bmi[height] = source_bmi[height].astype(float)
    source_bmi['bmi'] = round(source_bmi[weight] / (source_bmi[height]*0.01)**2, 1).copy()

    source_sbp["value_as_number"] = source_sbp[sbp].str.extract(r'(-?\d+\.\d+|\d+)').copy()
    source_sbp["value_as_number"].astype(float)

    source_dbp["value_as_number"] = source_dbp[dbp].str.extract(r'(-?\d+\.\d+|\d+)').copy()
    source_dbp["value_as_number"].astype(float)

    source_pulse["value_as_number"] = source_pulse[pulse].str.extract(r'(-?\d+\.\d+|\d+)').copy()
    source_pulse["value_as_number"].astype(float)

    source_breth["value_as_number"] = source_breth[breth].str.extract(r'(-?\d+\.\d+|\d+)').copy()
    source_breth["value_as_number"].astype(float)

    source_bdtp["value_as_number"] = source_bdtp[bdtp].str.extract(r'(-?\d+\.\d+|\d+)').copy()
    source_bdtp["value_as_number"].astype(float)

    source_spo2["value_as_number"] = source_spo2[spo2].str.extract(r'(-?\d+\.\d+|\d+)').copy()
    source_spo2["value_as_number"].astype(float)


    # weight값이 저장된 cdm생성
    cdm_weight = pd.DataFrame({
        "measurement_id": source_weight.index + 1,
        "person_id": source_weight["person_id"],
        "measurement_concept_id": measurement_concept["weight"][0],
        "measurement_date": source_weight[admtime].dt.date,
        "measurement_datetime": source_weight[admtime],
        "measurement_time": source_weight[admtime].dt.time, 
        "measurement_type_concept_id": 44818702,
        "operator_concept_id": 0,
        "value_as_number": source_weight["value_as_number"],
        "value_as_concept_id": 0,
        "unit_concept_id": measurement_concept["weight"][2],
        "range_low": None,
        "range_high": None,
        "provider_id": source_weight["provider_id"],
        "visit_occurrence_id": source_weight["visit_occurrence_id"],
        "visit_detail_id": 0, #source_weight["visit_detail_id"],
        "measurement_source_value": measurement_concept["weight"][1],
        "measurement_source_concept_id": measurement_concept["weight"][0],
        "unit_source_value": measurement_concept["weight"][3],
        "value_source_value": source_weight[weight],
        "vocabulary_id": "EDI"
        })

    # height값이 저장된 cdm생성
    cdm_height = pd.DataFrame({
        "measurement_id": source_height.index + 1,
        "person_id": source_height["person_id"],
        "measurement_concept_id": measurement_concept["height"][0],
        "measurement_date": source_height[admtime].dt.date,
        "measurement_datetime": source_height[admtime],
        "measurement_time": source_height[admtime].dt.time, 
        "measurement_type_concept_id": 44818702,
        "operator_concept_id": 0,
        "value_as_number": source_height["value_as_number"],
        "value_as_concept_id": 0,
        "unit_concept_id": measurement_concept["height"][2],
        "range_low": None,
        "range_high": None,
        "provider_id": source_height["provider_id"],
        "visit_occurrence_id": source_height["visit_occurrence_id"],
        "visit_detail_id": 0, #source_height["visit_detail_id"],
        "measurement_source_value": measurement_concept["height"][1],
        "measurement_source_concept_id": measurement_concept["height"][0],
        "unit_source_value": measurement_concept["height"][3],
        "value_source_value": source_height[height],
        "vocabulary_id": "EDI"
        })

    # bmi값이 저장된 cdm생성
    cdm_bmi = pd.DataFrame({
        "measurement_id": source_bmi.index + 1,
        "person_id": source_bmi["person_id"],
        "measurement_concept_id": measurement_concept["bmi"][0],
        "measurement_date": source_bmi[admtime].dt.date,
        "measurement_datetime": source_bmi[admtime],
        "measurement_time": source_bmi[admtime].dt.time, 
        "measurement_type_concept_id": 44818702,
        "operator_concept_id": 0,
        "value_as_number": source_bmi["bmi"],
        "value_as_concept_id": 0,
        "unit_concept_id": measurement_concept["bmi"][2],
        "range_low": None,
        "range_high": None,
        "provider_id": source_bmi["provider_id"],
        "visit_occurrence_id": source_bmi["visit_occurrence_id"],
        "visit_detail_id": 0, #source_bmi["visit_detail_id"],
        "measurement_source_value": measurement_concept["bmi"][1],
        "measurement_source_concept_id": measurement_concept["bmi"][0],
        "unit_source_value": measurement_concept["bmi"][3],
        "value_source_value": source_bmi["bmi"],
        "vocabulary_id": "EDI"
        })

    cdm_sbp = pd.DataFrame({
        "measurement_id": source_sbp.index + 1,
        "person_id": source_sbp["person_id"],
        "measurement_concept_id": measurement_concept["sbp"][0],
        "measurement_date": source_sbp[admtime].dt.date,
        "measurement_datetime": source_sbp[admtime],
        "measurement_time": source_sbp[admtime].dt.time, 
        "measurement_type_concept_id": 44818702,
        "operator_concept_id": 0,
        "value_as_number": source_sbp["value_as_number"],
        "value_as_concept_id": 0,
        "unit_concept_id": measurement_concept["sbp"][2],
        "range_low": None,
        "range_high": None,
        "provider_id": source_sbp["provider_id"],
        "visit_occurrence_id": source_sbp["visit_occurrence_id"],
        "visit_detail_id": 0, #source_sbp["visit_detail_id"],
        "measurement_source_value": measurement_concept["sbp"][1],
        "measurement_source_concept_id": measurement_concept["sbp"][0],
        "unit_source_value": measurement_concept["sbp"][3],
        "value_source_value": source_sbp[sbp],
        "vocabulary_id": "EDI"
        })
    
    cdm_dbp = pd.DataFrame({
        "measurement_id": source_dbp.index + 1,
        "person_id": source_dbp["person_id"],
        "measurement_concept_id": measurement_concept["dbp"][0],
        "measurement_date": source_dbp[admtime].dt.date,
        "measurement_datetime": source_dbp[admtime],
        "measurement_time": source_dbp[admtime].dt.time, 
        "measurement_type_concept_id": 44818702,
        "operator_concept_id": 0,
        "value_as_number": source_dbp["value_as_number"],
        "value_as_concept_id": 0,
        "unit_concept_id": measurement_concept["dbp"][2],
        "range_low": None,
        "range_high": None,
        "provider_id": source_dbp["provider_id"],
        "visit_occurrence_id": source_dbp["visit_occurrence_id"],
        "visit_detail_id": 0, #source_dbp["visit_detail_id"],
        "measurement_source_value": measurement_concept["dbp"][1],
        "measurement_source_concept_id": measurement_concept["dbp"][0],
        "unit_source_value": measurement_concept["dbp"][3],
        "value_source_value": source_dbp[dbp],
        "vocabulary_id": "EDI"
        })
    
    cdm_pulse = pd.DataFrame({
        "measurement_id": source_pulse.index + 1,
        "person_id": source_pulse["person_id"],
        "measurement_concept_id": measurement_concept["pulse"][0],
        "measurement_date": source_pulse[admtime].dt.date,
        "measurement_datetime": source_pulse[admtime],
        "measurement_time": source_pulse[admtime].dt.time, 
        "measurement_type_concept_id": 44818702,
        "operator_concept_id": 0,
        "value_as_number": source_pulse["value_as_number"],
        "value_as_concept_id": 0,
        "unit_concept_id": measurement_concept["pulse"][2],
        "range_low": None,
        "range_high": None,
        "provider_id": source_pulse["provider_id"],
        "visit_occurrence_id": source_pulse["visit_occurrence_id"],
        "visit_detail_id": 0, #source_pulse["visit_detail_id"],
        "measurement_source_value": measurement_concept["pulse"][1],
        "measurement_source_concept_id": measurement_concept["pulse"][0],
        "unit_source_value": measurement_concept["pulse"][3],
        "value_source_value": source_pulse[pulse],
        "vocabulary_id": "EDI"
        })

    cdm_breth = pd.DataFrame({
        "measurement_id": source_breth.index + 1,
        "person_id": source_breth["person_id"],
        "measurement_concept_id": measurement_concept["breth"][0],
        "measurement_date": source_breth[admtime].dt.date,
        "measurement_datetime": source_breth[admtime],
        "measurement_time": source_breth[admtime].dt.time, 
        "measurement_type_concept_id": 44818702,
        "operator_concept_id": 0,
        "value_as_number": source_breth["value_as_number"],
        "value_as_concept_id": 0,
        "unit_concept_id": measurement_concept["breth"][2],
        "range_low": None,
        "range_high": None,
        "provider_id": source_breth["provider_id"],
        "visit_occurrence_id": source_breth["visit_occurrence_id"],
        "visit_detail_id": 0, #source_breth["visit_detail_id"],
        "measurement_source_value": measurement_concept["breth"][1],
        "measurement_source_concept_id": measurement_concept["breth"][0],
        "unit_source_value": measurement_concept["breth"][3],
        "value_source_value": source_breth[breth],
        "vocabulary_id": "EDI"
        })

    cdm_bdtp = pd.DataFrame({
        "measurement_id": source_bdtp.index + 1,
        "person_id": source_bdtp["person_id"],
        "measurement_concept_id": measurement_concept["bdtp"][0],
        "measurement_date": source_bdtp[admtime].dt.date,
        "measurement_datetime": source_bdtp[admtime],
        "measurement_time": source_bdtp[admtime].dt.time, 
        "measurement_type_concept_id": 44818702,
        "operator_concept_id": 0,
        "value_as_number": source_bdtp["value_as_number"],
        "value_as_concept_id": 0,
        "unit_concept_id": measurement_concept["bdtp"][2],
        "range_low": None,
        "range_high": None,
        "provider_id": source_bdtp["provider_id"],
        "visit_occurrence_id": source_bdtp["visit_occurrence_id"],
        "visit_detail_id": 0, #source_bdtp["visit_detail_id"],
        "measurement_source_value": measurement_concept["bdtp"][1],
        "measurement_source_concept_id": measurement_concept["bdtp"][0],
        "unit_source_value": measurement_concept["bdtp"][3],
        "value_source_value": source_bdtp[bdtp],
        "vocabulary_id": "EDI"
        })

    cdm_spo2 = pd.DataFrame({
        "measurement_id": source_spo2.index + 1,
        "person_id": source_spo2["person_id"],
        "measurement_concept_id": measurement_concept["spo2"][0],
        "measurement_date": source_spo2[admtime].dt.date,
        "measurement_datetime": source_spo2[admtime],
        "measurement_time": source_spo2[admtime].dt.time, 
        "measurement_type_concept_id": 44818702,
        "operator_concept_id": 0,
        "value_as_number": source_spo2["value_as_number"],
        "value_as_concept_id": 0,
        "unit_concept_id": measurement_concept["spo2"][2],
        "range_low": None,
        "range_high": None,
        "provider_id": source_spo2["provider_id"],
        "visit_occurrence_id": source_spo2["visit_occurrence_id"],
        "visit_detail_id": 0, #source_spo2["visit_detail_id"],
        "measurement_source_value": measurement_concept["spo2"][1],
        "measurement_source_concept_id": measurement_concept["spo2"][0],
        "unit_source_value": measurement_concept["spo2"][3],
        "value_source_value": source_spo2[spo2],
        "vocabulary_id": "EDI"
        })

    cdm = pd.concat([cdm_weight, cdm_height, cdm_bmi, cdm_sbp, cdm_dbp, cdm_pulse, cdm_breth, cdm_bdtp, cdm_spo2], axis = 0, ignore_index=True)

    # csv파일로 저장
    cdm.to_csv(os.path.join(CDM_path, "measurement_vs.csv"), index=False)

    print(cdm.shape)
    print('CDM 데이터 개수', len(cdm))
    

start_time = datetime.now()
transform_to_measurement_vs( source_path = "F:\\01.감염병데이터베이스\\data\\knuh\\emr",
                             CDM_path = "F:\\01.감염병데이터베이스\\data\\knuh\\cdm",
                             source_data1 = "01.MNWMPGIF_간호정보조사(일반).csv",
                             care_site_data = "care_site.csv",
                             person_data = "person.csv",
                             provider_data = "provider.csv",
                             visit_data = "visit_occurrence.csv",
                             person_source_value = "PID",
                             provider = "RECRID",
                             admtime = "INDD",
                             height = "BDHT",
                             weight = "BDWT",
                             sbp = "HIGHBP",
                             dbp = "LOWBP",
                             pulse = "PULSE",
                             breth = "BRETH",
                             bdtp = "BDTP",
                             spo2 = "SPO2"
)

print(f"transform and Load end, elapsed_time is : {datetime.now() - start_time}")