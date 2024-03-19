import os
import pandas as pd
import numpy as np
from datetime import datetime

def transform_to_measurement(**kwargs):
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
        source1 = pd.read_csv(os.path.join(source_path, kwargs["source_data1"]), dtype=str)
    else :
        raise ValueError("source_data1(원천 데이터)가 없습니다.")
    
    if "local_edi" in kwargs :
        local_edi = pd.read_csv(os.path.join(CDM_path, kwargs["local_edi"]), dtype=str)
    else :
        raise ValueError("local_edi가 없습니다.")

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

    if "unit_data" in kwargs:
        unit_data = pd.read_csv(os.path.join(CDM_path, kwargs["unit_data"]), dtype=str)
    else :
        raise ValueError("unit_data가 없습니다.")   
    
    if "person_source_value" in kwargs:
            person_source_value = kwargs["person_source_value"]
    else :
        raise ValueError("person_source_value 없습니다.")
    
    if "edicode" in kwargs:
        edicode = kwargs["edicode"]
    else :
        raise ValueError("edicode 없습니다.")
    
    if "ordcode" in kwargs :
        ordcode = kwargs["ordcode"]
    else :
        raise ValueError("ordcode 없습니다.")
    
    if "fromdate" in kwargs :
        fromdate = kwargs["fromdate"]
    else :
        raise ValueError("fromdate 없습니다.")

    if "todate" in kwargs :
        todate = kwargs["todate"]
    else :
        raise ValueError("todate 없습니다.")
    
    if "measurement_date" in kwargs:
        measurement_date = kwargs["measurement_date"]
    else :
        raise ValueError("measurement_date 없습니다.")

    if "measurement_source_value" in kwargs:
        measurement_source_value = kwargs["measurement_source_value"]
    else :
        raise ValueError("measurement_source_value 없습니다.")
    
    if "value_source_value" in kwargs:
        value_source_value = kwargs["value_source_value"]
    else :
        raise ValueError("value_source_value 없습니다.")
    
    if "unit_source_value" in kwargs:
        unit_source_value = kwargs["unit_source_value"]
    else :
        raise ValueError("unit_source_value 없습니다.")
    
    if "result_range" in kwargs:
        result_range = kwargs["result_range"]
    else :
        raise ValueError("result_range 없습니다.")
    
    if "visit_source_key" in kwargs:
        visit_source_key = kwargs["visit_source_key"]
    else :
        raise ValueError("visit_source_key 없습니다.")
    
    
    print('원천 데이터 개수:', len(source1))

    # 원천에서 조건걸기
    source1 = source1[[person_source_value, "병원구분", measurement_date, measurement_source_value, result_range, value_source_value, unit_source_value, visit_source_key]]
    source1[measurement_date] = pd.to_datetime(source1[measurement_date])
    source = source1[(source1[measurement_date] <= "2023-08-31")]
    source = source[source[measurement_date].notna()]

    # 컬럼 정의
    source["value_as_number"] = source[value_source_value].str.extract('(-?\d+\.\d+|\d+)') # value_as_number
    source["range_low"] = source[result_range].apply(lambda x: x.split('~')[0]) # range_low
    source["range_low"] = source["range_low"].str.extract('(-?\d+\.\d+|\d+)')
    source["range_low"] = source["range_low"].astype(float)
    source["range_high"] = source[result_range].apply(lambda x: x.split('~')[1]) # range_high
    source["range_high"] = source["range_high"].str.extract('(-?\d+\.\d+|\d+)')
    source["range_high"] = source["range_high"].astype(float)

    print('조건 적용 후 데이터 개수:', len(source))

    # local_edi 전처리
    local_edi = local_edi[[ordcode, fromdate, todate, edicode, "concept_id", "병원구분코드"]]
    local_edi[fromdate] = pd.to_datetime(local_edi[fromdate] , errors="coerce")
    local_edi[fromdate].fillna(pd.Timestamp('1900-01-01'), inplace = True)
    local_edi[todate] = pd.to_datetime(local_edi[todate] , errors="coerce")
    local_edi[todate].fillna(pd.Timestamp('2099-12-31'), inplace = True)

    # LOCAL코드와 EDI코드 매핑 테이블과 병합
    source = pd.merge(source, local_edi, left_on=[measurement_source_value, "병원구분"], right_on=[ordcode, "병원구분코드"], how="left")
    print('EDI코드 테이블과 병합 후 데이터 개수', len(source))
    source = source[(source[measurement_date] >= source[fromdate]) & (source[measurement_date] <= source[todate])]
    print("EDI코드 사용기간별 필터 적용 후 데이터 개수", len(source))

    # 데이터 컬럼 줄이기
    person_data = person_data[["person_id", "person_source_value"]]
    # care_site_data = care_site_data[["care_site_id", "care_site_source_value"]]
    # provider_data = provider_data[["provider_id", "provider_source_value"]]
    visit_data = visit_data[["visit_occurrence_id", "visit_start_datetime", "person_id", "visit_source_key"]]
    visit_detail = visit_detail[["visit_detail_id", "visit_occurrence_id"]]
    unit_data = unit_data[["concept_id", "concept_code"]]

    # person table과 병합
    source = pd.merge(source, person_data, left_on=person_source_value, right_on="person_source_value", how="inner")
    # care_site table과 병합
    # source = pd.merge(source, care_site_data, left_on=meddept, right_on="care_site_source_value", how="left")
    # provider table과 병합
    # source = pd.merge(source, provider_data, left_on=provider, right_on="provider_source_value", how="left", suffixes=('', '_y'))

    # visit_start_datetime 형태 변경
    # visit_data["visit_start_datetime"] = pd.to_datetime(visit_data["visit_start_datetime"])

    # visit_occurrence table과 병합
    source = pd.merge(source, visit_data, left_on=["person_id", visit_source_key], right_on=["person_id", "visit_source_key"], how="left", suffixes=('', '_y'))

    # visit_detail table과 병합
    source = pd.merge(source, visit_detail, left_on=["visit_occurrence_id"], right_on=["visit_occurrence_id"], how="left", suffixes=('', '_y'))
    print(len(source))

    # concept_unit과 병합
    source = pd.merge(source, unit_data, left_on=unit_source_value, right_on="concept_code", how="left", suffixes=["", "_unit"])
    # 값이 없는 경우 0으로 값 입력
    # source.loc[source["care_site_id"].isna(), "care_site_id"] = 0
    source.loc[source["concept_id"].isna(), "concept_id"] = 0

    print(source.columns.to_list())

    measurement_date_condition = [source[measurement_date].notna()]
    measurement_date_value = [source[measurement_date].dt.date]
    measurement_datetime_value = [source[measurement_date]]
    measurement_time_value = [source[measurement_date].dt.time]

    operator_condition = [
            source[value_source_value].isin([">"])
            , source[value_source_value].isin([">="])
            , source[value_source_value].isin(["="])
            , source[value_source_value].isin(["<="])
            , source[value_source_value].isin(["<"])
    ]
    operator_value = [
            4172704
            , 4171755
            , 4172703
            , 4171754
            , 4171756
    ]

    value_concept_condition = [
        source[value_source_value] == "+"
        , source[value_source_value] == "++"
        , source[value_source_value] == "+++"
        , source[value_source_value] == "++++"
        , source[value_source_value].str.lower() == "negative"
        , source[value_source_value].str.lower() == "positive"
    ]
    value_concept_value = [
        4123508
        , 4126673
        , 4125547
        , 4126674
        , 9189
        , 9191
    ]
    print(source.dtypes.tolist())

    cdm = pd.DataFrame({
        "measurement_id": source.index + 1,
        "person_id": source["person_id"],
        "measurement_concept_id": source["concept_id"],
        "measurement_date": np.select(measurement_date_condition, measurement_date_value, default=source[measurement_date].dt.date),
        "measurement_datetime": np.select(measurement_date_condition, measurement_datetime_value, default=source[measurement_date]),
        "measurement_time": np.select(measurement_date_condition, measurement_time_value, default=source[measurement_date].dt.time),
        "measurement_type_concept_id": 44818702,
        "operator_concept_id": np.select(operator_condition, operator_value, default = 0),
        "value_as_number": source["value_as_number"],
        "value_as_concept_id": np.select(value_concept_condition, value_concept_value, default = 0),
        "unit_concept_id": source["concept_id_unit"],
        "range_low": source["range_low"],
        "range_high": source["range_high"],
        "provider_id": 0,
        "visit_occurrence_id": source["visit_occurrence_id"],
        "visit_detail_id": source["visit_detail_id"],
        "measurement_source_value": source[measurement_source_value],
        "measurement_source_concept_id": source[edicode],
        "unit_source_value": source[unit_source_value],
        "value_source_value": source[value_source_value].str[:50],
        "vocabulary_id": "EDI",
        "visit_source_key": source[visit_source_key]
        })

    cdm["measurement_date"] = pd.to_datetime(cdm["measurement_date"])
    cdm["measurement_datetime"] = pd.to_datetime(cdm["measurement_datetime"])

    # csv파일로 저장
    cdm.to_csv(os.path.join(CDM_path, "measurement.csv"), index=False)

    print(cdm)
    print('CDM 데이터 개수', len(cdm))
    

start_time = datetime.now()
transform_to_measurement(  source_path = "F:\\01.감염병데이터베이스\\data\\dsmc\\emr",
                                    CDM_path = "F:\\01.감염병데이터베이스\\data\\dsmc\\cdm",
                                    source_data1 = "measurement.csv",
                                    person_data = "person.csv",
                                    visit_data = "visit_occurrence.csv",
                                    visit_detail = "visit_detail.csv",
                                    local_edi = "local_edi.csv",
                                    unit_data = "concept_unit.csv",
                                    person_source_value = "환자번호",
                                    ordcode = "처방코드", # local_edi의 처방코드
                                    edicode = "EDI 코드", 
                                    fromdate = "적용시작일", # local_edi의 처방코드 사용 시작일
                                    todate = "적용종료일", # local_edi의 처방코드 사용 종료일
                                    measurement_date = "검사일시",
                                    measurement_source_value = "검사코드",
                                    value_source_value = "검사결과",
                                    unit_source_value = "검사단위",
                                    result_range = "검사결과 참고치",
                                    visit_source_key = "수진번호"
)

print(f"transform and Load end, elapsed_time is : {datetime.now() - start_time}")