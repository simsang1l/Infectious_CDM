import os
import pandas as pd
import numpy as np
from datetime import datetime

def transform_to_measurement_diag(**kwargs):
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
        source1 = pd.read_csv(os.path.join(source_path, kwargs["source_data1"]), dtype=str, encoding="cp949")
    else :
        raise ValueError("source_data1(원천 데이터)가 없습니다.")
    
    if "local_edi" in kwargs :
        local_edi = pd.read_csv(os.path.join(CDM_path, kwargs["local_edi"]), dtype=str)
    else :
        raise ValueError("local_edi가 없습니다.")
    
    if "visit_detail" in kwargs :
        visit_detail = pd.read_csv(os.path.join(CDM_path, kwargs["visit_detail"]), dtype=str)
    else :
        raise ValueError("visit_detail 없습니다.")
    
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

    if "unit_data" in kwargs:
        unit_data = pd.read_csv(os.path.join(CDM_path, kwargs["unit_data"]), dtype=str)
    else :
        raise ValueError("unit_data가 없습니다.")   
    
    if "provider" in kwargs:
        provider = kwargs["provider"]
    else :
        raise ValueError("provider 없습니다.")
    
    if "sugacode" in kwargs :
        sugacode = kwargs["sugacode"]
    else :
        raise ValueError("sugacode 없습니다.")
    
    if "edicode" in kwargs :
        edicode = kwargs["edicode"]
    else :
        raise ValueError("edicode 없습니다.")

    if "fromdate" in kwargs :
        fromdate = kwargs["fromdate"]
    else :
        raise ValueError("fromdate 없습니다.")

    if "todate" in kwargs :
        todate = kwargs["todate"]
    else :
        raise ValueError("todate 없습니다.")

    if "person_source_value" in kwargs :
        person_source_value = kwargs["person_source_value"]
    else :
        raise ValueError("person_source_value 없습니다.")
        
    if "orddate" in kwargs :
        orddate = kwargs["orddate"]
    else :
        raise ValueError("orddate 없습니다.")
    
    if "measurement_date" in kwargs :
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

    if "visit_source_key" in kwargs:
        visit_source_key = kwargs["visit_source_key"]
    else :
        raise ValueError("visit_source_key 없습니다.")
    
    if "place_of_service_source_value" in kwargs:
        place_of_service_source_value = kwargs["place_of_service_source_value"]
    else :
        raise ValueError("place_of_service_source_value 없습니다.")
    
    print('원천 데이터 개수:', len(source1))

    # 원천데이터 메모리량 줄이기 위한 조건걸기
    source1 = source1[[place_of_service_source_value, orddate, person_source_value, provider, measurement_date, measurement_source_value, value_source_value, unit_source_value, visit_source_key]]
    source1[orddate] = source1[orddate].str.replace("오전", "AM").str.replace("오후", "PM")
    source1[orddate] = pd.to_datetime(source1[orddate])
    source1[measurement_date] = source1[measurement_date].str.replace("오전", "AM").str.replace("오후", "PM")
    source1[measurement_date] = pd.to_datetime(source1[measurement_date], format='%Y-%m-%d %p %I:%M:%S')
    source1 = source1[(source1[orddate] <= "2023-08-31")]
    source = source1

    print(source.columns.tolist())
    print(source)
    # local_edi 전처리
    local_edi = local_edi[[sugacode, fromdate, todate, edicode, "concept_id", place_of_service_source_value]]
    local_edi[fromdate] = pd.to_datetime(local_edi[fromdate] , format="%Y%m%d", errors="coerce")
    local_edi[fromdate].fillna(pd.Timestamp('1900-01-01'), inplace = True)
    local_edi[todate] = pd.to_datetime(local_edi[todate] , format="%Y%m%d", errors="coerce")
    local_edi[todate].fillna(pd.Timestamp('2099-12-31'), inplace = True)

    # LOCAL코드와 EDI코드 매핑 테이블과 병합
    source = pd.merge(source, local_edi, left_on=[measurement_source_value, place_of_service_source_value], right_on=[sugacode, place_of_service_source_value], how="inner")
    print('EDI코드 테이블과 병합 후 데이터 개수', len(source))
    source = source[(source[orddate] >= source[fromdate]) & (source[orddate] <= source[todate])]
    print("EDI코드 사용기간별 필터 적용 후 데이터 개수", len(source))

    # 데이터 컬럼 줄이기
    person_data = person_data[["person_id", "person_source_value"]]
    provider_data = provider_data[["provider_id", "provider_source_value"]]
    visit_data = visit_data[["visit_occurrence_id", "visit_start_datetime", "care_site_id", "visit_source_value", "person_id", "visit_start_date", "visit_source_key"]]
    unit_data = unit_data[["concept_id", "concept_code"]]

    # person table과 병합
    source = pd.merge(source, person_data, left_on=person_source_value, right_on="person_source_value", how="inner")

    # provider table과 병합
    source = pd.merge(source, provider_data, left_on=provider, right_on="provider_source_value", how="left", suffixes=('', '_y'))

    # visit_occurrence table과 병합
    source = pd.merge(source, visit_data, left_on=["person_id", visit_source_key], right_on=["person_id", "visit_source_key"], how="left", suffixes=('', '_y'))

    # visit_detail table과 병합
    # source = pd.merge(source, visit_detail, left_on=["visit_occurrence_id"], right_on=["visit_occurrence_id"], how="left", suffixes=('', '_y'))
    print(len(source))

    # concept_unit과 병합
    source = pd.merge(source, unit_data, left_on=unit_source_value, right_on="concept_code", how="left", suffixes=["", "_unit"])

    # 값이 없는 경우 0으로 값 입력
    source.loc[source["concept_id"].isna(), "concept_id"] = 0
    
    # value_as_number 컬럼 추가
    source["value_as_number"] = source[value_source_value].str.extract(r'(-?\d+\.\d+|\d+)').copy()
    source["value_as_number"] = source["value_as_number"].astype(float)

    print(measurement_date)
    print(source.columns.to_list())
    source[measurement_date] = pd.to_datetime(source[measurement_date]) #, format="%Y%m%d%H%M%S")

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
        "measurement_date": source[measurement_date].dt.date,
        "measurement_datetime": source[measurement_date],
        "measurement_time": source[measurement_date].dt.time,
        "measurement_type_concept_id": 44818702,
        "operator_concept_id": np.select(operator_condition, operator_value, default = 0),
        "value_as_number": source["value_as_number"],
        "value_as_concept_id": np.select(value_concept_condition, value_concept_value, default = 0),
        "unit_concept_id": 0,
        "range_low": None, #source[range_low],
        "range_high": None, #source[range_high],
        "provider_id": source["provider_id"],
        "visit_occurrence_id": source["visit_occurrence_id"],
        "visit_detail_id": None, #source["visit_detail_id"],
        "measurement_source_value": source[measurement_source_value],
        "measurement_source_concept_id": source[edicode],
        "unit_source_value": None,
        "value_source_value": source[value_source_value].str[:50],
        "vocabulary_id": "EDI",
        "visit_source_key": source[visit_source_key]
        })


    # csv파일로 저장
    cdm.to_csv(os.path.join(CDM_path, "measurement.csv"), index=False)

    print('null 개수:', cdm.isnull().sum())
    print(cdm.shape)
    print('CDM 데이터 개수', len(cdm))
    

start_time = datetime.now()
transform_to_measurement_diag( source_path = "F:\\01.감염병데이터베이스\\data\\cnuh\\emr",
                               CDM_path = "F:\\01.감염병데이터베이스\\data\\cnuh\\cdm",
                               source_data1 = "MSELMAID_진단검사결과.csv",
                               person_data = "person.csv",
                               provider_data = "provider.csv",
                               visit_data = "visit_occurrence.csv",
                               visit_detail = "visit_detail.csv", 
                               local_edi = "local_edi.csv",
                               unit_data = "concept_unit.csv",
                               sugacode = "수가코드", # local_edi의 처방코드
                               edicode = "보험EDI코드", 
                               fromdate = "적용시작일자", # local_edi의 처방코드 사용 시작일
                               todate = "적용종료일자", # local_edi의 처방코드 사용 종료일
                               person_source_value = "환자번호",
                               provider = "검사시작직원번호",
                               orddate = "처방일자",
                               measurement_date = "최종결과검증일시",
                               measurement_source_value = "검사코드",
                               value_source_value = "검사결과내용",
                               unit_source_value = "검사결과단위",
                               visit_source_key = "원무접수ID",
                               place_of_service_source_value = "병원구분코드"
)

print(f"transform and Load end, elapsed_time is : {datetime.now() - start_time}")