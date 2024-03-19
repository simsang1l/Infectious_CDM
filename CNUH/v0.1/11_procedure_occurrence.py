import os
import pandas as pd
import numpy as np
from datetime import datetime

# ODS_처치/재료/수술료/마취료처방
def transform_to_procedure_occurrence(**kwargs):
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
    
    if "sugacode" in kwargs:
        sugacode = kwargs["sugacode"]
    else :
        raise ValueError("sugacode 없습니다.")

    if "edicode" in kwargs:
        edicode = kwargs["edicode"]
    else :
        raise ValueError("edicode 없습니다.")

    if "fromdate" in kwargs:
        fromdate = kwargs["fromdate"]
    else :
        raise ValueError("fromdate 없습니다.")

    if "todate" in kwargs:
        todate = kwargs["todate"]
    else :
        raise ValueError("todate 없습니다.")
    
    if "provider" in kwargs :
        provider = kwargs["provider"]
    else :
        raise ValueError("provider 없습니다.")

    if "procedure_date" in kwargs :
        procedure_date = kwargs["procedure_date"]
    else :
        raise ValueError("procedure_date 없습니다.")
    
    if "procedure_source_value" in kwargs:
        procedure_source_value = kwargs["procedure_source_value"]
    else :
        raise ValueError("procedure_source_value 없습니다.")
    
    if "visit_source_key" in kwargs:
        visit_source_key = kwargs["visit_source_key"]
    else :
        raise ValueError("visit_source_key 없습니다.")

    if "place_of_service_source_value" in kwargs:
        place_of_service_source_value = kwargs["place_of_service_source_value"]
    else :
        raise ValueError("place_of_service_source_value 없습니다.")

    print('원천 데이터 개수:', len(source1))

    # 원천에서 조건걸기
    source1[procedure_date] = pd.to_datetime(source1[procedure_date])
    source1 = source1[(source1[procedure_date] <= "2023-08-31")]
    source = source1[[person_source_value, procedure_date, procedure_source_value, place_of_service_source_value, provider, visit_source_key]]
    print(f"조건적용 후 원천 데이터 수: {len(source)}")

    local_edi = local_edi[[sugacode, fromdate, todate, edicode, "concept_id", place_of_service_source_value]]
    local_edi[fromdate] = pd.to_datetime(local_edi[fromdate], errors="coerce")
    local_edi[fromdate].fillna(pd.Timestamp('1900-01-01'), inplace = True)
    local_edi[todate] = pd.to_datetime(local_edi[todate], errors="coerce")
    local_edi[todate].fillna(pd.Timestamp('2099-12-31'), inplace = True)

    # LOCAL코드와 EDI코드 매핑 테이블과 병합
    source = pd.merge(source, local_edi, left_on=[procedure_source_value, place_of_service_source_value], right_on=[sugacode, place_of_service_source_value], how="inner")
    print('EDI코드 테이블과 병합 후 데이터 개수', len(source))
    # source = source[(source[orddate] >= source[fromdate]) & (source[orddate] <= source[todate])]
    print("EDI코드 사용기간별 필터 적용 후 데이터 개수", len(source))

    # person table과 병합
    source = pd.merge(source, person_data, left_on=person_source_value, right_on="person_source_value", how="inner")
    source = source.drop(columns = ["care_site_id", "provider_id"])

    # provider table과 병합
    source = pd.merge(source, provider_data, left_on=provider, right_on="provider_source_value", how="left", suffixes=('', '_y'))
 
    # visit_start_datetime 형태 변경
    visit_data["visit_start_date"] = pd.to_datetime(visit_data["visit_start_date"])

    # visit_occurrence table과 병합
    source = pd.merge(source, visit_data, left_on=["person_id", visit_source_key], right_on=["person_id", "visit_source_key"], how="left", suffixes=('', '_y'))

    # visit_detail table과 병합
    # source = pd.merge(source, visit_detail, left_on=["visit_occurrence_id"], right_on=["visit_occurrence_id"], how="left", suffixes=('', '_y'))
    print(len(source))

    # 값이 없는 경우 0으로 값 입력
    source.loc[source["concept_id"].isna(), "concept_id"] = 0

    print(source.columns.to_list())
                            
    cdm = pd.DataFrame({
        "procedure_occurrence_id": source.index + 1,
        "person_id": source["person_id"],
        "procedure_concept_id": source["concept_id"],
        "procedure_date": source[procedure_date].dt.date,
        "procedure_datetime": source[procedure_date],
        "procedure_type_concept_id": 38000275,
        "modifier_concept_id": 0,
        "quantity": None,
        "provider_id": source["provider_id"],
        "visit_occurrence_id": source["visit_occurrence_id"],
        "visit_detail_id": None, #source["visit_detail_id"],
        "procedure_source_value": source[procedure_source_value],
        "procedure_source_concept_id": source[edicode],
        "modifier_source_value": None ,
        "vocabulary_id": "EDI",
        "visit_source_key": source[visit_source_key]
        })

    cdm["procedure_date"] = pd.to_datetime(cdm["procedure_date"])
    cdm["procedure_datetime"] = pd.to_datetime(cdm["procedure_datetime"])

    # csv파일로 저장
    cdm.to_csv(os.path.join(CDM_path, "procedure_occurrence.csv"), index=False)

    print(cdm.shape)
    print('CDM 데이터 개수', len(cdm))
    

start_time = datetime.now()
transform_to_procedure_occurrence( source_path = "F:\\01.감염병데이터베이스\\data\\cnuh\\emr",
                                  CDM_path = "F:\\01.감염병데이터베이스\\data\\cnuh\\cdm",
                                  source_data1 = "MOOORPTD_처방기본.csv",
                                  person_data = "person.csv",
                                  provider_data = "provider.csv",
                                  visit_data = "visit_occurrence.csv",
                                  visit_detail = "visit_detail.csv",
                                  local_edi = "local_edi.csv",
                                  person_source_value = "환자번호",
                                  sugacode = "수가코드", # local_edi의 처방코드
                                  edicode = "보험EDI코드", 
                                  fromdate = "적용시작일자", # local_edi의 처방코드 사용 시작일
                                  todate = "적용종료일자", # local_edi의 처방코드 사용 종료일
                                  provider = "주치의직원번호",
                                  procedure_date = "처방일자",
                                  procedure_source_value = "처방코드",
                                  visit_source_key = "진료원무접수ID",
                                  place_of_service_source_value = "병원구분코드"
)

print(f"transform and Load end, elapsed_time is : {datetime.now() - start_time}")