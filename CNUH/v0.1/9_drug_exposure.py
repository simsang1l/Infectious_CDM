import os
import pandas as pd
import numpy as np
from datetime import datetime

def transform_to_drug_exposure(**kwargs):
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
        source = pd.read_csv(os.path.join(source_path, kwargs["source_data"]), dtype=str, encoding='cp949')
    else :
        raise ValueError("source_data(원천 데이터)가 없습니다.")
    
    if "local_edi" in kwargs :
        local_edi = pd.read_csv(os.path.join(CDM_path, kwargs["local_edi"]), dtype=str)
    else :
        raise ValueError("local_edi가 없습니다.")
    
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
    
    if "edicode" in kwargs:
        edicode = kwargs["edicode"]
    else :
        raise ValueError("edicode 없습니다.")

    if "drugcd" in kwargs:
        drugcd = kwargs["drugcd"]
    else :
        raise ValueError("drugcd 없습니다.")

    if "fromdate" in kwargs:
        fromdate = kwargs["fromdate"]
    else :
        raise ValueError("fromdate 없습니다.")

    if "todate" in kwargs:
        todate = kwargs["todate"]
    else :
        raise ValueError("todate 없습니다.")

    if "person_source_value" in kwargs:
            person_source_value = kwargs["person_source_value"]
    else :
        raise ValueError("person_source_value 없습니다.")
    
    if "meddept" in kwargs:
            meddept = kwargs["meddept"]
    else :
        raise ValueError("meddept 없습니다.")
    
    if "provider" in kwargs:
        provider = kwargs["provider"]
    else :
        raise ValueError("provider 없습니다.")
    
    #####
    if "drug_exposure_start_datetime" in kwargs :
        drug_exposure_start_datetime = kwargs["drug_exposure_start_datetime"]
    else :
        raise ValueError("drug_exposure_start_datetime 없습니다.")
    
    if "drug_source_value" in kwargs :
        drug_source_value = kwargs["drug_source_value"]
    else :
        raise ValueError("drug_source_value 없습니다.")
    
    if "days_supply" in kwargs:
        days_supply = kwargs["days_supply"]
    else :
        raise ValueError("days_supply 없습니다.")
    
    if "qty" in kwargs:
        qty = kwargs["qty"]
    else :
        raise ValueError("qty 없습니다.")
    
    if "cnt" in kwargs:
        cnt = kwargs["cnt"]
    else :
        raise ValueError("cnt 없습니다.")
    
    if "route_source_value" in kwargs:
        route_source_value = kwargs["route_source_value"]
    else :
        raise ValueError("route_source_value 없습니다.")

    if "dose_unit_source_value" in kwargs:
        dose_unit_source_value = kwargs["dose_unit_source_value"]
    else :
        raise ValueError("dose_unit_source_value 없습니다.")
    
    if "visit_source_key" in kwargs:
        visit_source_key = kwargs["visit_source_key"]
    else :
        raise ValueError("visit_source_key 없습니다.")
    
    
    print('원천 데이터 개수:', len(source))

    # 컬럼 줄이기
    source = source[[person_source_value, drug_source_value, drug_exposure_start_datetime, meddept, provider, days_supply, qty, cnt, dose_unit_source_value, "병원구분코드", route_source_value, visit_source_key]]

    # 원천에서 조건걸기
    source[drug_exposure_start_datetime] = source[drug_exposure_start_datetime].str.replace("오전", "AM").str.replace("오후", "PM")
    source[drug_exposure_start_datetime] = pd.to_datetime(source[drug_exposure_start_datetime])
    source = source[source[drug_exposure_start_datetime] <= "2023-08-31"]
    print('조건 적용후 원천 데이터 개수:', len(source))

    local_edi = local_edi[[drugcd, fromdate, todate, edicode, "concept_id"]]
    local_edi[fromdate] = pd.to_datetime(local_edi[fromdate] , format="%Y%m%d", errors="coerce")
    local_edi[fromdate].fillna(pd.Timestamp('1900-01-01'), inplace = True)
    local_edi[todate] = pd.to_datetime(local_edi[todate] , format="%Y%m%d", errors="coerce")
    local_edi[todate].fillna(pd.Timestamp('2099-12-31'), inplace = True)

    # LOCAL코드와 EDI코드 매핑 테이블과 병합
    source = pd.merge(source, local_edi, left_on=drug_source_value, right_on=drugcd, how="inner")
    print(len(source))
    source = source[(source[drug_exposure_start_datetime] >= source[fromdate]) & (source[drug_exposure_start_datetime] <= source[todate])]
    print(len(source))

    # 컬럼 줄이기
    person_data = person_data[["person_id", "person_source_value"]]
    care_site_data = care_site_data[["care_site_id", "care_site_source_value", "place_of_service_source_value"]]
    provider_data = provider_data[["provider_id", "provider_source_value"]]
    visit_data = visit_data[["visit_occurrence_id", "visit_start_datetime", "care_site_id", "visit_source_value", "person_id", "visit_start_date", "visit_source_key"]]

    # person table과 병합
    source = pd.merge(source, person_data, left_on=person_source_value, right_on="person_source_value", how="inner")
    
    # care_site table과 병합
    source = pd.merge(source, care_site_data, left_on=[meddept, "병원구분코드"], right_on=["care_site_source_value", "place_of_service_source_value"], how="left")
    # provider table과 병합
    source = pd.merge(source, provider_data, left_on=provider, right_on="provider_source_value", how="left", suffixes=('', '_y'))

    # visit_occurrence table과 병합
    source = pd.merge(source, visit_data, left_on=["person_id", visit_source_key], right_on=["person_id", "visit_source_key"], how="left", suffixes=('', '_y'))

    # visit_detail table과 병합
    source = pd.merge(source, visit_detail, left_on=["visit_occurrence_id"], right_on=["visit_occurrence_id"], how="left", suffixes=('', '_y'))
    # print(len(source))
    # care_site_id가 없는 경우 0으로 값 입력
    source.loc[source["care_site_id"].isna(), "care_site_id"] = 0
    source.loc[source["concept_id"].isna(), "concept_id"] = 0

    print(source.columns.to_list())

    cdm = pd.DataFrame({
        "drug_exposure_id": source.index + 1,
        "person_id": source["person_id"],
        "drug_concept_id": source["concept_id"],
        "drug_exposure_start_date": source[drug_exposure_start_datetime].dt.date,
        "drug_exposure_start_datetime": source[drug_exposure_start_datetime],
        "drug_exposure_end_date": source[drug_exposure_start_datetime].dt.date + pd.to_timedelta(source[days_supply].astype(int) + 1, unit = "D"),
        "drug_exposure_end_datetime": source[drug_exposure_start_datetime] + pd.to_timedelta(source[days_supply].astype(int) + 1, unit = "D"),
        "verbatim_end_date": None,
        "drug_type_concept_id": 38000177,
        "stop_reason": None,
        "refills": 0,
        "quantity": source[days_supply].astype(int) * source[qty].astype(float) * source[cnt].astype(float),
        "days_supply": source[days_supply].astype(int),
        "sig": None,
        "route_concept_id": 0,
        "lot_number": None,
        "provider_id": source["provider_id"],
        "visit_occurrence_id": source["visit_occurrence_id"],
        "visit_detail_id": source["visit_detail_id"],
        "drug_source_value": source[drug_source_value],
        "drug_source_concept_id": source[edicode],
        "route_source_value": source[route_source_value],
        "dose_unit_source_value": source[dose_unit_source_value],
        "vocabulary_id": "EDI",
        "visit_source_key": source[visit_source_key]
        })

    # csv파일로 저장
    cdm.to_csv(os.path.join(CDM_path, "drug_exposure.csv"), index=False)

    print(cdm.shape)
    print('CDM 데이터 개수', len(cdm))
    

start_time = datetime.now()
transform_to_drug_exposure( source_path = "F:\\01.감염병데이터베이스\\data\\cnuh\\emr",
                            CDM_path = "F:\\01.감염병데이터베이스\\data\\cnuh\\cdm",
                            source_data = "MOOORDRM_약처방.csv",
                            care_site_data = "care_site.csv",
                            person_data = "person.csv",
                            provider_data = "provider.csv",
                            visit_data = "visit_occurrence.csv",
                            visit_detail = "visit_detail.csv",
                            local_edi = "local_edi.csv",
                            drugcd = "수가코드",
                            fromdate = "적용시작일자",
                            todate = "적용종료일자",
                            edicode = "보험EDI코드",
                            person_source_value = "환자번호", 
                            meddept = "발행처부서코드",
                            provider = "주치의직원번호",
                            drug_exposure_start_datetime = "처방일자",
                            drug_source_value = "처방코드",
                            days_supply = "전체기간일수",
                            qty = "기간별횟수",
                            cnt = "투여수량",
                            route_source_value = "투여경로구분코드",
                            dose_unit_source_value = "약품단위구분코드",
                            visit_source_key = "원무접수ID"
)

print(f"transform and Load end, elapsed_time is : {datetime.now() - start_time}")