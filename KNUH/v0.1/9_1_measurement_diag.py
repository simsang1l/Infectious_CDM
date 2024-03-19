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
    
    if "source_data2" in kwargs :
        source2 = pd.read_csv(os.path.join(source_path, kwargs["source_data2"]), dtype=str, encoding="cp949")
    else :
        raise ValueError("source_data2(원천 데이터)가 없습니다.")

    if "source_data3" in kwargs :
        source3 = pd.read_csv(os.path.join(source_path, kwargs["source_data3"]), dtype=str, encoding="cp949")
    else :
        raise ValueError("source_data3(원천 데이터)가 없습니다.")
    
    if "source_data4" in kwargs :
        source4 = pd.read_csv(os.path.join(source_path, kwargs["source_data4"]), dtype=str, encoding="cp949")
    else :
        raise ValueError("source_data4(원천 데이터)가 없습니다.")
    
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

    if "unit_data" in kwargs:
        unit_data = pd.read_csv(os.path.join(CDM_path, kwargs["unit_data"]), dtype=str)
    else :
        raise ValueError("unit_data가 없습니다.")   
    
    if "meddept" in kwargs:
            meddept = kwargs["meddept"]
    else :
        raise ValueError("meddept 없습니다.")
    
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
    
    if "range_low" in kwargs:
        range_low = kwargs["range_low"]
    else :
        raise ValueError("range_low 없습니다.")
    
    if "range_high" in kwargs:
        range_high = kwargs["range_high"]
    else :
        raise ValueError("range_high 없습니다.")
    
    print('원천 데이터 개수:', len(source1), len(source2), len(source3), len(source4))

    # 원천데이터 메모리량 줄이기 위한 조건걸기
    source1 = source1[["INSTCD", orddate, "PID", "PRCPHISTNO", "ORDDD", "CRETNO", "PRCPCLSCD", "LASTUPDTDT", "ORDDRID", "PRCPNM", "PRCPCD", "PRCPHISTCD", "PRCPNO", "ORDDEPTCD"]]
    source1[orddate] = pd.to_datetime(source1[orddate])
    source1["ORDDD"] = pd.to_datetime(source1["ORDDD"])
    source1 = source1[(source1[orddate] <= "2023-08-31")]
    source1 = source1[(source1["PRCPHISTCD"] == "O") & (source1["INSTCD"] == "031") ]
    
    source2 = source2[["INSTCD", orddate, "PRCPNO", "PRCPHISTNO", "EXECPRCPUNIQNO", "ORDDD"]]
    source2[orddate] = pd.to_datetime(source2[orddate])
    source2 = source2[(source2[orddate] <= "2023-08-31")]

    source3 = source3[["INSTCD", orddate, "EXECPRCPUNIQNO", "BCNO", "TCLSCD", "SPCCD", "ORDDD"]]
    source3[orddate] = pd.to_datetime(source3[orddate])
    source3 = source3[(source3[orddate] <= "2023-08-31")]

    source4 = source4[["INSTCD", "BCNO", "TCLSCD", "SPCCD", "RSLTFLAG", measurement_source_value, measurement_date, range_low, range_high, value_source_value, "RSLTSTAT"]]
    source4 = source4[(source4["RSLTFLAG"] == "O") & (source4["RSLTSTAT"].isin(["4", "5"]))]

    print('조건 적용후 원천 데이터 개수:', len(source1), len(source2), len(source3), len(source4), len(source1)+len(source2)+len(source3)+len(source4))
    source = pd.merge(source2, source1, left_on=["INSTCD", orddate, "PRCPNO", "PRCPHISTNO"], right_on=["INSTCD", orddate, "PRCPNO", "PRCPHISTNO"], how="inner", suffixes=("", "_diag1"))
    print('원천 병합 후 데이터 개수:', len(source))
    del source1
    del source2

    # 각 TABLE JOIN 하기
    source = pd.merge(source, source3, left_on=["INSTCD", orddate, "EXECPRCPUNIQNO"], right_on=["INSTCD", orddate, "EXECPRCPUNIQNO"], how="inner", suffixes=("", "_diag3"))
    print('원천 병합 후 데이터 개수:', len(source))
    source = pd.merge(source, source4, left_on=["INSTCD", "BCNO", "TCLSCD", "SPCCD"], right_on=["INSTCD", "BCNO", "TCLSCD", "SPCCD"], how="inner", suffixes=("", "_diag4"))
    print('원천 병합 후 데이터 개수:', len(source))
    del source3
    del source4

    source["value_as_number"] = source[value_source_value].str.extract('(-?\d+\.\d+|\d+)')
    source["value_as_number"].astype(float)
    source[range_low] = source[range_low].str.extract('(-?\d+\.\d+|\d+)')
    source[range_low].astype(float)
    source[range_high] = source[range_high].str.extract('(-?\d+\.\d+|\d+)')
    source[range_high].astype(float)

    print(source)
    print(source.columns.tolist())

    # local_edi 전처리
    local_edi = local_edi[[sugacode, fromdate, todate, edicode, "concept_id", "INSTCD", "SPCCD"]]
    local_edi[fromdate] = pd.to_datetime(local_edi[fromdate] , format="%Y%m%d", errors="coerce")
    local_edi[fromdate].fillna(pd.Timestamp('1900-01-01'), inplace = True)
    local_edi[todate] = pd.to_datetime(local_edi[todate] , format="%Y%m%d", errors="coerce")
    local_edi[todate].fillna(pd.Timestamp('2099-12-31'), inplace = True)

    # LOCAL코드와 EDI코드 매핑 테이블과 병합
    source = pd.merge(source, local_edi, left_on=[measurement_source_value, "INSTCD", "SPCCD"], right_on=[sugacode, "INSTCD", "SPCCD"], how="left")
    print('EDI코드 테이블과 병합 후 데이터 개수', len(source))
    # source = source[(source[orddate] >= source[fromdate]) & (source[orddate] <= source[todate])]
    print("EDI코드 사용기간별 필터 적용 후 데이터 개수", len(source))

    # 데이터 컬럼 줄이기
    person_data = person_data[["person_id", "person_source_value"]]
    care_site_data = care_site_data[["care_site_id", "care_site_source_value", "place_of_service_source_value"]]
    provider_data = provider_data[["provider_id", "provider_source_value"]]
    visit_data = visit_data[["visit_occurrence_id", "visit_start_datetime", "care_site_id", "visit_source_value", "person_id", "visit_start_date"]]
    unit_data = unit_data[["concept_id", "concept_code"]]

    # person table과 병합
    source = pd.merge(source, person_data, left_on="PID", right_on="person_source_value", how="inner")
    # care_site table과 병합
    source = pd.merge(source, care_site_data, left_on=[meddept, "INSTCD"], right_on=["care_site_source_value", "place_of_service_source_value"], how="left")
    # provider table과 병합
    source = pd.merge(source, provider_data, left_on=provider, right_on="provider_source_value", how="left", suffixes=('', '_y'))

    # visit_start_datetime 형태 변경
    # visit_data["visit_start_datetime"] = pd.to_datetime(visit_data["visit_start_datetime"]).dt.strftie("%Y%m%d%H%M%S").str[:12]

    # visit_occurrence table과 병합
    visit_data["visit_start_date"] = pd.to_datetime(visit_data["visit_start_date"])
    source = pd.merge(source, visit_data, left_on=["person_id", "care_site_id", "ORDDD"], right_on=["person_id", "care_site_id", "visit_start_date"], how="left", suffixes=('', '_y'))

    # visit_detail table과 병합
    # source = pd.merge(source, visit_detail, left_on=["visit_occurrence_id"], right_on=["visit_occurrence_id"], how="left", suffixes=('', '_y'))
    print(len(source))

    # concept_unit과 병합
    # source = pd.merge(source, unit_data, left_on=unit_source_value, right_on="concept_code", how="left", suffixes=["", "_unit"])
    # 값이 없는 경우 0으로 값 입력
    source.loc[source["care_site_id"].isna(), "care_site_id"] = 0
    source.loc[source["concept_id"].isna(), "concept_id"] = 0
    
    print(measurement_date)
    print(source.columns.to_list())
    source[measurement_date] = pd.to_datetime(source[measurement_date]) #, format="%Y%m%d%H%M%S")

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
        "measurement_date": source[measurement_date].dt.date,
        "measurement_datetime": source[measurement_date],
        "measurement_time": source[measurement_date].dt.time,
        "measurement_type_concept_id": 44818702,
        "operator_concept_id": np.select(operator_condition, operator_value, default = 0),
        "value_as_number": source["value_as_number"],
        "value_as_concept_id": np.select(value_concept_condition, value_concept_value, default = 0),
        "unit_concept_id": 0,
        "range_low": source[range_low],
        "range_high": source[range_high],
        "provider_id": source["provider_id"],
        "visit_occurrence_id": source["visit_occurrence_id"],
        "visit_detail_id": 0,
        "measurement_source_value": source[measurement_source_value],
        "measurement_source_concept_id": source[edicode],
        "unit_source_value": None,
        "value_source_value": source[value_source_value],
        "vocabulary_id": "EDI"
        })

    # cdm["measurement_date"] = pd.to_datetime(cdm["measurement_date"])
    # cdm["measurement_datetime"] = pd.to_datetime(cdm["measurement_datetime"])

    # csv파일로 저장
    cdm.to_csv(os.path.join(CDM_path, "measurement_diag.csv"), index=False)

    print(cdm.isnull().sum())
    print(cdm.shape)
    print('CDM 데이터 개수', len(cdm))
    

start_time = datetime.now()
transform_to_measurement_diag( source_path = "F:\\01.감염병데이터베이스\\data\\knuh\\emr",
                               CDM_path = "F:\\01.감염병데이터베이스\\data\\knuh\\cdm",
                               source_data1 = "02.MMOHIPRC_검사처방.csv",
                               source_data2 = "28.MMODEXIP_처방상세.csv",
                               source_data3 = "25.LLCHSBGD_진단검사접수.csv",
                               source_data4 = "11.LLRHSPDO_진단검사결과.csv",
                               care_site_data = "care_site.csv",
                               person_data = "person.csv",
                               provider_data = "provider.csv",
                               visit_data = "visit_occurrence.csv",
                               local_edi = "local_edi.csv",
                               unit_data = "concept_unit.csv",
                               sugacode = "CALCSCORCD", # local_edi의 처방코드
                               edicode = "INSUEDICD", 
                               fromdate = "FROMDD", # local_edi의 처방코드 사용 시작일
                               todate = "TODD", # local_edi의 처방코드 사용 종료일
                               meddept = "ORDDEPTCD",
                               provider = "ORDDRID",
                               orddate = "PRCPDD",
                               measurement_date = "LASTREPTDT",
                               measurement_source_value = "TESTCD",
                               value_source_value = "INPTRSLT",
                               range_low = "REFL",
                               range_high = "REFH"
)

print(f"transform and Load end, elapsed_time is : {datetime.now() - start_time}")