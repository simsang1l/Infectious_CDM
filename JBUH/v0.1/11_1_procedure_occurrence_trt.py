import os
import pandas as pd
import numpy as np
from datetime import datetime

# ODS_처치/재료/수술료/마취료처방
def transform_to_procedure_order_trt(**kwargs):
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
    if "orddate" in kwargs :
        orddate = kwargs["orddate"]
    else :
        raise ValueError("orddate 없습니다.")
    
    if "exectime" in kwargs :
        exectime = kwargs["exectime"]
    else :
        raise ValueError("exectime 없습니다.")
    
    
    if "opdate" in kwargs:
        opdate = kwargs["opdate"]
    else :
        raise ValueError("opdate 없습니다.")
    
    if "procedure_source_value" in kwargs:
        procedure_source_value = kwargs["procedure_source_value"]
    else :
        raise ValueError("procedure_source_value 없습니다.")
    
    print('원천 데이터 개수:', len(source1))

    # 원천에서 조건걸기
    source1[orddate] = pd.to_datetime(source1[orddate], format="%Y%m%d")
    source1[exectime] = pd.to_datetime(source1[exectime], format="%Y%m%d%H%M%S", errors = "coerce")
    source1["MEDTIME"] = source1["MEDTIME"].apply(lambda x : x[:4] + "-" + x[4:6] + "-" + x[6:8] + " " + x[8:10] + ":" + x[10:])
    source1["MEDTIME"] = pd.to_datetime(source1["MEDTIME"], errors = "coerce")
    source1 = source1[(source1[orddate] <= "2023-08-31") & ((source1["DCYN"] == "N" )| (source1["DCYN"] == None)) & source1["ORDCLSTYP"] != "D2"]
    source = source1[["PATNO", orddate, exectime, "ORDSEQNO", opdate, procedure_source_value, meddept, provider, "MEDTIME", "PATFG"]]
    print(f"조건적용 후 원천 데이터 수: {len(source)}")

    local_edi = local_edi[["ORDCODE", "FROMDATE", "TODATE", "INSEDICODE", "concept_id"]]
    local_edi["FROMDATE"] = pd.to_datetime(local_edi["FROMDATE"] , format="%Y%m%d", errors="coerce")
    local_edi["FROMDATE"].fillna(pd.Timestamp('1900-01-01'), inplace = True)
    local_edi["TODATE"] = pd.to_datetime(local_edi["TODATE"] , format="%Y%m%d", errors="coerce")
    local_edi["TODATE"].fillna(pd.Timestamp('2099-12-31'), inplace = True)

    # LOCAL코드와 EDI코드 매핑 테이블과 병합
    source = pd.merge(source, local_edi, left_on=procedure_source_value, right_on="ORDCODE", how="left")
    print('EDI코드 테이블과 병합 후 데이터 개수', len(source))
    source = source[(source[orddate] >= source["FROMDATE"]) & (source[orddate] <= source["TODATE"])]
    print("EDI코드 사용기간별 필터 적용 후 데이터 개수", len(source))

    # person table과 병합
    source = pd.merge(source, person_data, left_on="PATNO", right_on="person_source_value", how="inner")
    source = source.drop(columns = ["care_site_id", "provider_id"])
    # care_site table과 병합
    source = pd.merge(source, care_site_data, left_on=meddept, right_on="care_site_source_value", how="left")
    # provider table과 병합
    source = pd.merge(source, provider_data, left_on=provider, right_on="provider_source_value", how="left", suffixes=('', '_y'))

    # visit_start_datetime 형태 변경
    visit_data["visit_start_datetime"] = pd.to_datetime(visit_data["visit_start_datetime"])

    # visit_occurrence table과 병합
    source = pd.merge(source, visit_data, left_on=["person_id", "care_site_id", "PATFG", "MEDTIME"], right_on=["person_id", "care_site_id", "visit_source_value", "visit_start_datetime"], how="left", suffixes=('', '_y'))

    # visit_detail table과 병합
    source = pd.merge(source, visit_detail, left_on=["visit_occurrence_id"], right_on=["visit_occurrence_id"], how="left", suffixes=('', '_y'))
    print(len(source))
    # 값이 없는 경우 0으로 값 입력
    source.loc[source["care_site_id"].isna(), "care_site_id"] = 0
    source.loc[source["concept_id"].isna(), "concept_id"] = 0

    print(source.columns.to_list())

    procedure_date_condition = [source[opdate].notna()
                                , source[exectime].notna()
                                ]
    procedure_date_value = [pd.to_datetime(source[opdate], format = "%Y%m%d").dt.date
                            , source[exectime].dt.date
                            ]
    procedure_datetime_value = [
                            pd.to_datetime(source[opdate], format = "%Y%m%d")
                            , pd.to_datetime(source[exectime], format = "%Y%m%d%H%M%S")
                            ]
                            
    cdm = pd.DataFrame({
        "procedure_occurrence_id": source.index + 1,
        "person_id": source["person_id"],
        "procedure_concept_id": source["concept_id"],
        "procedure_date": np.select(procedure_date_condition, procedure_date_value, default = source[orddate].dt.date),
        "procedure_datetime": np.select(procedure_date_condition, procedure_datetime_value, default = source[orddate]),
        "procedure_type_concept_id": 38000275,
        "modifier_concept_id": 0,
        "quantity": None,
        "provider_id": source["provider_id"],
        "visit_occurrence_id": source["visit_occurrence_id"],
        "visit_detail_id": source["visit_detail_id"],
        "procedure_source_value": source[procedure_source_value],
        "procedure_source_concept_id": source["INSEDICODE"],
        "modifier_source_value": None ,
        "vocabulary_id": "EDI"
        })

    cdm["procedure_date"] = pd.to_datetime(cdm["procedure_date"])
    cdm["procedure_datetime"] = pd.to_datetime(cdm["procedure_datetime"])

    # csv파일로 저장
    cdm.to_csv(os.path.join(CDM_path, "procedure_order_trt.csv"), index=False)

    print(cdm.isnull().sum())
    print(cdm.shape)
    print('CDM 데이터 개수', len(cdm))
    

start_time = datetime.now()
transform_to_procedure_order_trt( source_path = "F:\\01.감염병데이터베이스\\data\\2023_infectious_DW"
                            , CDM_path = "F:\\01.감염병데이터베이스\\data\\2023_infectious_CDM"
                            , source_data1 = "ods_mmtrtort.csv"
                            , care_site_data = "care_site.csv"
                            , person_data = "person.csv"
                            , provider_data = "provider.csv"
                            , visit_data = "visit_occurrence.csv"
                            , visit_detail = "visit_detail.csv"
                            , local_edi = "local_edi.csv"
                            , meddept = "MEDDEPT"
                            , provider = "ORDDR"

                            , orddate = "ORDDATE"
                            , exectime = "EXECTIME"
                            , opdate = "OPDATE"
                            , procedure_source_value = "ORDCODE"
)

print(f"transform and Load end, elapsed_time is : {datetime.now() - start_time}")