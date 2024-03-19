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
        source1 = pd.read_csv(os.path.join(source_path, kwargs["source_data1"]), dtype=str)
    else :
        raise ValueError("source_data1(원천 데이터)가 없습니다.")

    if "source_data2" in kwargs :
        source2 = pd.read_csv(os.path.join(source_path, kwargs["source_data2"]), dtype=str)
    else :
        raise ValueError("source_data2(원천 데이터)가 없습니다.")

    if "source_data3" in kwargs :
        source3 = pd.read_csv(os.path.join(source_path, kwargs["source_data3"]), dtype=str)
    else :
        raise ValueError("source_data3(원천 데이터)가 없습니다.")
    
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
    
    if "meddept" in kwargs:
        meddept = kwargs["meddept"]
    else :
        raise ValueError("meddept 없습니다.")

    if "provider" in kwargs:
        provider = kwargs["provider"]
    else :
        raise ValueError("provider 없습니다.")

    if "orddate" in kwargs :
        orddate = kwargs["orddate"]
    else :
        raise ValueError("orddate 없습니다.")
    
    if "procedure_source_value" in kwargs:
        procedure_source_value = kwargs["procedure_source_value"]
    else :
        raise ValueError("procedure_source_value 없습니다.")
    
    print('원천 데이터 개수:', len(source1))

    # 원천에서 조건걸기
    source1 = source1[["INSTCD", orddate, "PID", "PRCPHISTCD", "ORDDD", "CRETNO", "PRCPCLSCD", "PRCPNO", "PRCPHISTNO", "FSTRGSTDT", "LASTUPDTDT", "ORDDRID", "PRCPNM", "PRCPCD", meddept]]
    source1[orddate] = pd.to_datetime(source1[orddate])
    source1["ORDDD"] = pd.to_datetime(source1["ORDDD"])
    source1 = source1[(source1[orddate] <= "2023-08-31")]

    source2 = source2[["INSTCD", orddate, "PRCPNO", "PRCPHISTNO", "EXECPRCPUNIQNO"]]
    source2["HISORDERID"] = source2["PRCPDD"] + source2["EXECPRCPUNIQNO"]
    source2[orddate] = pd.to_datetime(source2[orddate])
    source2 = source2[(source2[orddate] <= "2023-08-31")]

    source3 = source3[["PATID", "HISORDERID", "QUEUEID", "READTEXT", "CONFDATE", "CONFTIME"]]
    print(f"조건적용 후 원천 데이터 수: {len(source1)}, {len(source2)}, {len(source3)}")

    source = pd.merge(source1, source2, left_on=["INSTCD", orddate, "PRCPNO", "PRCPHISTNO"], right_on=["INSTCD", orddate, "PRCPNO", "PRCPHISTNO"], how="inner", suffixes=("", "_2"))
    print(f"결합 후 원천 데이터 수: {len(source)}")
    del source1
    del source2

    source3 = source3.groupby('HISORDERID').agg({'PATID': 'first', 'QUEUEID': 'max', "CONFDATE": "first", "CONFTIME": "first"}).reset_index()
    source = pd.merge(source, source3, left_on=["PID", "HISORDERID"], right_on=["PATID", "HISORDERID"], how="inner", suffixes=("", "_3"))
    del source3

    # procedure_datetime 컬럼 추가
    source["procedure_datetime"] = source["CONFDATE"] + source["CONFTIME"]
    source["procedure_datetime"] = pd.to_datetime(source["procedure_datetime"])
    print(f"결합 후 원천 데이터 수: {len(source)}")
    print(source.columns.tolist())


    local_edi = local_edi[[sugacode, fromdate, todate, edicode, "concept_id", "INSTCD", "SPCCD"]]
    local_edi[fromdate] = pd.to_datetime(local_edi[fromdate], errors="coerce")
    local_edi[fromdate].fillna(pd.Timestamp('1900-01-01'), inplace = True)
    local_edi[todate] = pd.to_datetime(local_edi[todate], errors="coerce")
    local_edi[todate].fillna(pd.Timestamp('2099-12-31'), inplace = True)

    # LOCAL코드와 EDI코드 매핑 테이블과 병합
    source = pd.merge(source, local_edi, left_on=[procedure_source_value, "INSTCD"], right_on=[sugacode, "INSTCD"], how="left")
    print('EDI코드 테이블과 병합 후 데이터 개수', len(source))
    # source = source[(source[orddate] >= source[fromdate]) & (source[orddate] <= source[todate])]
    print("EDI코드 사용기간별 필터 적용 후 데이터 개수", len(source))

    # person table과 병합
    source = pd.merge(source, person_data, left_on="PID", right_on="person_source_value", how="inner")
    source = source.drop(columns = ["care_site_id", "provider_id"])
    # care_site table과 병합
    source = pd.merge(source, care_site_data, left_on=meddept, right_on="care_site_source_value", how="left")
    # provider table과 병합
    source = pd.merge(source, provider_data, left_on=provider, right_on="provider_source_value", how="left", suffixes=('', '_y'))

    # visit_start_date 형태 변경
    visit_data["visit_start_date"] = pd.to_datetime(visit_data["visit_start_date"])

    # visit_occurrence table과 병합
    source = pd.merge(source, visit_data, left_on=["person_id", "care_site_id", "ORDDD" ], right_on=["person_id", "care_site_id", "visit_start_date" ], how="left", suffixes=('', '_y'))

    # visit_detail table과 병합
    # source = pd.merge(source, visit_detail, left_on=["visit_occurrence_id"], right_on=["visit_occurrence_id"], how="left", suffixes=('', '_y'))

    print(len(source))
    # 값이 없는 경우 0으로 값 입력
    source.loc[source["care_site_id"].isna(), "care_site_id"] = 0
    source.loc[source["concept_id"].isna(), "concept_id"] = 0

    print(source.columns.to_list())
    
    cdm = pd.DataFrame({
        "procedure_occurrence_id": source.index + 1,
        "person_id": source["person_id"],
        "procedure_concept_id": source["concept_id"],
        "procedure_date": np.select([source["procedure_datetime"].notna()], [source["procedure_datetime"].dt.date], default = source[orddate].dt.date),
        "procedure_datetime": np.select([source["procedure_datetime"].notna()], [source["procedure_datetime"]], default = source[orddate]),
        "procedure_type_concept_id": 38000275,
        "modifier_concept_id": 0,
        "quantity": None,
        "provider_id": 0,
        "visit_occurrence_id": source["visit_occurrence_id"],
        "visit_detail_id": 0,
        "procedure_source_value": source[procedure_source_value],
        "procedure_source_concept_id": source[edicode],
        "modifier_source_value": None ,
        "vocabulary_id": "EDI"
        })

    # cdm["procedure_date"] = pd.to_datetime(cdm["procedure_date"])
    # cdm["procedure_datetime"] = pd.to_datetime(cdm["procedure_datetime"])

    # csv파일로 저장
    cdm.to_csv(os.path.join(CDM_path, "procedure_occurrence.csv"), index=False)

    print(cdm.isnull().sum())
    print(cdm.shape)
    print('CDM 데이터 개수', len(cdm))
    

start_time = datetime.now()
transform_to_procedure_occurrence( source_path = "F:\\01.감염병데이터베이스\\data\\knuh\\emr",
                                   CDM_path = "F:\\01.감염병데이터베이스\\data\\knuh\\cdm",
                                   source_data1 = "02.MMOHIPRC_검사처방.csv", # 검사처방
                                   source_data2 = "28.MMODEXIP_처방상세.csv", # 처방상세
                                   source_data3 = "09.MMOHIPRC_영상검사결과.csv", # 영상검사 결과
                                   person_data = "person.csv",
                                   care_site_data = "care_site.csv",
                                   provider_data = "provider.csv",
                                   visit_data = "visit_occurrence.csv",
                                   local_edi = "local_edi.csv",
                                   sugacode = "CALCSCORCD", # local_edi의 처방코드
                                   edicode = "INSUEDICD", 
                                   fromdate = "FROMDD", # local_edi의 처방코드 사용 시작일
                                   todate = "TODD", # local_edi의 처방코드 사용 종료일
                                   meddept = "ORDDEPTCD",
                                   provider = "ORDDRID",
                                   orddate = "PRCPDD",
                                   procedure_source_value = "PRCPCD"
)

print(f"transform and Load end, elapsed_time is : {datetime.now() - start_time}")