import os
import pandas as pd
import numpy as np
from datetime import datetime

def transform_to_visit_occurrence(**kwargs):
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
        source = pd.read_csv(os.path.join(source_path, kwargs["source_data"]), dtype=str, encoding="CP949")
    else :
        raise ValueError("source_data(원천 데이터)가 없습니다.")

    if "source_data2" in kwargs :
        source2 = pd.read_csv(os.path.join(source_path, kwargs["source_data2"]), dtype=str, encoding="CP949")
    else :
        raise ValueError("source_data2(원천 데이터)가 없습니다.")
    
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
    
    if "admitting_from_source_value" in kwargs :
        admitting_from_source_value = kwargs["admitting_from_source_value"]
    else :
        raise ValueError("admitting_from_source_value 없습니다.")
    
    if "discharge_to_source_value" in kwargs:
        discharge_to_source_value = kwargs["discharge_to_source_value"]
    else :
        raise ValueError("discharge_to_source_value 없습니다.")

    if "meddate" in kwargs:
        meddate = kwargs["meddate"]
    else :
        raise ValueError("meddate 없습니다")  

    if "medtime" in kwargs:
        medtime = kwargs["medtime"]
    else :
        raise ValueError("medtime 없습니다")    
    
    if "admdate" in kwargs:
        admdate = kwargs["admdate"]
    else :
        raise ValueError("admdate 없습니다.")

    if "admtime" in kwargs:
        admtime = kwargs["admtime"]
    else :
        raise ValueError("admtime 없습니다.")
    
    if "dschdate" in kwargs:
        dschdate = kwargs["dschdate"]
    else :
        raise ValueError("dschdate 없습니다.")

    if "dschtime" in kwargs:
        dschtime = kwargs["dschtime"]
    else :
        raise ValueError("dschtime 없습니다.")
    
    if "meddept" in kwargs:
        meddept = kwargs["meddept"]
    else :
        raise ValueError("meddept 없습니다.")
    
    if "visit_source_value" in kwargs:
        visit_source_value = kwargs["visit_source_value"]
    else :
        raise ValueError("visit_source_value 없습니다.")
    
    print('원천 데이터 개수:', len(source), len(source2))

    ## 외래데이터 변환하기
    # person table과 병합
    source = pd.merge(source, person_data, left_on="PID", right_on="person_source_value", how="inner")
    source = source.drop(columns = ["care_site_id", "provider_id"])
    source = pd.merge(source, care_site_data, left_on=[meddept, "INSTCD"], right_on=["care_site_source_value", "place_of_service_source_value"], how="left")
    source = pd.merge(source, provider_data, left_on="ORDDRID", right_on="provider_source_value", how="left", suffixes=('', '_y'))

    source.loc[source["care_site_id"].isna(), "care_site_id"] = 0
    
    cdm_o = pd.DataFrame({
        "person_id": source["person_id"],
        "visit_concept_id": 9202,
        "visit_start_date": pd.to_datetime(source[meddate], format="%Y%m%d"),
        "visit_start_datetime": pd.to_datetime(source[meddate] + source[medtime], format="%Y%m%d%H%M%S"),
        "visit_end_date": pd.to_datetime(source[meddate], format="%Y%m%d"),
        "visit_end_datetime": pd.to_datetime(source[meddate] + source[medtime], format="%Y%m%d%H%M%S"),
        "visit_type_concept_id": 44818518,
        "provider_id": source["provider_id"],
        "care_site_id": source["care_site_id"],
        "visit_source_value": source[visit_source_value],
        "visit_source_concept_id": 9202,
        "admitted_from_concept_id": 0,
        "admitted_from_source_value": None,
        "discharge_to_concept_id": 0,
        "discharge_to_source_value": None
        })


    ## 입원 응급 데이터 변환하기
    ## 외래데이터 변환하기
    # person table과 병합
    source2 = pd.merge(source2, person_data, left_on="PID", right_on="person_source_value", how="inner")
    source2 = source2.drop(columns = ["care_site_id", "provider_id"])
    source2 = pd.merge(source2, care_site_data, left_on=[meddept, "INSTCD"], right_on=["care_site_source_value", "place_of_service_source_value"], how="left")
    source2 = pd.merge(source2, provider_data, left_on="ATDOCTID", right_on="provider_source_value", how="left", suffixes=('', '_y'))

    visit_condition = [
        source2[visit_source_value] == "I",
        source2[visit_source_value] == "E",
    ]
    visit_concept_id = [9201, 9203]

    
    cdm_ie = pd.DataFrame({
        "person_id": source2["person_id"],
        "visit_concept_id": np.select(visit_condition, visit_concept_id, default = 0),
        "visit_start_date": pd.to_datetime(source2[admdate], format="%Y%m%d"),
        "visit_start_datetime": pd.to_datetime(source2[admdate] + source2[admtime], format="%Y%m%d%H%M%S"),
        "visit_end_date": pd.to_datetime(source2[dschdate], format="%Y%m%d"),
        "visit_end_datetime": (source2[dschdate] + source2[dschtime]).apply(lambda x: datetime.strptime(x, '%Y%m%d%H%M%S')),
        "visit_type_concept_id": 44818518,
        "provider_id": source2["provider_id"],
        "care_site_id": source2["care_site_id"],
        "visit_source_value": source2[visit_source_value],
        "visit_source_concept_id": np.select(visit_condition, visit_concept_id, default = 0),
        "admitted_from_concept_id": 0,
        "admitted_from_source_value": source2[admitting_from_source_value],
        "discharge_to_concept_id": 0,
        "discharge_to_source_value": source2[discharge_to_source_value]
        })

    cdm = pd.concat([cdm_o, cdm_ie], axis = 0)
    cdm["visit_occurrence_id"] = cdm.index + 1
    cdm["preceding_visit_occurrence_id"] = cdm["visit_occurrence_id"].shift(1)

    columns = ["visit_occurrence_id", "person_id", "visit_concept_id", "visit_start_date", "visit_start_datetime"
               , "visit_end_date", "visit_end_datetime", "visit_type_concept_id", "provider_id"
               , "care_site_id", "visit_source_value", "visit_source_concept_id", "admitted_from_concept_id"
               , "admitted_from_source_value", "discharge_to_concept_id", "discharge_to_source_value"
               , "preceding_visit_occurrence_id"]
    cdm = cdm[columns]

    print(len(cdm_o), len(cdm_ie), len(cdm_o)+len(cdm_ie))
    print(cdm.shape)
    cdm = cdm[cdm["visit_start_date"] <= "2023-08-31"]
    cdm.to_csv(os.path.join(CDM_path, "visit_occurrence.csv"), index=False, float_format = '%.0f')

    print('CDM 데이터 개수', len(cdm_o), len(cdm_ie))
    print('CDM 데이터 개수', len(cdm))
    

start_time = datetime.now()
transform_to_visit_occurrence( source_path = "F:\\01.감염병데이터베이스\\data\\knuh\\emr",
                               CDM_path = "F:\\01.감염병데이터베이스\\data\\knuh\\cdm",
                               source_data = "20.PMOHOTPT_외래환자예약.csv",
                               source_data2 = "16.PMIHINPT_환자입원이력.csv",
                               care_site_data = "care_site.csv",
                               person_data = "person.csv",
                               provider_data = "provider.csv",
                               admitting_from_source_value = "INPATH",
                               discharge_to_source_value = "DSCHTYPE",
                               meddate = "ORDDD",
                               medtime = "ORDTM",
                               admdate = "INDD",
                               admtime = "INTM",
                               dschdate = "DSCHDD",
                               dschtime = "DSCHTM",
                               meddept = "ORDDEPTCD",
                               visit_source_value = "ORDTYPE"
)

print(f"transform and Load end, elapsed_time is : {datetime.now() - start_time}")