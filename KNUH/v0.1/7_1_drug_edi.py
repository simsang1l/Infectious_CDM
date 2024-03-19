import os
import pandas as pd
import numpy as np
from datetime import datetime

def transform_to_drug_edi(**kwargs):
    # kwargs가 모두 입력됐는지 확인 및 변수 정의
    if "source_path" in kwargs :
        source_path = kwargs["source_path"]
    else :
        raise ValueError("source_path가 없습니다.")

    if "CDM_path" in kwargs:
        CDM_path = kwargs["CDM_path"]
    else :
        raise ValueError("CDM_path가 없습니다.")

    if "order_data" in kwargs :
        source = pd.read_csv(os.path.join(source_path, kwargs["order_data"]), dtype=str, encoding = 'cp949')
    else :
        raise ValueError("order_data가 없습니다.")
    
    if "concept_data" in kwargs:
        concept_data = pd.read_csv(os.path.join(CDM_path, kwargs["concept_data"]), dtype=str)
    else :
        raise ValueError("concept_data가 없습니다.")
    
    #####
    if "ordercode" in kwargs :
        ordercode = kwargs["ordercode"]
    else :
        raise ValueError("ordercode 없습니다.")
    
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
    
    
    print(f'원천 데이터 개수, 처방코드: {len(source)}')


    # null값에 fromdate, todate 설정
    source[fromdate].fillna("19000101")
    source[todate].fillna("20991231")
    # source.loc[source[todate] == '99991231', todate] = '20991231'

    # KDC, EDI 순으로 매핑을 위한 처리
    concept_data = concept_data.sort_values(by = ["vocabulary_id"], ascending=[False])
    concept_data['Sequence'] = concept_data.groupby(["concept_code"]).cumcount() + 1
    concept_data = concept_data[concept_data["Sequence"] == 1]

    # concept_id 매핑
    source = pd.merge(source, concept_data, left_on=edicode, right_on="concept_code", how="left")
    print('concept merge후 데이터 개수', len(source))


    local_edi = source[[ordercode, fromdate, todate, edicode, "concept_id", "concept_name", "domain_id", "vocabulary_id", "concept_class_id", "standard_concept", "concept_code", "valid_start_date", "valid_end_date", "invalid_reason"]]

    # csv파일로 저장
    local_edi.to_csv(os.path.join(CDM_path, "drug_edi.csv"), index=False)

    print(local_edi)
    print(local_edi.shape)
    print('CDM 데이터 개수', len(local_edi))
    

start_time = datetime.now()
transform_to_drug_edi( source_path = "F:\\01.감염병데이터베이스\\data\\knuh\\emr",
                        CDM_path = "F:\\01.감염병데이터베이스\\data\\knuh\\cdm",
                        order_data = "22.ADBMDRUG_약제마스터.csv",
                        concept_data = "concept_EDI_KDC.csv",
                        ordercode = "DRUGCD",
                        edicode = "EDICD",
                        fromdate = "DRUGFROMDD",
                        todate = "DRUGTODD"
)

print(f"transform and Load end, elapsed_time is : {datetime.now() - start_time}")