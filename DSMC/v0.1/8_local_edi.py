import os
import pandas as pd
import numpy as np
from datetime import datetime

def transform_to_local_edi(**kwargs):
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
        order_data = pd.read_csv(os.path.join(source_path, kwargs["order_data"]), dtype=str)
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
    
    source = order_data
    print(f"원천 데이터 개수, {len(source)}")

    # drug의 경우 concept_id를 KDC, EDI 순으로 매핑
    concept_data = concept_data.sort_values(by = ["vocabulary_id"], ascending=[False])
    concept_data['Sequence'] = concept_data.groupby(["concept_code"]).cumcount() + 1
    concept_data = concept_data[concept_data["Sequence"] == 1]
    print('중복되는 concept_id 제거 후 데이터 개수: ', len(source))

    print(source.columns.tolist())
    # concept_id 매핑
    source = pd.merge(source, concept_data, left_on=edicode, right_on="concept_code", how="left")
    print('concept merge후 데이터 개수', len(source))

    # csv파일로 저장
    source.to_csv(os.path.join(CDM_path, "local_edi.csv"), index=False)

    print(source)
    print(source.shape)
    print('CDM 데이터 개수', len(source))
    

start_time = datetime.now()
transform_to_local_edi( source_path = "F:\\01.감염병데이터베이스\\data\\dsmc\\emr",
                        CDM_path = "F:\\01.감염병데이터베이스\\data\\dsmc\\cdm",
                        order_data = "계명대병원_처방코드마스터_20231117.csv",
                        concept_data = "concept_EDI_KDC.csv",
                        ordercode = "처방코드",
                        edicode = "EDI 코드",
                        fromdate = "적용시작일",
                        todate = "적용종료일"
)

print(f"transform and Load end, elapsed_time is : {datetime.now() - start_time}")