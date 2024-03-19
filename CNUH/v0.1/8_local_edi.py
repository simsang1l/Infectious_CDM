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
    
    if "edi_data" in kwargs :
        edi_data = pd.read_csv(os.path.join(source_path, kwargs["edi_data"]), dtype=str, encoding="cp949", skiprows = 1)
    else :
        raise ValueError("edi_data가 없습니다.")
    
    if "concept_data" in kwargs:
        concept_data = pd.read_csv(os.path.join(CDM_path, kwargs["concept_data"]), dtype=str)
    else :
        raise ValueError("concept_data가 없습니다.")
    
    #####
    if "ordercode" in kwargs :
        ordercode = kwargs["ordercode"]
    else :
        raise ValueError("ordercode 없습니다.")
    
    if "sugacode" in kwargs :
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
    
    
    print(f'원천 데이터 개수, 처방코드: {len(order_data)}, EDI: {len(edi_data)}, {len(order_data) + len(edi_data)}')

    # 처방코드 마스터와 수가코드 매핑
    source = pd.merge(edi_data, order_data, left_on=[sugacode, "병원구분코드"], right_on=[ordercode, "병원구분코드"], how="left", suffixes=('', '_y'))
    print(len(source))

    # null값에 fromdate, todate 설정
    source[fromdate].fillna("19000101")
    source[todate].fillna("20991231")
    # source[fromdate] = source["FROMDATE_y"].where(source["FROMDATE_y"].notna(), source["FROMDATE_x"])
    # source[todate] = source["TODATE_y"].where(source["TODATE_y"].notna(), source["TODATE_x"])

    concept_data = concept_data.sort_values(by = ["vocabulary_id"], ascending=[False])
    concept_data['Sequence'] = concept_data.groupby(["concept_code"]).cumcount() + 1
    concept_data = concept_data[concept_data["Sequence"] == 1]

    # concept_id 매핑
    source = pd.merge(source, concept_data, left_on=edicode, right_on="concept_code", how="left")
    print('concept merge후 데이터 개수', len(source))

    # drug의 경우 KCD, EDI 순으로 매핑
    source = source.sort_values(by = [ordercode, fromdate, "vocabulary_id"], ascending=[True, True, False])
    source['Sequence'] = source.groupby([ordercode, fromdate]).cumcount() + 1
    source = source[source["Sequence"] == 1]
    print('중복되는 concept_id 제거 후 데이터 개수: ', len(source))

    local_edi = source[[sugacode, fromdate, todate, edicode, "병원구분코드", "한글수가명", "영문수가명", "concept_id", "concept_name", "domain_id", "vocabulary_id", "concept_class_id", "standard_concept", "concept_code", "valid_start_date", "valid_end_date", "invalid_reason"]]

    # csv파일로 저장
    local_edi.to_csv(os.path.join(CDM_path, "local_edi.csv"), index=False)

    print(local_edi)
    print(local_edi.shape)
    print('CDM 데이터 개수', len(local_edi))
    

start_time = datetime.now()
transform_to_local_edi( source_path = "F:\\01.감염병데이터베이스\\data\\cnuh\\emr",
                        CDM_path = "F:\\01.감염병데이터베이스\\data\\cnuh\\cdm",
                        order_data = "처방코드분류_전남대학교병원.csv", # 처방코드마스터
                        edi_data = "AIMIRPMC_수가코드.csv", # 수가마스터
                        concept_data = "concept_EDI_KDC.csv",
                        ordercode = "처방코드",
                        sugacode = "수가코드",
                        edicode = "보험EDI코드",
                        fromdate = "적용시작일자",
                        todate = "적용종료일자"
)

print(f"transform and Load end, elapsed_time is : {datetime.now() - start_time}")