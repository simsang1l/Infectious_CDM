import os
import pandas as pd
import numpy as np

def transform_to_provider(**kwargs):
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
        source = pd.read_csv(os.path.join(source_path, kwargs["source_data"]), dtype=str)
    else :
        raise ValueError("source_data(원천 데이터)가 없습니다.")

    if "care_site_data" in kwargs:
        care_site = pd.read_csv(os.path.join(CDM_path, kwargs["care_site_data"]), dtype=str)
    else :
        raise ValueError("care_site_data가 없습니다.")

    if "provider_name" in kwargs :
        provider_name = kwargs["provider_name"]
    else :
        raise ValueError("provider_name 없습니다.")
    
    if "year_of_birth" in kwargs :
        year_of_birth = kwargs["year_of_birth"]
    else :
        raise ValueError("year_of_birth 없습니다.")
    
    if "provider_source_value" in kwargs:
        provider_source_value = kwargs["provider_source_value"]
    else :
        raise ValueError("provider_source_value 없습니다.")
    
    if "specialty_source_value" in kwargs:
        specialty_source_value = kwargs["specialty_source_value"]
    else :
        raise ValueError("specialty_source_value 없습니다")    
    
    if "gender_source_value" in kwargs:
        gender_source_value = kwargs["gender_source_value"]
    else :
        raise ValueError("gender_source_value 없습니다.")
    
    if "care_site_source_value" in kwargs:
        care_site_source_value = kwargs["care_site_source_value"]
    else :
        raise ValueError("care_site_source_value 없습니다.")
    
    # source = pd.merge(source, care_site, left_on = care_site_source_value, right_on="care_site_source_value", how = "left")

    # specialty_conditions = [
    #     source[specialty_source_value].isin(['500', '916', '912']),
    #     source[specialty_source_value].isin(['010', '020', '100', '110', '120'
	# 							, '121', '122', '130', '133', '140'
	# 							, '150', '160', '170', '180', '200'])
    # ]
    # specialty_concept_id = [32581, 32577]

    gender_conditions = [
        source[gender_source_value].isin(['남']),
        source[gender_source_value].isin(['여'])
    ]
    gender_concept_id = [8507, 8532]

    cdm = pd.DataFrame({
        "provider_id" : source.index + 1,
        "provider_name": None,
        "npi": None,
        "dea": None,
        "specialty_concept_id": 0,
        "care_site_id": 0, 
        "year_of_birth": None,
        "gender_concept_id": np.select(gender_conditions, gender_concept_id, default = 0),
        "provider_source_value": source[provider_source_value],
        "specialty_source_value": source[specialty_source_value],
        "specialty_source_concept_id": 0,
        "gender_source_value": source[gender_source_value],
        "gender_source_concept_id": np.select(gender_conditions, gender_concept_id, default = 0),
        })
    

    cdm.to_csv(os.path.join(CDM_path, "provider.csv"), index=False)

    print('원천 데이터 개수:', len(source))
    print('CDM 데이터 개수', len(cdm))
    


transform_to_provider( source_path = "F:\\01.감염병데이터베이스\\data\\dsmc\\emr",
                       CDM_path = "F:\\01.감염병데이터베이스\\data\\dsmc\\cdm",
                       source_data = "provider.csv",
                       care_site_data = "care_site.csv",
                       provider_name = None,
                       year_of_birth = "의료제공자 출생년도",
                       provider_source_value = "의료제공자 식별ID",
                       specialty_source_value  = "의료제공자 전문분야",
                       gender_source_value = "의료제공자 성별",
                       care_site_source_value = None 
)