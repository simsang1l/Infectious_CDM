import os
import pandas as pd


def transform_to_care_site(**kwargs):
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

    if "location_data" in kwargs:
        location = pd.read_csv(os.path.join(CDM_path, kwargs["location_data"]), dtype=str)
    else :
        raise ValueError("location_data가 없습니다.")

    if "care_site_name" in kwargs :
        care_site_name = kwargs["care_site_name"]
    else :
        raise ValueError("care_site_name이 없습니다.")
    
    if "care_site_source_value" in kwargs:
        care_site_source_value = kwargs["care_site_source_value"]
    else :
        raise ValueError("care_site_source_value가 없습니다")    
    
    if "place_of_service_source_value" in kwargs:
        place_of_service_source_value = kwargs["place_of_service_source_value"]
    else :
        raise ValueError("place_of_service_source_value가 없습니다.")
    
    cdm = pd.DataFrame({
        "care_site_id" : source.index + 1,
        "care_site_name": source[care_site_name],
        "place_of_service_concept_id": 0,
        "location_id": location.loc[location["ZIP"] == "426", "LOCATION_ID"].tolist()[0],
        "care_site_source_value": source[care_site_source_value],
        "place_of_service_source_value": source[place_of_service_source_value] 
        })
    
    cdm.to_csv(os.path.join(CDM_path, "care_site.csv"), index=False)
    


transform_to_care_site( source_path = "F:\\01.감염병데이터베이스\\data\\dsmc\\emr",
                        CDM_path = "F:\\01.감염병데이터베이스\\data\\dsmc\\cdm",
                        source_data = "care_site.csv",
                        location_data = "location.csv",
                        care_site_name = "병원구분",
                        care_site_source_value = "부서코드",
                        place_of_service_source_value = "병원구분"
)