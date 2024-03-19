import os
import pandas as pd
import numpy as np

def transform_to_person(**kwargs):
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
        source = pd.read_csv(os.path.join(source_path, kwargs["source_data"]), dtype=str, encoding="cp949")
    else :
        raise ValueError("source_data(원천 데이터)가 없습니다.")

    if "location_data" in kwargs:
        location_data = pd.read_csv(os.path.join(CDM_path, kwargs["location_data"]), dtype=str)
    else :
        raise ValueError("location_data가 없습니다.")

    if "gender_source_value" in kwargs :
        gender_source_value = kwargs["gender_source_value"]
    else :
        raise ValueError("gender_source_value 없습니다.")
    
    if "person_source_value" in kwargs:
        person_source_value = kwargs["person_source_value"]
    else :
        raise ValueError("person_source_value 없습니다.")
    
    if "death_datetime" in kwargs:
        death_datetime = kwargs["death_datetime"]
    else :
        raise ValueError("death_datetime 없습니다")    
    
    if "birth_datetime" in kwargs:
        birth_datetime = kwargs["birth_datetime"]
    else :
        raise ValueError("birth_datetime 없습니다.")
    
    if "race_source_value" in kwargs:
        race_source_value = kwargs["race_source_value"]
    else :
        raise ValueError("race_source_value 없습니다.")
    
    print('원천 데이터 개수:', len(source))

    source['ZIPCD1'] = source['ZIPCD1']
    
    source = pd.merge(source, location_data, left_on = "ZIPCD1", right_on="ZIP", how = "left")
    source.loc[source["LOCATION_ID"].isna(), "LOCATION_ID"] = 0

    race_conditions = [
        source[race_source_value] == 'N',
        source[race_source_value] == 'Y'
    ]
    race_concept_id = [38003585, 8552]

    gender_conditions = [
        source[gender_source_value].isin(['M']),
        source[gender_source_value].isin(['F'])
    ]
    gender_concept_id = [8507, 8532]

    cdm = pd.DataFrame({
        "person_id" : source.index + 1,
        "gender_concept_id": np.select(gender_conditions, gender_concept_id, default = 0),
        "year_of_birth": source[birth_datetime].str[:4],
        "month_of_birth": source[birth_datetime].str[4:6],
        "day_of_birth": source[birth_datetime].str[6:8],
        "birth_datetime": pd.to_datetime(source[birth_datetime], format = "%Y%m%d", errors='coerce'),
        "death_datetime": pd.to_datetime(source[death_datetime], format = "%Y%m%d", errors='coerce'),
        "race_concept_id": np.select(race_conditions, race_concept_id, default = 0),
        "ethnicity_concept_id": 0,
        "location_id": source["LOCATION_ID"],
        "provider_id": 0,
        "care_site_id": 0, 
        "person_source_value": source[person_source_value],
        "gender_source_value": source[gender_source_value],
        "gender_source_concept_id": np.select(gender_conditions, gender_concept_id, default = 0),
        "race_source_value": source[race_source_value],
        "race_source_concept_id": 0,
        "ethnicity_source_value": None,
        "ethnicity_source_concept_id": 0
        })
    
    cdm["birth_datetime"] = cdm["birth_datetime"].dt.strftime('%Y-%m-%d %H:%M:%S')
    cdm["death_datetime"] = cdm["death_datetime"].dt.strftime('%Y-%m-%d %H:%M:%S')

    cdm.to_csv(os.path.join(CDM_path, "person.csv"), index=False)

    
    print('CDM 데이터 개수', len(cdm))
    


transform_to_person( source_path = "F:\\01.감염병데이터베이스\\data\\knuh\\emr",
                     CDM_path = "F:\\01.감염병데이터베이스\\data\\knuh\\cdm",
                     source_data = "19.PMCMPTBS_환자기본정보.csv",
                     location_data = "location.csv",
                     gender_source_value = "SEX",
                     person_source_value = "PID",
                     birth_datetime = "BRTHDD",
                     death_datetime = "DETHDT",
                     race_source_value = "FORGERYN"
)