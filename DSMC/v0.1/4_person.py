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
        source = pd.read_csv(os.path.join(source_path, kwargs["source_data"]), dtype=str)
    else :
        raise ValueError("source_data(원천 데이터)가 없습니다.")

    if "location_data" in kwargs:
        location_data = pd.read_csv(os.path.join(CDM_path, kwargs["location_data"]), dtype=str)
    else :
        raise ValueError("location_data가 없습니다.")

    if "death_data" in kwargs:
        death_data = pd.read_csv(os.path.join(source_path, kwargs["death_data"]), dtype=str)
    else :
        raise ValueError("death_data 없습니다.")
    
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
    
    if "race_source_value" in kwargs:
        race_source_value = kwargs["race_source_value"]
    else :
        raise ValueError("race_source_value 없습니다.")
    
    if "birth_date" in kwargs:
        birth_date = kwargs["birth_date"]
    else :
        raise ValueError("birth_date 없습니다.")

    if "location_source_value" in kwargs:
        location_source_value = kwargs["location_source_value"]
    else :
        raise ValueError("location_source_value 없습니다.")
    
    print('원천 데이터 개수:', len(source))

    source[location_source_value] = source[location_source_value].str[:3]
    
    source = pd.merge(source, location_data, left_on = location_source_value, right_on="ZIP", how = "left")
    source.loc[source["LOCATION_ID"].isna(), "LOCATION_ID"] = 0

    source[birth_date] = pd.to_datetime(source[birth_date])

    # 사망일시 불러오기
    source = pd.merge(source, death_data, left_on=person_source_value, right_on=person_source_value, how="left")

    race_conditions = [
        source[race_source_value].isin(["한국인"]),
        source[race_source_value].isin(["외국인"])
    ]
    race_concept_id = [38003585, 8552]

    gender_conditions = [
        source[gender_source_value].isin(['남']),
        source[gender_source_value].isin(['여'])
    ]
    gender_concept_id = [8507, 8532]

    cdm = pd.DataFrame({
        "person_id" : source.index + 1,
        "gender_concept_id": np.select(gender_conditions, gender_concept_id, default = 0),
        "year_of_birth": source[birth_date].dt.year,
        "month_of_birth": source[birth_date].dt.month,
        "day_of_birth": source[birth_date].dt.day,
        "birth_datetime": pd.to_datetime(source[birth_date], format = "%Y%m%d", errors='coerce'),
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
    


transform_to_person( source_path = "F:\\01.감염병데이터베이스\\data\\dsmc\\emr",
                     CDM_path = "F:\\01.감염병데이터베이스\\data\\dsmc\\cdm",
                     source_data = "person.csv",
                     location_data = "location.csv",
                     death_data = "death.csv",
                     race_source_value = "외국인구분",
                     gender_source_value = "성별",
                     person_source_value = "환자번호",
                     death_datetime = "사망일시",
                     birth_date = "생년월일",
                     location_source_value = "우편번호"
)