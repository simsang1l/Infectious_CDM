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
    
    if "birth_resno1" in kwargs:
        birth_resno1 = kwargs["birth_resno1"]
    else :
        raise ValueError("birth_resno1 없습니다.")
    
    if "birth_resno2" in kwargs:
        birth_resno2 = kwargs["birth_resno2"]
    else :
        raise ValueError("birth_resno2 없습니다.")
    
    print('원천 데이터 개수:', len(source))

    source['ZIPCODE'] = source['ZIPCODE'].str[:3]
    source.loc[source[birth_resno2].str[:1].isin(['9', '0']), birth_resno1] = "18" + source[birth_resno1].astype(str)
    source.loc[source[birth_resno2].str[:1].isin(['1', '2', '5', '6']), birth_resno1] = "19" + source[birth_resno1].astype(str)
    source.loc[source[birth_resno2].str[:1].isin(['3', '4', '7', '8']), birth_resno1] = "20" + source[birth_resno1].astype(str)
    
    source = pd.merge(source, location_data, left_on = "ZIPCODE", right_on="ZIP", how = "left")
    source.loc[source["LOCATION_ID"].isna(), "LOCATION_ID"] = 0

    race_conditions = [
        source[birth_resno2].str[:1].isin(['0', '1', '2', '3', '4', '9']),
        source[birth_resno2].str[:1].isin(['5', '6', '7', '8'])
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
        "year_of_birth": source[birth_resno1].str[:4],
        "month_of_birth": source[birth_resno1].str[4:6],
        "day_of_birth": source[birth_resno1].str[6:8],
        "birth_datetime": pd.to_datetime(source[birth_resno1], format = "%Y%m%d", errors='coerce'),
        "death_datetime": pd.to_datetime(source[death_datetime], format = "%Y%m%d", errors='coerce'),
        "race_concept_id": np.select(race_conditions, race_concept_id, default = 0),
        "ethnicity_concept_id": 0,
        "location_id": source["LOCATION_ID"],
        "provider_id": 0,
        "care_site_id": 0, 
        "person_source_value": source[person_source_value],
        "gender_source_value": source[gender_source_value],
        "gender_source_concept_id": np.select(gender_conditions, gender_concept_id, default = 0),
        "race_source_value": source[birth_resno2].str[:1],
        "race_source_concept_id": 0,
        "ethnicity_source_value": None,
        "ethnicity_source_concept_id": 0
        })
    
    cdm["birth_datetime"] = cdm["birth_datetime"].dt.strftime('%Y-%m-%d %H:%M:%S')
    cdm["death_datetime"] = cdm["death_datetime"].dt.strftime('%Y-%m-%d %H:%M:%S')

    cdm.to_csv(os.path.join(CDM_path, "person.csv"), index=False)

    
    print('CDM 데이터 개수', len(cdm))
    


transform_to_person( source_path = "F:\\01.감염병데이터베이스\\data\\2023_infectious_DW"
                            , CDM_path = "F:\\01.감염병데이터베이스\\data\\2023_infectious_CDM"
                            , source_data = "ods_appatbat.csv"
                            , location_data = "location.csv"
                            , gender_source_value = "SEX"
                            , person_source_value = "PATNO"
                            , death_datetime = "DIEDATE"
                            , birth_resno1 = "RESNO1"
                            , birth_resno2 = "RESNO2"
)