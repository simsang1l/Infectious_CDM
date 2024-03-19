import pandas as pd
import os

def transform_to_observation_period(**kwargs):
    cdmpath = kwargs["CDM_path"]

    if "visit_data" in kwargs :
        visit_data = pd.read_csv(os.path.join(cdmpath, kwargs["visit_data"]), dtype=str)
    else :
        raise ValueError("visit_data 없습니다.")

    if "condition_data" in kwargs :
        condition_data = pd.read_csv(os.path.join(cdmpath, kwargs["condition_data"]), dtype=str)
    else :
        raise ValueError("condition_data 없습니다.")

    if "drug_data" in kwargs :
        drug_data = pd.read_csv(os.path.join(cdmpath, kwargs["drug_data"]), dtype=str)
    else :
        raise ValueError("drug_data 없습니다.")

    if "measurement_data" in kwargs :
        measurement_data = pd.read_csv(os.path.join(cdmpath, kwargs["measurement_data"]), dtype=str)
    else :
        raise ValueError("measurement_data 없습니다.")

    if "procedure_data" in kwargs :
        procedure_data = pd.read_csv(os.path.join(cdmpath, kwargs["procedure_data"]), dtype=str)
    else :
        raise ValueError("procedure_data 없습니다.")
        

    # 각 파일별 환자의 min, max date 구하기
    visit_data = visit_data[["person_id", "visit_start_date", "visit_end_date"]]
    visit_data["visit_start_date"] = pd.to_datetime(visit_data["visit_start_date"])
    # 2999-12-31과 같이 너무 먼 날짜 데이터 처리에 오류발생하여 null값으로 변경
    visit_data["visit_end_date"] = pd.to_datetime(visit_data["visit_end_date"], errors='coerce')
    # null값은 visit_start_date값이 들어가게 처리
    visit_data["visit_end_date"].fillna(visit_data["visit_start_date"])
    visit_data = visit_data.groupby("person_id").agg({"visit_start_date": "min"
                                                , "visit_end_date": "max"
    }).reset_index()
    visit_data.rename(columns = {"visit_start_date": "start_date",
                        "visit_end_date": "end_date"
    }, inplace = True)
    condition_data = condition_data[["person_id", "condition_start_date", "condition_end_date"]]
    condition_data["condition_start_date"] = pd.to_datetime(condition_data["condition_start_date"])
    condition_data["condition_end_date"] = pd.to_datetime(condition_data["condition_end_date"], errors='coerce')
    condition_data["condition_end_date"].fillna(condition_data["condition_start_date"])
    condition_data = condition_data.groupby("person_id").agg({"condition_start_date": "min"
                                                , "condition_end_date": "max"
    }).reset_index()
    condition_data.rename(columns = {"condition_start_date": "start_date",
                        "condition_end_date": "end_date"
    }, inplace = True)
    drug_data = drug_data[["person_id", "drug_exposure_start_date", "drug_exposure_end_date"]]
    drug_data["drug_exposure_start_date"] = pd.to_datetime(drug_data["drug_exposure_start_date"])
    drug_data["drug_exposure_end_date"] = pd.to_datetime(drug_data["drug_exposure_end_date"])
    drug_data = drug_data.groupby("person_id").agg({"drug_exposure_start_date": "min"
                                                , "drug_exposure_end_date": "max"
    }).reset_index()
    drug_data.rename(columns = {"drug_exposure_start_date": "start_date",
                        "drug_exposure_end_date": "end_date"
    }, inplace = True)

    measurement_data = measurement_data[["person_id", "measurement_date"]]
    measurement_data["measurement_date"] = pd.to_datetime(measurement_data["measurement_date"])
    measurement_data = measurement_data.groupby("person_id")["measurement_date"].agg([("start_date", "min"),("end_date", "max")]).reset_index()

    procedure_data = procedure_data[["person_id", "procedure_date"]]
    procedure_data["procedure_date"] = pd.to_datetime(procedure_data["procedure_date"])
    procedure_data = procedure_data.groupby("person_id")["procedure_date"].agg([("start_date", "min"),("end_date", "max")]).reset_index()
    # axis = 0을 통해 행으로 데이터 합치기, ignore_index = True를 통해 dataframe index재설정
    cdm = pd.concat([visit_data, condition_data, drug_data, measurement_data, procedure_data], axis = 0, ignore_index=True)
    cdm = cdm.groupby("person_id").agg({"start_date": "min", "end_date": "max"}).reset_index()


    cdm = pd.DataFrame({
        "observation_period_id": cdm.index + 1,
        "person_id": cdm["person_id"],
        "observation_period_start_date": cdm["start_date"],
        "observation_period_end_date": cdm["end_date"],
        "period_type_concept_id": 44814724
    })
    
    print(cdm.isnull().sum())
    print(f"병합된 cdm 데이터 수 : {len(cdm)}")
    cdm.to_csv(os.path.join(cdmpath, "observation_period.csv"), index = False)


transform_to_observation_period(CDM_path = "F:\\01.감염병데이터베이스\\data\\knuh\\cdm",
                                visit_data = "visit_occurrence.csv",
                                condition_data = 'condition_occurrence.csv',
                                drug_data = 'drug_exposure.csv',
                                measurement_data = 'measurement.csv',
                                procedure_data = 'procedure_occurrence.csv'
)