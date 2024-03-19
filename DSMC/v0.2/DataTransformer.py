import pandas as pd
import numpy as np
import yaml
import os
from datetime import datetime
import logging
import warnings
import inspect

class DataTransformer:
    """
    기본 데이터 변환 클래스.
    설정 파일을 로드하고, CSV 파일 읽기 및 쓰기를 담당합니다.
    """
    def __init__(self, config_path):
        self.config = self.load_config(config_path)
        self.setup_logging()
        warnings.showwarning = self.custom_warning_handler

        # 공통 변수 정의
        self.cdm_path = self.config["CDM_path"]
        self.person_data = self.config["person_data"]
        self.provider_data = self.config["provider_data"]
        self.care_site_data = self.config["care_site_data"]
        self.visit_data = self.config["visit_data"]
        self.visit_detail = self.config["visit_detail_data"]
        self.local_edi_data = self.config["local_edi_data"]
        self.source_flag = "source"
        self.cdm_flag = "CDM"
        self.source_dtype = self.config["source_dtype"]
        self.encoding = self.config["encoding"]
        self.person_source_value = self.config["person_source_value"]
        self.data_range = self.config["data_range"]
        self.target_zip = self.config["target_zip"]
        self.location_data = self.config["location_data"]
        self.concept_unit = self.config["concept_unit"]
        self.visit_source_key = self.config["visit_source_key"]
        self.ordcode = self.config["ordcode"]
        self.edicode = self.config["edicode"]
        self.fromdate = self.config["fromdate"]
        self.todate = self.config["todate"]
        self.hospital = self.config["hospital"]

    def load_config(self, config_path):
        """
        YAML 설정 파일을 로드합니다.
        """
        with open(config_path, 'r', encoding="utf-8") as file:
            return yaml.safe_load(file)
        
    def read_csv(self, file_name, path_type = 'source', encoding = 'utf-8', dtype = None):
        """
        CSV 파일을 읽어 DataFrame으로 반환합니다.
        path_type에 따라 'source' 또는 'CDM' 경로에서 파일을 읽습니다.
        """
        if path_type == "source":
            full_path = os.path.join(self.config["source_path"], file_name + ".csv")
        elif path_type == "CDM":
            full_path = os.path.join(self.config["CDM_path"], file_name + ".csv")
        else :
            raise ValueError(f"Invalid path type: {path_type}")
        
        return pd.read_csv(full_path, dtype = dtype, encoding = encoding)

    def write_csv(self, df, file_path):
        """
        DataFrame을 CSV 파일로 저장합니다.
        """
        df.to_csv(file_path + ".csv", index = False)

    def transform(self):
        """
        데이터 변환을 수행하는 메소드. 하위 클래스에서 구현해야 합니다.
        """
        raise NotImplementedError("This method should be implemented by subclasses.")
            
    def setup_logging(self):
        """
        실행 시 로그에 기록하는 메소드입니다.
        """
        log_path = "./log"
        os.makedirs(log_path, exist_ok = True)
        log_filename = datetime.now().strftime('log_%Y-%m-%d_%H%M%S.log')
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s\n'

        filename = os.path.join(log_path, log_filename)
        logging.basicConfig(filename = filename, level = logging.DEBUG, format = log_format, encoding = "utf-8")

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.ERROR)
        console_handler.setFormatter(logging.Formatter(log_format))
        logging.getLogger().addHandler(console_handler)

    def custom_warning_handler(self, message, category, filename, lineno, file=None, line=None):
        """
        실행 시 로그에 warning 항목을 기록하는 메소드입니다.
        """
        calling_frame = inspect.currentframe().f_back
        calling_code = calling_frame.f_code
        calling_function_name = calling_code.co_name
        logging.warning(f"{category.__name__} in {calling_function_name} (Line {lineno}): {message}")
                                 

class CareSiteTransformer(DataTransformer):
    def __init__(self, config_path):
        """
        CareSiteTransformer 클래스의 생성자.
        상위 클래스인 DataTransformer의 생성자를 호출하고,
        care_site 관련 설정을 로드합니다.
        """
        super().__init__(config_path)
        self.table = "care_site"
        self.cdm_config = self.config[self.table]

        # 출력 파일명, 소스 데이터, 그리고 각종 컬럼 설정을 불러옵니다.
        self.output_filename = self.cdm_config["data"]["output_filename"]
        self.source_data = self.cdm_config["data"]["source_data"]
        self.location_source_value = self.cdm_config["columns"]["location_source_value"]
        self.care_site_name = self.cdm_config["columns"]["care_site_name"]
        self.place_of_service_concept_id = self.cdm_config["columns"]["place_of_service_concept_id"]
        self.care_site_source_value = self.cdm_config["columns"]["care_site_source_value"]
        self.place_of_service_source_value = self.cdm_config["columns"]["place_of_service_source_value"]

    def transform(self):
        """
        소스 데이터를 읽어들여 CDM 형식으로 변환하고 결과를 CSV 파일로 저장하는 메소드.
        """
        try :
            # 소스 데이터 처리
            source_data, location = self.process_source()
            # 데이터 변환
            transformed_data = self.transform_cdm(source_data, location)
            # CSV 파일로 저장
            save_path = os.path.join(self.cdm_path, self.output_filename)
            self.write_csv(transformed_data, save_path)

            # 로그에 변환 완료 메시지 기록
            logging.info(f"{self.table} 테이블 변환 완료")
            logging.info(f"============================")

        except Exception as e :
            # 예외 발생 시 로그에 에러 메시지 기록
            logging.error(f"Error in transformation: {e}", exc_info = True)

    def process_source(self):
        """
        소스 데이터를 읽어들이고 전처리하는 메소드.
        원본 데이터와 위치 데이터를 CSV 파일로부터 읽어들입니다.
        """
        try:
            source_data = self.read_csv(self.source_data, path_type = self.source_flag, dtype = self.source_dtype, encoding = self.encoding)
            logging.debug(f"원천 데이터 row수: {len(source_data)}")

            location = self.read_csv(self.location_data, path_type = self.cdm_flag, dtype = self.source_dtype, encoding = self.encoding)
            return source_data, location

        except Exception as e:
            logging.error(f"소스 데이터 처리 중 오류 발생: {e}", exc_info=True)
            raise

    def transform_cdm(self, source_data, location):
        """
        원본 데이터를 CDM 형식으로 변환하는 메소드.
        변환된 데이터는 새로운 DataFrame으로 구성됩니다.
        """
        try:
            cdm = pd.DataFrame({
                "care_site_id" : source_data.index + 1,
                "care_site_name": source_data[self.care_site_name],
                "place_of_service_concept_id": self.place_of_service_concept_id,            
                "location_id": location.loc[location[self.location_source_value] == self.target_zip, "LOCATION_ID"].tolist()[0],
                "care_site_source_value": source_data[self.care_site_source_value],
                "place_of_service_source_value": source_data[self.place_of_service_source_value] 
            })

            logging.debug(f"CDM 데이터 row수: {len(cdm)}")
            logging.debug(f"요약(문자형_data):\n{cdm.describe(include = 'O').T.to_string()}")
            logging.debug(f"요약:\n{cdm.describe().T.to_string()}")
            logging.debug(f"컬럼별 null 개수:\n{cdm.isnull().sum().to_string()}")
            
            return cdm

        except Exception as e :
            logging.error(f"{self.table} 테이블 CDM 데이터 변환 중 오류:\n {e}", exc_info = True)
            
        
class ProviderTransformer(DataTransformer):
    def __init__(self, config_path):
        super().__init__(config_path)
        self.table = "provider"
        self.cdm_config = self.config[self.table]

        self.source_data = self.cdm_config["data"]["source_data"]
        self.care_site_data = self.config["care_site_data"]
        self.output_filename = self.cdm_config["data"]["output_filename"]
        self.specialty_source_value = self.cdm_config["columns"]["specialty_source_value"]
        self.provider_name = self.cdm_config["columns"]["provider_name"]
        self.gender_source_value = self.cdm_config["columns"]["gender_source_value"]
        self.provider_source_value = self.cdm_config["columns"]["provider_source_value"]

    def transform(self):
        """
        소스 데이터를 읽어들여 CDM 형식으로 변환하고 결과를 CSV 파일로 저장하는 메소드.
        """
        try:
            source_data = self.process_source()
            transformed_data = self.transform_cdm(source_data)
            save_path = os.path.join(self.cdm_path, self.output_filename)
            self.write_csv(transformed_data, save_path)

            logging.info(f"{self.table} 테이블 변환 완료")
            logging.info(f"============================")

        except Exception as e :
            logging.error(f"{self.table} 테이블 변환 중 오류: {e}", exc_info=True)

    def process_source(self):
        """
        소스 데이터와 care site 데이터를 읽어들이고 병합하는 메소드.
        """
        try :
            source_data = self.read_csv(self.source_data, path_type = self.source_flag, dtype = self.source_dtype, encoding = self.encoding)
            logging.debug(f"원천 데이터 row수: {len(source_data)}")
        
            return source_data

        except Exception as e :
            logging.error(f"{self.table} 테이블 원천 데이터 처리 중 오류:\n {e}", exc_info=True)

    def transform_cdm(self, source_data):
        """
        주어진 소스 데이터를 CDM 형식에 맞게 변환하는 메소드.
        변환된 데이터는 새로운 DataFrame으로 구성됩니다.
        """
        try :
            gender_conditions = [
                source_data[self.gender_source_value].isin(['M']),
                source_data[self.gender_source_value].isin(['F'])
            ]
            gender_concept_id = [8507, 8532]  

            cdm = pd.DataFrame({
                "provider_id" : source_data.index + 1,
                "provider_name": self.provider_name,
                "npi": None,
                "dea": None,
                "specialty_concept_id": 0,
                "care_site_id": None,
                "year_of_birth": None,
                "gender_concept_id": 0,
                "provider_source_value": source_data[self.provider_source_value],
                "specialty_source_value": source_data[self.specialty_source_value],
                "specialty_source_concept_id": 0,
                "gender_source_value": self.gender_source_value,
                "gender_source_concept_id": np.select(gender_conditions, gender_concept_id, default = 0),
                })

            logging.debug(f"CDM 데이터 row수: {len(cdm)}")
            logging.debug(f"요약(문자형_data):\n{cdm.describe(include = 'O').T.to_string()}")
            logging.debug(f"요약:\n{cdm.describe().T.to_string()}")
            logging.debug(f"컬럼별 null 개수:\n{cdm.isnull().sum().to_string()}")

            return cdm   

        except Exception as e :
            logging.error(f"{self.table} 테이블 CDM 데이터 변환 중 오류: {e}", exc_info = True)

         
class PersonTransformer(DataTransformer):
    def __init__(self, config_path):
        super().__init__(config_path)
        self.table = "person"
        self.cdm_config = self.config[self.table]

        self.source_data = self.cdm_config["data"]["source_data"]
        self.output_filename = self.cdm_config["data"]["output_filename"]
        self.death_data = self.cdm_config["data"]["death_data"]
        self.location_source_value = self.cdm_config["columns"]["location_source_value"]
        self.race_source_value = self.cdm_config["columns"]["race_source_value"]
        self.gender_source_value = self.cdm_config["columns"]["gender_source_value"]
        self.death_datetime = self.cdm_config["columns"]["death_datetime"]
        self.birth_date = self.cdm_config["columns"]["birth_date"]

    def transform(self):
        """
        소스 데이터를 읽어들여 CDM 형식으로 변환하고 결과를 CSV 파일로 저장하는 메소드.
        """
        try:
            source_data = self.process_source()
            transformed_data = self.transform_cdm(source_data)

            save_path = os.path.join(self.cdm_path, self.output_filename)
            self.write_csv(transformed_data, save_path)

            logging.info(f"{self.table} 테이블 변환 완료")
            logging.info(f"============================")

        except Exception as e:
            logging.error(f"{self.table} 테이블 변환 중 오류:\n {e}", exc_info=True)
            raise

    def process_source(self):
        """
        소스 데이터와 care site 데이터를 읽어들이고 병합하는 메소드.
        """
        try:
            source_data = self.read_csv(self.source_data, path_type = self.source_flag, dtype = self.source_dtype)
            location_data = self.read_csv(self.location_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            death_data = self.read_csv(self.death_data, path_type = self.source_flag, dtype = self.source_dtype)
            logging.debug(f"원천 데이터 row수: {len(source_data)}, death: {len(death_data)}")

            source_data[self.location_source_value] = source_data[self.location_source_value].str[:3]
            
            source_data = pd.merge(source_data, location_data, left_on = self.location_source_value, right_on="LOCATION_SOURCE_VALUE", how = "left")
            source_data.loc[source_data["LOCATION_ID"].isna(), "LOCATION_ID"] = 0
            logging.debug(f"location 테이블과 결합 후 원천 데이터1 row수: {len(source_data)}")

            source_data[self.birth_date] = pd.to_datetime(source_data[self.birth_date])
            death_data[self.death_datetime] = pd.to_datetime(death_data[self.death_datetime])
            source_data = pd.merge(source_data, death_data, left_on=self.person_source_value, right_on = self.person_source_value, how = "left")
            logging.debug(f"death 테이블과 결합 후 원천 데이터1 row수: {len(source_data)}")

            logging.debug(f"CDM테이블과 결합 후 원천 데이터 row수: source: {len(source_data)}")

            return source_data
        
        except Exception as e :
            logging.error(f"{self.table} 테이블 소스 데이터 처리 중 오류: {e}", exc_info=True)
            raise

    def transform_cdm(self, source):
        """
        주어진 소스 데이터를 CDM 형식에 맞게 변환하는 메소드.
        변환된 데이터는 새로운 DataFrame으로 구성됩니다.
        """
        try : 
            race_conditions = [
                source[self.race_source_value].isin(["한국인"]),
                source[self.race_source_value].isin(["외국인"])
            ]
            race_concept_id = [38003585, 8552]

            gender_conditions = [
                source[self.gender_source_value].isin(['남']),
                source[self.gender_source_value].isin(['여'])
            ]
            gender_concept_id = [8507, 8532]

            cdm = pd.DataFrame({
                "person_id" : source.index + 1,
                "gender_concept_id": np.select(gender_conditions, gender_concept_id, default = 0),
                "year_of_birth": source[self.birth_date].dt.year,
                "month_of_birth": source[self.birth_date].dt.month,
                "day_of_birth": source[self.birth_date].dt.day,
                "birth_datetime": pd.to_datetime(source[self.birth_date], format = "%Y%m%d", errors='coerce'),
                "death_datetime": pd.to_datetime(source[self.death_datetime], format = "%Y%m%d", errors='coerce'),
                "race_concept_id": np.select(race_conditions, race_concept_id, default = 0),
                "ethnicity_concept_id": 0,
                "location_id": source["LOCATION_ID"],
                "provider_id": 0,
                "care_site_id": 0, 
                "person_source_value": source[self.person_source_value],
                "gender_source_value": source[self.gender_source_value],
                "gender_source_concept_id": np.select(gender_conditions, gender_concept_id, default = 0),
                "race_source_value": source[self.race_source_value],
                "race_source_concept_id": 0,
                "ethnicity_source_value": None,
                "ethnicity_source_concept_id": 0
                })
            
            cdm["birth_datetime"] = cdm["birth_datetime"].dt.strftime('%Y-%m-%d %H:%M:%S')
            cdm["death_datetime"] = cdm["death_datetime"].dt.strftime('%Y-%m-%d %H:%M:%S')

            logging.debug(f"CDM 데이터 row수: {len(cdm)}")
            logging.debug(f"요약(문자형_data):\n{cdm.describe(include = 'O').T.to_string()}")
            logging.debug(f"요약:\n{cdm.describe().T.to_string()}")
            logging.debug(f"컬럼별 null 개수:\n{cdm.isnull().sum().to_string()}")

            return cdm   

        except Exception as e :
            logging.error(f"{self.table} 테이블 CDM 데이터 변환 중 오류: {e}", exc_info = True)


class VisitOccurrenceTransformer(DataTransformer):
    def __init__(self, config_path):
        super().__init__(config_path)
        self.table = "visit_occurrence"
        self.cdm_config = self.config[self.table]

        # CDM 컬럼 정의
        self.columns = ["visit_occurrence_id", "person_id", "visit_concept_id", "visit_start_date", "visit_start_datetime"
                    , "visit_end_date", "visit_end_datetime", "visit_type_concept_id", "provider_id"
                    , "care_site_id", "visit_source_value", "visit_source_concept_id", "admitted_from_concept_id"
                    , "admitted_from_source_value", "discharge_to_source_value", "discharge_to_concept_id"
                    , "preceding_visit_occurrence_id", "visit_source_key"]

        # 컬럼 변수 재정의
        self.source_data = self.cdm_config["data"]["source_data"]
        self.output_filename = self.cdm_config["data"]["output_filename"]

        self.admitted_from_source_value = self.cdm_config["columns"]["admitted_from_source_value"]
        self.discharge_to_source_value = self.cdm_config["columns"]["discharge_to_source_value"]
        self.visit_source_value = self.cdm_config["columns"]["visit_source_value"]
        self.visit_start_datetime = self.cdm_config["columns"]["visit_start_datetime"]
        self.visit_end_datetime = self.cdm_config["columns"]["visit_end_datetime"]
        

    def transform(self):
        """
        소스 데이터를 읽어들여 CDM 형식으로 변환하고 결과를 CSV 파일로 저장하는 메소드입니다.
        """
        try : 
            source_data = self.process_source()
            transformed_data = self.transform_cdm(source_data)

            save_path = os.path.join(self.config["CDM_path"], self.output_filename)
            self.write_csv(transformed_data, save_path)

            logging.info(f"{self.table} 테이블 변환 완료")
            logging.info(f"============================")

        except Exception as e :
            logging.error(f"{self.table} 테이블 변환 중 오류:\n {e}", exc_info=True)
            raise

    def process_source(self):
        """
        소스 데이터를 로드하고 전처리 작업을 수행하는 메소드입니다.
        여기서는 방문 시간과 관련된 데이터를 처리합니다.
        """
        try : 
            # 원천 및 CDM 데이터 불러오기
            source = self.read_csv(self.source_data, path_type = self.source_flag , dtype = self.source_dtype)
            person_data = self.read_csv(self.person_data, path_type = self.cdm_flag , dtype = self.source_dtype)
            logging.debug(f"원천 데이터 row수: source: {len(source)}")

            # 원천 데이터 범위 설정
            source[self.visit_start_datetime] = pd.to_datetime(source[self.visit_start_datetime], errors = "coerce")
            source[self.visit_end_datetime] = pd.to_datetime(source[self.visit_end_datetime], errors = "coerce")
            source = source[source[self.visit_start_datetime] <= self.data_range]
            logging.debug(f"데이터 범위 조건 적용 후 원천 데이터 row수: {len(source)}")

            # 불러온 원천 전처리
            source = pd.merge(source, person_data, left_on = self.person_source_value, right_on="person_source_value", how="inner")
            source = source.drop(columns = ["care_site_id", "provider_id"])
            logging.debug(f"person 테이블과 결합 후 원천 데이터1 row수: {len(source)}")

            return source

        except Exception as e :
            logging.error(f"{self.table} 테이블 소스 데이터 처리 중 오류: {e}", exc_info = True)

    def transform_cdm(self, source):
        """
        주어진 소스 데이터를 CDM 형식에 맞게 변환하는 메소드.
        변환된 데이터는 새로운 DataFrame으로 구성됩니다.
        """
        try : 
            visit_condition = [
                source[self.visit_source_value] == "외래",
                source[self.visit_source_value] == "입원",
                source[self.visit_source_value] == "응급"
            ]
            visit_concept_id = [9202, 9201, 9203]

            cdm = pd.DataFrame({
                "person_id": source["person_id"],
                "visit_concept_id": np.select(visit_condition, visit_concept_id, default = 0),
                "visit_start_date": source[self.visit_start_datetime].dt.date ,
                "visit_start_datetime": source[self.visit_start_datetime],
                "visit_end_date": source[self.visit_end_datetime].dt.date,
                "visit_end_datetime": source[self.visit_end_datetime],
                "visit_type_concept_id": 44818519,
                "provider_id": None,
                "care_site_id": None,
                "visit_source_value": source[self.visit_source_value],
                "visit_source_concept_id": np.select(visit_condition, visit_concept_id, default = 0),
                "admitted_from_concept_id": None,
                "admitted_from_source_value": source[self.admitted_from_source_value],
                "discharge_to_concept_id": 0,
                "discharge_to_source_value": source[self.discharge_to_source_value],
                "visit_source_key": source[self.visit_source_key]
                })
            
            cdm.reset_index(drop=True, inplace = True)
            cdm["visit_occurrence_id"] = cdm.index + 1
            cdm.sort_values(by=["person_id", "visit_start_datetime"], inplace = True)
            cdm["preceding_visit_occurrence_id"] = cdm.groupby("person_id")["visit_occurrence_id"].shift(1)
            cdm["preceding_visit_occurrence_id"] = cdm["preceding_visit_occurrence_id"].apply(lambda x : x if pd.isna(x) else str(int(x)))

            cdm = cdm[self.columns]
            
            logging.debug(f"CDM 데이터 row수: {len(cdm)}")
            logging.debug(f"요약(문자형_data):\n{cdm.describe(include = 'O').T.to_string()}")
            logging.debug(f"요약:\n{cdm.describe().T.to_string()}")
            logging.debug(f"컬럼별 null 개수:\n{cdm.isnull().sum().to_string()}")

            return cdm   

        except Exception as e :
            logging.error(f"{self.table} 테이블 CDM 데이터 변환 중 오류: {e}", exc_info = True)


class VisitDetailTransformer(DataTransformer):
    """
    ICU 방문 발생 정보를 변환하는 클래스.
    """
    def __init__(self, config_path):
        super().__init__(config_path)
        self.table = "visit_detail"
        self.cdm_config = self.config[self.table]

        # CDM 컬럼 정의
        self.columns = ["visit_detail_id", "person_id", "visit_detail_concept_id", "visit_detail_start_date"
                        , "visit_detail_start_datetime", "visit_detail_end_date", "visit_detail_end_datetime"
                        , "visit_detail_type_concept_id", "provider_id", "care_site_id", "visit_detail_source_value"
                        , "visit_detail_source_concept_id", "admitted_from_concept_id", "admitted_from_source_value"
                        , "discharge_to_source_value", "discharge_to_concept_id", "preceding_visit_detail_id"
                        , "visit_detail_parent_id", "visit_occurrence_id", "visit_source_key"]

        # 컬럼 변수 재정의    
        self.source_data = self.cdm_config["data"]["source_data"]
        self.output_filename = self.cdm_config["data"]["output_filename"]
        self.visit_detail_start_datetime = self.cdm_config["columns"]["visit_detail_start_datetime"]
        self.visit_detail_end_datetime = self.cdm_config["columns"]["visit_detail_end_datetime"]
        self.visit_detail_source_value = self.cdm_config["columns"]["visit_detail_source_value"]
        self.admitted_from_source_value = self.cdm_config["columns"]["admitted_from_source_value"]
        self.discharge_to_source_value = self.cdm_config["columns"]["discharge_to_source_value"]
        
        
    def transform(self):
        """
        소스 데이터를 읽어들여 CDM 형식으로 변환하고 결과를 CSV 파일로 저장하는 메소드입니다.
        """
        try : 
            source_data = self.process_source()
            transformed_data = self.transform_cdm(source_data)


            save_path = os.path.join(self.cdm_path, self.output_filename)
            self.write_csv(transformed_data, save_path)

            logging.info(f"{self.table} 테이블 변환 완료")
            logging.info(f"============================")
        
        except Exception as e :
            logging.error(f"{self.table} 테이블 변환 중 오류:\n {e}", exc_info=True)
            raise

    def process_source(self):
        """
        소스 데이터를 로드하고 전처리 작업을 수행하는 메소드입니다.
        """
        try :
            source = self.read_csv(self.source_data, path_type = self.source_flag, dtype = self.source_dtype)
            person_data = self.read_csv(self.person_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_data = self.read_csv(self.visit_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            logging.debug(f"원천 데이터 row수: {len(source)}")

            # 원천에서 조건걸기
            source[self.visit_detail_start_datetime] = pd.to_datetime(source[self.visit_detail_start_datetime])
            source[self.visit_detail_end_datetime] = pd.to_datetime(source[self.visit_detail_end_datetime], errors = "coerce")
            source = source[source[self.visit_detail_start_datetime] <= self.data_range]
            logging.debug(f"조건 적용 후 원천 데이터 row수: {len(source)}")

            # person table과 병합
            source = pd.merge(source, person_data, left_on=self.person_source_value, right_on="person_source_value", how="inner")
            source = source.drop(columns = ["care_site_id", "provider_id"])
            logging.debug(f"person 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            # visit_occurrence table과 병합
            source = pd.merge(source, visit_data, left_on=["person_id", self.visit_source_key], right_on=["person_id", "visit_source_key"], how="left", suffixes=('', '_y'))
            logging.debug(f"visit_occurrence 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            source = source[(source[self.visit_detail_start_datetime] >= source["visit_start_datetime"]) & (source[self.visit_detail_start_datetime] <= source["visit_end_datetime"])]
            source.loc[source["care_site_id"].isna(), "care_site_id"] = 0
            source[self.visit_detail_end_datetime] = pd.to_datetime(source[self.visit_detail_end_datetime], errors="coerce")
            logging.debug(f"CDM 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            return source
        
        except Exception as e :
            logging.error(f"{self.table} 테이블 소스 데이터 처리 중 오류: {e}", exc_info = True)

    def transform_cdm(self, source):
        """
        주어진 소스 데이터를 CDM 형식에 맞게 변환하는 메소드.
        변환된 데이터는 새로운 DataFrame으로 구성됩니다.
        """
        try : 
            cdm = pd.DataFrame({
                "person_id": source["person_id"],
                "visit_detail_concept_id": 32037,
                "visit_detail_start_date": source[self.visit_detail_start_datetime].dt.date,
                "visit_detail_start_datetime": source[self.visit_detail_start_datetime],
                "visit_detail_end_date": source[self.visit_detail_end_datetime].dt.date,
                "visit_detail_end_datetime": source[self.visit_detail_end_datetime],
                "visit_detail_type_concept_id": 44818518,
                "provider_id": None,
                "care_site_id": None,
                "visit_detail_source_value": self.visit_detail_source_value,
                "visit_detail_source_concept_id": 0,
                "admitted_from_concept_id": 0,
                "admitted_from_source_value": self.admitted_from_source_value,
                "discharge_to_source_value": self.discharge_to_source_value,
                "discharge_to_concept_id": 0,
                "visit_detail_parent_id": 0,
                "visit_occurrence_id": source["visit_occurrence_id"],
                "visit_source_key": source[self.visit_source_key]
                })
            
            # 컬럼 생성
            cdm.reset_index(drop = True, inplace = True)
            cdm["visit_detail_id"] = cdm.index + 1
            cdm.sort_values(by=["person_id", "visit_detail_start_datetime"], inplace = True)
            cdm["preceding_visit_detail_id"] = cdm.groupby("person_id")["visit_detail_id"].shift(1)
            cdm["preceding_visit_detail_id"] = cdm["preceding_visit_detail_id"].apply(lambda x : str(int(x)) if not pd.isna(x) else x)

            cdm = cdm[self.columns]

            logging.debug(f"CDM 데이터 row수: {len(cdm)}")
            logging.debug(f"요약(문자형_data):\n{cdm.describe(include = 'O').T.to_string()}")
            logging.debug(f"요약:\n{cdm.describe().T.to_string()}")
            logging.debug(f"컬럼별 null 개수:\n{cdm.isnull().sum().to_string()}")

            return cdm   

        except Exception as e :
            logging.error(f"{self.table} 테이블 CDM 데이터 변환 중 오류: {e}", exc_info = True)


class ConditionOccurrenceTransformer(DataTransformer):
    def __init__(self, config_path):
        super().__init__(config_path)
        self.table = "condition_occurrence"
        self.cdm_config = self.config[self.table]

        # 컬럼 변수 재정의     
        self.source_data = self.cdm_config["data"]["source_data"]
        self.output_filename = self.cdm_config["data"]["output_filename"]
        self.condition_start_datetime = self.cdm_config["columns"]["condition_start_datetime"]
        self.condition_type = self.cdm_config["columns"]["condition_type"]
        self.condition_source_value = self.cdm_config["columns"]["condition_source_value"]
        self.condition_status_source_value = self.cdm_config["columns"]["condition_status_source_value"]
        self.visit_source_key = self.cdm_config["columns"]["visit_source_key"]
        
    def transform(self):
        """
        소스 데이터를 읽어들여 CDM 형식으로 변환하고 결과를 CSV 파일로 저장하는 메소드입니다.
        """
        try:
            source_data = self.process_source()
            logging.info(f"{self.table} 테이블: {len(source_data)}건")
            transformed_data = self.transform_cdm(source_data)


            save_path = os.path.join(self.cdm_path, self.output_filename)
            self.write_csv(transformed_data, save_path)

            logging.info(f"{self.table} 테이블 변환 완료")
            logging.info(f"============================")
        
        except Exception as e :
            logging.error(f"{self.table} 테이블 변환 중 오류:\n {e}", exc_info=True)
            raise

    def process_source(self):
        """
        소스 데이터를 로드하고 전처리 작업을 수행하는 메소드입니다.
        """
        try: 
            source = self.read_csv(self.source_data, path_type = self.source_flag, dtype = self.source_dtype)
            person_data = self.read_csv(self.person_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_data = self.read_csv(self.visit_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_detail = self.read_csv(self.visit_detail, path_type = self.cdm_flag, dtype = self.source_dtype)
            logging.debug(f"원천 데이터 row수: {len(source)}")

            # 원천에서 조건걸기
            source[self.condition_start_datetime] = pd.to_datetime(source[self.condition_start_datetime])
            source = source[source[self.condition_start_datetime] <= self.data_range]
            logging.debug(f"조건 적용후 원천 데이터 row수: {len(source)}")

            # person table과 병합
            source = pd.merge(source, person_data, left_on=self.person_source_value, right_on="person_source_value", how="inner")
            source = source.drop(columns = ["care_site_id", "provider_id"])
            logging.debug(f"person 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            # visit_start_datetime 형태 변경
            visit_data["visit_start_datetime"] = pd.to_datetime(visit_data["visit_start_datetime"], errors = "coerce")
            visit_data["visit_end_datetime"] = pd.to_datetime(visit_data["visit_end_datetime"], errors = "coerce")
            # visit_occurrence table과 병합
            source = pd.merge(source, visit_data, left_on=["person_id", self.visit_source_key], right_on=["person_id", "visit_source_key"], how="left", suffixes=('', '_y'))
            logging.debug(f"visit_occurrence 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            # visit_detail table과 병합
            source = pd.merge(source, visit_detail, left_on=["visit_occurrence_id"], right_on=["visit_occurrence_id"], how="left", suffixes=('', '_y'))
            logging.debug(f"visit_detail 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            # care_site_id가 없는 경우 0으로 값 입력
            source.loc[source["care_site_id"].isna(), "care_site_id"] = 0

            logging.debug(f"CDM테이블과 결합 후 원천 데이터 row수: {len(source)}")

            return source

        except Exception as e :
            logging.error(f"{self.table} 테이블 소스 데이터 처리 중 오류: {e}", exc_info = True)
    

    def transform_cdm(self, source):
        """
        주어진 소스 데이터를 CDM 형식에 맞게 변환하는 메소드.
        변환된 데이터는 새로운 DataFrame으로 구성됩니다.
        """
        try : 
            type_condition = [
                source[self.condition_type] == "Y",
                source[self.condition_type] == "N"
            ]
            type_id = [44786627, 44786629]

            status_condition = [
                source[self.condition_status_source_value] == "Y",
                source[self.condition_status_source_value] == "N"
            ]
            status_id = [4230359, 4033240] 

            cdm = pd.DataFrame({
                "condition_occurrence_id": source.index + 1,
                "person_id": source["person_id"],
                "condition_concept_id": 0,
                "condition_start_date": source[self.condition_start_datetime].dt.date,
                "condition_start_datetime": source[self.condition_start_datetime],
                "condition_end_date": source["visit_end_datetime"].dt.date,
                "condition_end_datetime": source["visit_end_datetime"],
                "condition_type_concept_id": np.select(type_condition, type_id, default = 0),
                "condition_status_concept_id": np.select(status_condition, status_id, default = 0),
                "stop_reason": None,
                "provider_id": None,
                "visit_occurrence_id": source["visit_occurrence_id"],
                "visit_detail_id": source["visit_detail_id"],
                "condition_source_value": source[self.condition_source_value],
                "condition_source_concept_id": 0,
                "condition_status_source_value": source[self.condition_status_source_value],
                "visit_source_key": source[self.visit_source_key]
                })

            logging.debug(f"CDM 데이터 row수: {len(cdm)}")
            logging.debug(f"요약:\n{cdm.describe(include = 'O').T.to_string()}")
            logging.debug(f"요약:\n{cdm.describe().T.to_string()}")
            logging.debug(f"컬럼별 null 개수:\n{cdm.isnull().sum().to_string()}")

            return cdm   

        except Exception as e :
            logging.error(f"{self.table} 테이블 CDM 데이터 변환 중 오류: {e}", exc_info = True)


class LocalEDITransformer(DataTransformer):
    def __init__(self, config_path):
        super().__init__(config_path)
        self.table = "local_edi"
        self.cdm_config = self.config[self.table]

        # 컬럼 변수 재정의      
        self.order_data = self.cdm_config["data"]["order_data"]
        self.concept_data = self.cdm_config["data"]["concept_data"]
        self.output_filename = self.cdm_config["data"]["output_filename"]

        self.ordercode = self.cdm_config["columns"]["ordercode"]
        self.edicode = self.cdm_config["columns"]["edicode"]
        self.fromdate = self.cdm_config["columns"]["fromdate"]
        self.todate = self.cdm_config["columns"]["todate"]
        
    def transform(self):
        """
        소스 데이터를 읽어들여 CDM 형식으로 변환하고 결과를 CSV 파일로 저장하는 메소드입니다.
        """
        try:
            transformed_data = self.process_source()

            save_path = os.path.join(self.cdm_path, self.output_filename)
            self.write_csv(transformed_data, save_path)

            logging.info(f"{self.table} 테이블 변환 완료")
            logging.info(f"============================")
        
        except Exception as e :
            logging.error(f"{self.table} 테이블 변환 중 오류:\n {e}", exc_info=True)
            raise

    def process_source(self):
        """
        소스 데이터를 로드하고 전처리 작업을 수행하는 메소드입니다.
        """
        try : 
            source = self.read_csv(self.order_data, path_type = self.source_flag, dtype = self.source_dtype)
            concept_data = self.read_csv(self.concept_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            logging.debug(f'원천 데이터 row수: order: {len(source)}')

            # concept_id 매핑
            source = pd.merge(source, concept_data, left_on=self.edicode, right_on="concept_code", how="left")
            logging.debug(f'concept merge후 데이터 row수: {len(source)}')
            
            # drug의 경우 KCD, EDI 순으로 매핑
            source = source.sort_values(by = [self.ordercode, self.fromdate, "vocabulary_id"], ascending=[True, True, False])
            source['Sequence'] = source.groupby([self.ordercode, self.fromdate]).cumcount() + 1
            source = source[source["Sequence"] == 1]
            logging.debug(f'중복되는 concept_id 제거 후 데이터 row수: {len(source)}')

            local_edi = source
        
            logging.debug(f'local_edi row수: {len(local_edi)}')
            logging.debug(f"요약:\n{local_edi.describe(include = 'O').T.to_string()}")
            logging.debug(f"요약:\n{local_edi.describe().T.to_string()}")
            logging.debug(f"컬럼별 null 개수:\n{local_edi.isnull().sum().to_string()}")

            return local_edi

        except Exception as e :
            logging.error(f"{self.table} 테이블 소스 데이터 처리 중 오류:\n {e}", exc_info = True)


class DrugexposureTransformer(DataTransformer):
    def __init__(self, config_path):
        super().__init__(config_path)
        self.table = "drug_exposure"
        self.cdm_config = self.config[self.table]

        # 컬럼 변수 재정의     
        self.source_data = self.cdm_config["data"]["source_data"]
        self.output_filename = self.cdm_config["data"]["output_filename"]
        self.drug_exposure_start_datetime = self.cdm_config["columns"]["drug_exposure_start_datetime"]
        self.drug_source_value = self.cdm_config["columns"]["drug_source_value"]
        self.days_supply = self.cdm_config["columns"]["days_supply"]
        self.qty = self.cdm_config["columns"]["qty"]
        self.cnt = self.cdm_config["columns"]["cnt"]
        self.route_source_value = self.cdm_config["columns"]["route_source_value"]
        self.dose_unit_source_value = self.cdm_config["columns"]["dose_unit_source_value"]

        
    def transform(self):
        """
        소스 데이터를 읽어들여 CDM 형식으로 변환하고 결과를 CSV 파일로 저장하는 메소드입니다.
        """
        try:
            source_data = self.process_source()
            transformed_data = self.transform_cdm(source_data)


            save_path = os.path.join(self.cdm_path, self.output_filename)
            self.write_csv(transformed_data, save_path)

            logging.info(f"{self.table} 테이블 변환 완료")
            logging.info(f"============================")

        except Exception as e :
            logging.error(f"{self.table} 테이블 변환 중 오류:\n {e}", exc_info=True)
            raise

    def process_source(self):
        """
        소스 데이터를 로드하고 전처리 작업을 수행하는 메소드입니다.
        """
        try : 
            source = self.read_csv(self.source_data, path_type = self.source_flag, dtype = self.source_dtype, encoding = self.encoding)
            local_edi = self.read_csv(self.local_edi_data, path_type = self.cdm_flag, dtype = self.source_dtype, encoding = self.encoding)
            person_data = self.read_csv(self.person_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_data = self.read_csv(self.visit_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_detail = self.read_csv(self.visit_detail, path_type = self.cdm_flag, dtype = self.source_dtype)

            person_data = person_data[["person_id", "person_source_value"]]
            visit_data = visit_data[["visit_occurrence_id", "visit_start_datetime", "care_site_id", "visit_source_value", "person_id", "visit_source_key"]]
            visit_detail = visit_detail[["visit_detail_id", "visit_occurrence_id"]]
            logging.info(f"원천 데이터 row수:, {len(source)}")

            # 원천에서 조건걸기
            source[self.drug_exposure_start_datetime] = pd.to_datetime(source[self.drug_exposure_start_datetime])
            source = source[(source[self.drug_exposure_start_datetime] <= self.data_range)]
            source = source[[self.person_source_value, self.drug_source_value, self.drug_exposure_start_datetime,
                             self.days_supply, self.qty, self.cnt, self.route_source_value, self.dose_unit_source_value,
                             self.visit_source_key, self.hospital]]
            logging.info(f"조건 적용후 원천 데이터 row수:, {len(source)}")

            local_edi = local_edi[[self.ordcode, self.fromdate, self.todate, self.edicode, "concept_id", "병원구분코드"]]
            local_edi[self.fromdate] = pd.to_datetime(local_edi[self.fromdate] , format="%Y%m%d", errors="coerce")
            local_edi[self.fromdate].fillna(pd.Timestamp('1900-01-01'), inplace = True)
            local_edi[self.todate] = pd.to_datetime(local_edi[self.todate] , format="%Y%m%d", errors="coerce")
            local_edi[self.todate].fillna(pd.Timestamp('2099-12-31'), inplace = True)

            # LOCAL코드와 EDI코드 매핑 테이블과 병합
            source = pd.merge(source, local_edi, left_on=[self.drug_source_value, self.hospital], right_on=[self.ordcode, "병원구분코드"], how="inner")
            logging.info(f"local_edi와 병합 후 데이터 row수:, {len(source)}")
            source = source[(source[self.drug_exposure_start_datetime] >= source[self.fromdate]) & (source[self.drug_exposure_start_datetime] <= source[self.todate])]
            logging.info(f"local_edi날짜 조건 적용 후 데이터 row수: {len(source)}")

            # person table과 병합
            source = pd.merge(source, person_data, left_on=self.person_source_value, right_on="person_source_value", how="inner")
            logging.info(f"person 테이블과 결합 후 데이터 row수: {len(source)}")

            # visit_start_datetime 형태 변경
            visit_data["visit_start_datetime"] = pd.to_datetime(visit_data["visit_start_datetime"])
            # visit_occurrence table과 병합
            source = pd.merge(source, visit_data, left_on=["person_id", self.visit_source_key], right_on=["person_id", "visit_source_key"], how="left", suffixes=('', '_y'))
            logging.info(f"visit_occurrence 테이블과 결합 후 데이터 row수: {len(source)}")

            # visit_detail table과 병합
            source = pd.merge(source, visit_detail, left_on=["visit_occurrence_id"], right_on=["visit_occurrence_id"], how="left", suffixes=('', '_y'))
            logging.info(f"visit_detail 테이블과 결합 후 데이터 row수: {len(source)}")

            source.loc[source["concept_id"].isna(), "concept_id"] = 0
            logging.info(f"CDM테이블과 병합 후 데이터 row수: {len(source)}")

            return source
        
        except Exception as e :
            logging.error(f"{self.table} 테이블 소스 데이터 처리 중 오류: {e}", exc_info = True)

    def transform_cdm(self, source):
        """
        주어진 소스 데이터를 CDM 형식에 맞게 변환하는 메소드.
        변환된 데이터는 새로운 DataFrame으로 구성됩니다.
        """
        try : 
            cdm = pd.DataFrame({
            "drug_exposure_id": source.index + 1,
            "person_id": source["person_id"],
            "drug_concept_id": source["concept_id"],
            "drug_exposure_start_date": source[self.drug_exposure_start_datetime].dt.date,
            "drug_exposure_start_datetime": source[self.drug_exposure_start_datetime],
            "drug_exposure_end_date": (source[self.drug_exposure_start_datetime]+ pd.to_timedelta(source[self.days_supply].astype(int) + 1, unit = "D")).dt.date ,
            "drug_exposure_end_datetime": source[self.drug_exposure_start_datetime] + pd.to_timedelta(source[self.days_supply].astype(int) + 1, unit = "D"),
            "verbatim_end_date": None,
            "drug_type_concept_id": 38000177,
            "stop_reason": None,
            "refills": 0,
            "quantity": source[self.days_supply].astype(int) * source[self.qty].astype(float) * source[self.cnt].astype(float),
            "days_supply": source[self.days_supply].astype(int),
            "sig": None,
            "route_concept_id": 0,
            "lot_number": None,
            "provider_id": None,
            "visit_occurrence_id": source["visit_occurrence_id"],
            "visit_detail_id": source["visit_detail_id"],
            "drug_source_value": source[self.drug_source_value],
            "drug_source_concept_id": source[self.edicode],
            "route_source_value": source[self.route_source_value],
            "dose_unit_source_value": source[self.dose_unit_source_value],
            "vocabulary_id": "EDI",
            "visit_source_key": source[self.visit_source_key]
            })

            logging.info(f"CDM테이블 row수: {len(cdm)}")
            logging.debug(f"요약:\n{cdm.describe(include = 'O').T.to_string()}")
            logging.debug(f"요약:\n{cdm.describe().T.to_string()}")
            logging.debug(f"컬럼별 null 개수:\n{cdm.isnull().sum().to_string()}")

            return cdm   

        except Exception as e :
            logging.error(f"{self.table} 테이블 CDM 데이터 변환 중 오류: {e}", exc_info = True)

class MeasurementTransformer(DataTransformer):
    def __init__(self, config_path):
        super().__init__(config_path)
        self.table = "measurement"
        self.cdm_config = self.config[self.table]

        # 컬럼 변수 재정의   
        self.source_data = self.cdm_config["data"]["source_data"]
        self.output_filename = self.cdm_config["data"]["output_filename"]
        self.measurement_date = self.cdm_config["columns"]["measurement_date"]
        self.measurement_source_value = self.cdm_config["columns"]["measurement_source_value"]
        self.value_source_value = self.cdm_config["columns"]["value_source_value"]
        self.unit_source_value = self.cdm_config["columns"]["unit_source_value"]
        self.result_range = self.cdm_config["columns"]["result_range"]
        
        
    def transform(self):
        """
        소스 데이터를 읽어들여 CDM 형식으로 변환하고 결과를 CSV 파일로 저장하는 메소드입니다.
        """
        try : 
            source_data = self.process_source()
            transformed_data = self.transform_cdm(source_data)


            save_path = os.path.join(self.cdm_path, self.output_filename)
            self.write_csv(transformed_data, save_path)

            logging.info(f"{self.table} 테이블 변환 완료")
            logging.info(f"============================")
        
        except Exception as e :
            logging.error(f"{self.table} 테이블 변환 중 오류:\n {e}", exc_info=True)
            raise

    def process_source(self):
        """
        소스 데이터를 로드하고 전처리 작업을 수행하는 메소드입니다.
        """
        try:
            source = self.read_csv(self.source_data, path_type = self.source_flag, dtype = self.source_dtype, encoding = self.encoding)
            local_edi = self.read_csv(self.local_edi_data, path_type = self.cdm_flag, dtype = self.source_dtype, encoding = self.encoding)
            person_data = self.read_csv(self.person_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_data = self.read_csv(self.visit_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_detail = self.read_csv(self.visit_detail, path_type = self.cdm_flag, dtype = self.source_dtype)
            unit_data = self.read_csv(self.concept_unit, path_type = self.cdm_flag , dtype = self.source_dtype)
            logging.debug(f'원천 데이터 row수: {len(source)}')

            # 원천에서 조건걸기
            source = source[source[self.measurement_date] <= self.data_range]
            source = source[[self.person_source_value, self.measurement_date, self.measurement_source_value, self.value_source_value, self.unit_source_value, self.result_range, self.visit_source_key]]
            source[self.measurement_date] = pd.to_datetime(source[self.measurement_date])
            source = source[source[self.measurement_date] <= self.data_range]
            source = source[source[self.measurement_date].notna()]

            # value_as_number float형태로 저장되게 값 변경
            source["value_as_number"] = source[self.value_source_value].str.extract('(-?\d+\.\d+|\d+)')
            source["value_as_number"] = source["value_as_number"].astype(float)
            source["range_low"] = source[self.result_range].apply(lambda x: x.split('~')[0])
            source["range_low"] = source["range_low"].astype(float)
            source["range_high"] = source[self.result_range].apply(lambda x: x.split('~')[1])
            source["range_high"] = source["range_high"].astype(float)

            logging.debug(f'조건적용 후 원천 데이터 row수: {len(source)}')

            # local_edi 전처리
            local_edi = local_edi[[self.ordcode, self.fromdate, self.todate, self.edicode, "concept_id", "병원구분코드"]]
            local_edi[self.fromdate] = pd.to_datetime(local_edi[self.todate] , format="%Y%m%d", errors="coerce")
            local_edi[self.fromdate].fillna(pd.Timestamp('1900-01-01'), inplace = True)
            local_edi[self.todate] = pd.to_datetime(local_edi[self.todate] , format="%Y%m%d", errors="coerce")
            local_edi[self.todate].fillna(pd.Timestamp('2099-12-31'), inplace = True)

            # 데이터 컬럼 줄이기
            person_data = person_data[["person_id", "person_source_value"]]
            visit_data = visit_data[["visit_occurrence_id", "visit_start_datetime", "care_site_id", "visit_source_value", "person_id", "visit_source_key"]]
            visit_detail = visit_detail[["visit_detail_id", "visit_occurrence_id"]]
            unit_data = unit_data[["concept_id", "concept_code"]]
            
            # LOCAL코드와 EDI코드 매핑 테이블과 병합
            source = pd.merge(source, local_edi, left_on=self.measurement_source_value, right_on=self.ordcode, how="inner")
            del local_edi
            logging.debug(f'EDI코드 테이블과 병합 후 데이터 row수: {len(source)}')
            source = source[(source[self.measurement_date] >= source[self.fromdate]) & (source[self.measurement_date] <= source[self.todate])]
            logging.debug(f"EDI코드 사용기간별 필터 적용 후 데이터 row수: {len(source)}")

            # person table과 병합
            source = pd.merge(source, person_data, left_on=self.person_source_value.lower(), right_on="person_source_value", how="inner")
            del person_data
            logging.debug(f'person 테이블과 결합 후 데이터 row수: {len(source)}')

            # visit_start_datetime 형태 변경
            visit_data["visit_start_datetime"] = pd.to_datetime(visit_data["visit_start_datetime"])

            # visit_occurrence table과 병합
            source = pd.merge(source, visit_data, left_on=["person_id", self.visit_source_key], right_on=["person_id", "visit_source_key"], how="left", suffixes=('', '_y'))
            del visit_data
            logging.debug(f'visit_occurrence 테이블과 결합 후 데이터 row수: {len(source)}')

            # visit_detail table과 병합
            source = pd.merge(source, visit_detail, left_on=["visit_occurrence_id"], right_on=["visit_occurrence_id"], how="left", suffixes=('', '_y'))
            logging.debug(f'visit_detail 테이블과 결합 후 데이터 row수: {len(source)}')

            # concept_unit과 병합
            source = pd.merge(source, unit_data, left_on=self.unit_source_value, right_on="concept_code", how="left", suffixes=["", "_unit"])
            del unit_data

            # 값이 없는 경우 0으로 값 입력
            source.loc[source["concept_id"].isna(), "concept_id"] = 0
            
            logging.debug(f'CDM 테이블과 결합 후 데이터 row수: {len(source)}')

            return source
        
        except Exception as e :
            logging.error(f"{self.table} 테이블 소스 데이터 처리 중 오류: {e}", exc_info = True)

    def transform_cdm(self, source):
        """
        주어진 소스 데이터를 CDM 형식에 맞게 변환하는 메소드.
        변환된 데이터는 새로운 DataFrame으로 구성됩니다.
        """
        try : 
            operator_condition = [
                    source[self.value_source_value].isin([">"])
                    , source[self.value_source_value].isin([">="])
                    , source[self.value_source_value].isin(["="])
                    , source[self.value_source_value].isin(["<="])
                    , source[self.value_source_value].isin(["<"])
            ]
            operator_value = [
                    4172704
                    , 4171755
                    , 4172703
                    , 4171754
                    , 4171756
            ]

            value_concept_condition = [
                source[self.value_source_value] == "+"
                , source[self.value_source_value] == "++"
                , source[self.value_source_value] == "+++"
                , source[self.value_source_value] == "++++"
                , source[self.value_source_value].str.lower() == "negative"
                , source[self.value_source_value].str.lower() == "positive"
            ]
            value_concept_value = [
                4123508
                , 4126673
                , 4125547
                , 4126674
                , 9189
                , 9191
            ]

            cdm = pd.DataFrame({
                "measurement_id": source.index + 1,
                "person_id": source["person_id"],
                "measurement_concept_id": source["concept_id"],
                "measurement_date": source[self.measurement_date].dt.date,
                "measurement_datetime": source[self.measurement_date],
                "measurement_time": source[self.measurement_date].dt.time,
                "measurement_type_concept_id": 44818702,
                "operator_concept_id": np.select(operator_condition, operator_value, default = 0),
                "value_as_number": source["value_as_number"],
                "value_as_concept_id": np.select(value_concept_condition, value_concept_value, default = 0),
                "unit_concept_id": source["concept_id_unit"],
                "range_low": source["range_low"],
                "range_high": source["range_high"],
                "provider_id": None,
                "visit_occurrence_id": source["visit_occurrence_id"],
                "visit_detail_id": source["visit_detail_id"],
                "measurement_source_value": source[self.measurement_source_value],
                "measurement_source_concept_id": source[self.edicode],
                "unit_source_value": source[self.unit_source_value],
                "value_source_value": source[self.value_source_value].str[:50],
                "vocabulary_id": "EDI",
                "visit_source_key": source[self.visit_source_key]
                })

            logging.debug(f'CDM 데이터 row수: {len(cdm)}')
            logging.debug(f"요약:\n{cdm.describe(include = 'O').T.to_string()}")
            logging.debug(f"요약:\n{cdm.describe().T.to_string()}")
            logging.debug(f"컬럼별 null 개수:\n{cdm.isnull().sum().to_string()}")

            return cdm   

        except Exception as e :
            logging.error(f"{self.table} 테이블 CDM 데이터 변환 중 오류:\n {e}", exc_info = True)

        
class ProcedureTransformer(DataTransformer):
    def __init__(self, config_path):
        super().__init__(config_path)
        self.table = "procedure"
        self.cdm_config = self.config[self.table]

        self.source_data = self.cdm_config["data"]["source_data"]
        self.output_filename = self.cdm_config["data"]["output_filename"]
        self.procedure_source_value = self.cdm_config["columns"]["procedure_source_value"]
        self.procedure_date = self.cdm_config["columns"]["procedure_date"]
        self.orddate = self.cdm_config["columns"]["orddate"]

        
    def transform(self):
        """
        소스 데이터를 읽어들여 CDM 형식으로 변환하고 결과를 CSV 파일로 저장하는 메소드입니다.
        """
        try:
            source_data = self.process_source()
            transformed_data = self.transform_cdm(source_data)


            save_path = os.path.join(self.cdm_path, self.output_filename)
            self.write_csv(transformed_data, save_path)

            logging.info(f"{self.table} 테이블 변환 완료")
            logging.info(f"============================")
        
        except Exception as e :
            logging.error(f"{self.table} 테이블 변환 중 오류:\n {e}", exc_info=True)
            raise

    def process_source(self):
        """
        소스 데이터를 로드하고 전처리 작업을 수행하는 메소드입니다.
        """
        try: 
            source = self.read_csv(self.source_data, path_type = self.source_flag, dtype = self.source_dtype, encoding = self.encoding)
            local_edi = self.read_csv(self.local_edi_data, path_type = self.cdm_flag, dtype = self.source_dtype, encoding = self.encoding)
            person_data = self.read_csv(self.person_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_data = self.read_csv(self.visit_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_detail = self.read_csv(self.visit_detail, path_type = self.cdm_flag, dtype = self.source_dtype)
            logging.debug(f"원천 데이터 row수: {len(source)}")

            # 원천에서 조건걸기
            source[self.orddate] = pd.to_datetime(source[self.orddate])
            source[self.procedure_date] = pd.to_datetime(source[self.procedure_date])
            source = source[source[self.orddate] <= self.data_range]
            source = source[[self.person_source_value, self.orddate, self.procedure_date, self.procedure_source_value, self.visit_source_key, self.hospital]]
            logging.debug(f"조건적용 후 원천 데이터 row수: {len(source)}")

            local_edi = local_edi[[self.ordcode, self.fromdate, self.todate, self.edicode, "concept_id", "병원구분코드"]]
            local_edi[self.fromdate] = pd.to_datetime(local_edi[self.fromdate] , format="%Y%m%d", errors="coerce")
            local_edi[self.fromdate].fillna(pd.Timestamp('1900-01-01'), inplace = True)
            local_edi[self.todate] = pd.to_datetime(local_edi[self.todate] , format="%Y%m%d", errors="coerce")
            local_edi[self.todate].fillna(pd.Timestamp('2099-12-31'), inplace = True)

            # LOCAL코드와 EDI코드 매핑 테이블과 병합
            source = pd.merge(source, local_edi, left_on=self.procedure_source_value, right_on=self.ordcode, how="left")
            logging.debug(f'local_edi 테이블과 결합 후 데이터 row수: {len(source)}')
            source = source[(source[self.orddate] >= source[self.fromdate]) & (source[self.orddate] <= source[self.todate])]
            logging.debug(f"local_edi 사용기간별 필터 적용 후 데이터 row수: {len(source)}")

            # person table과 병합
            source = pd.merge(source, person_data, left_on=self.person_source_value, right_on="person_source_value", how="inner")
            source = source.drop(columns = ["care_site_id", "provider_id"])
            logging.debug(f'person 테이블과 결합 후 데이터 row수, {len(source)}')

            # visit_start_datetime 형태 변경
            visit_data["visit_start_datetime"] = pd.to_datetime(visit_data["visit_start_datetime"])

            # visit_occurrence table과 병합
            source = pd.merge(source, visit_data, left_on=["person_id", self.visit_source_key], right_on=["person_id", "visit_source_key"], how="left", suffixes=('', '_y'))
            logging.debug(f'visit_occurrence 테이블과 결합 후 데이터 row수, {len(source)}')

            # visit_detail table과 병합
            source = pd.merge(source, visit_detail, left_on=["visit_occurrence_id"], right_on=["visit_occurrence_id"], how="left", suffixes=('', '_y'))
            logging.debug(f'visit_detail 테이블과 결합 후 데이터 row수, {len(source)}')

            # 값이 없는 경우 0으로 값 입력
            source.loc[source["concept_id"].isna(), "concept_id"] = 0

            logging.debug(f'CDM 테이블과 결합 후 데이터 row수, {len(source)}')

            return source

        except Exception as e :
            logging.error(f"{self.table} 테이블 소스 데이터 처리 중 오류: {e}", exc_info = True)

    def transform_cdm(self, source):
        """
        주어진 소스 데이터를 CDM 형식에 맞게 변환하는 메소드.
        변환된 데이터는 새로운 DataFrame으로 구성됩니다.
        """
        try :                      
            cdm = pd.DataFrame({
                "procedure_occurrence_id": source.index + 1,
                "person_id": source["person_id"],
                "procedure_concept_id": source["concept_id"],
                "procedure_date": np.select([source[self.procedure_date].notna()], [source[self.procedure_date].dt.date], default = source[self.orddate].dt.date),
                "procedure_datetime": np.select([source[self.procedure_date].notna()], [source[self.procedure_date]], default = source[self.orddate]),
                "procedure_type_concept_id": 38000275,
                "modifier_concept_id": 0,
                "quantity": None,
                "provider_id": None,
                "visit_occurrence_id": source["visit_occurrence_id"],
                "visit_detail_id": source["visit_detail_id"],
                "procedure_source_value": source[self.procedure_source_value],
                "procedure_source_concept_id": source[self.edicode],
                "modifier_source_value": None ,
                "vocabulary_id": "EDI",
                "visit_source_key": source[self.visit_source_key]
                })

            cdm["procedure_date"] = pd.to_datetime(cdm["procedure_date"])
            cdm["procedure_datetime"] = pd.to_datetime(cdm["procedure_datetime"])

            logging.debug(f'CDM 데이터 row수, {len(cdm)}')
            logging.debug(f"요약:\n{cdm.describe(include = 'O').T.to_string()}")
            logging.debug(f"요약:\n{cdm.describe().T.to_string()}")
            logging.debug(f"컬럼별 null 개수:\n{cdm.isnull().sum().to_string()}")

            return cdm   

        except Exception as e :
            logging.error(f"{self.table} 테이블 CDM 데이터 변환 중 오류:\n {e}", exc_info = True)

        
class ObservationPeriodTransformer(DataTransformer):
    def __init__(self, config_path):
        super().__init__(config_path)
        self.table = "observation_period"
        self.cdm_config = self.config[self.table]

        self.visit = self.cdm_config["data"]["visit"]
        self.condition = self.cdm_config["data"]["condition"]
        self.drug = self.cdm_config["data"]["drug"]
        self.measurement = self.cdm_config["data"]["measurement"]
        self.procedure = self.cdm_config["data"]["procedure"]
        self.output_filename = self.cdm_config["data"]["output_filename"]
    
    def transform(self):
        """
        소스 데이터를 읽어들여 CDM 형식으로 변환하고 결과를 CSV 파일로 저장하는 메소드입니다.
        """
        transformed_data = self.process_source()

        save_path = os.path.join(self.cdm_path, self.output_filename)
        self.write_csv(transformed_data, save_path)

        logging.info(f"{self.table} 테이블 변환 완료")
        logging.info(f"============================")

    def process_source(self):
        """
        소스 데이터를 로드하고 전처리 작업을 수행하는 메소드입니다.
        """
        try:
            visit_data = self.read_csv(self.visit, path_type = self.cdm_flag, dtype = self.source_dtype, encoding = self.encoding)
            condition_data = self.read_csv(self.condition, path_type = self.cdm_flag, dtype = self.source_dtype, encoding = self.encoding)
            drug_data = self.read_csv(self.drug, path_type = self.cdm_flag, dtype = self.source_dtype, encoding = self.encoding)
            measurement_data = self.read_csv(self.measurement, path_type = self.cdm_flag, dtype = self.source_dtype, encoding = self.encoding)
            procedure_data = self.read_csv(self.procedure, path_type = self.cdm_flag, dtype = self.source_dtype, encoding = self.encoding)

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
            cdm["end_date"].fillna(cdm["start_date"])

            cdm = pd.DataFrame({
                "observation_period_id": cdm.index + 1,
                "person_id": cdm["person_id"],
                "observation_period_start_date": cdm["start_date"],
                "observation_period_end_date": cdm["end_date"],
                "period_type_concept_id": 44814724
            })  

            logging.debug(f"CDM 데이터 row수 {len(cdm)}")
            logging.debug(f"요약:\n{cdm.describe(include = 'O').T.to_string()}")
            logging.debug(f"요약:\n{cdm.describe().T.to_string()}")
            logging.debug(f"컬럼별 null 개수:\n{cdm.isnull().sum().to_string()}")

            return cdm

        except Exception as e :
            logging.error(f"{self.table} 테이블 소스 데이터 처리 중 오류: {e}", exc_info = True)