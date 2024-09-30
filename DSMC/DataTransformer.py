import pandas as pd
import numpy as np
import yaml
import os
from datetime import datetime
import logging
import warnings
import inspect
import csv

# 숫자 값을 유지하고, 문자가 포함된 값을 NaN으로 대체하는 함수 정의
def convert_to_numeric(value):
    try:
        # pd.to_numeric을 사용하여 숫자로 변환 시도
        return pd.to_numeric(value)
    except ValueError:
        # 변환이 불가능한 경우 NaN 반환
        return np.nan
    
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
        self.local_kcd_data = self.config["local_kcd_data"]
        self.local_edi_data = self.config["local_edi_data"]
        self.source_flag = "source"
        self.cdm_flag = "CDM"
        self.source_dtype = self.config["source_dtype"]
        self.source_encoding = self.config["source_encoding"]
        self.cdm_encoding = self.config["cdm_encoding"]
        self.person_source_value = self.config["person_source_value"]
        self.data_range = self.config["data_range"]
        self.target_zip = self.config["target_zip"]
        self.location_data = self.config["location_data"]
        self.concept_unit = self.config["concept_unit"]
        self.source_key = self.config["source_key"]
        self.hospital = self.config["hospital"]
        self.edicode = self.config["edicode"]
        self.concept_etc = self.config["concept_etc"]
        self.concept_kcd = self.config["concept_kcd"]
        self.unit_concept_synonym = self.config["unit_concept_synonym"]
        self.diag_condition = self.config["diag_condition"]
        self.no_matching_concept = self.config["no_matching_concept"]
        
        self.ordcode = self.config["ordcode"]
        self.ordname = self.config["ordname"]
        self.edicode = self.config["edicode"]
        self.fromdate = self.config["fromdate"]
        self.todate = self.config["todate"]
        self.hospital_code = self.config["hospital_code"]

        # 상병조건이 있다면 조건에 맞는 폴더 생성
        os.makedirs(os.path.join(self.cdm_path, self.diag_condition ), exist_ok = True)

    def load_config(self, config_path):
        """
        YAML 설정 파일을 로드합니다.
        """
        with open(config_path, 'r', encoding="utf-8") as file:
            return yaml.safe_load(file)
        
    def read_csv(self, file_name, path_type = 'source', encoding = None, dtype = None):
        """
        CSV 파일을 읽어 DataFrame으로 반환합니다.
        path_type에 따라 'source' 또는 'CDM' 경로에서 파일을 읽습니다.
        """
        if path_type == "source":
            full_path = os.path.join(self.config["source_path"], file_name + ".csv")
            default_encoding = self.source_encoding
        elif path_type == "CDM":
            if self.diag_condition :
                full_path = os.path.join(self.config["CDM_path"], self.diag_condition , file_name + ".csv")
            else :
                full_path = os.path.join(self.config["CDM_path"], file_name + ".csv")
            default_encoding = self.cdm_encoding
        else :
            raise ValueError(f"Invalid path type: {path_type}")
        
        encoding = encoding if encoding else default_encoding
        
        return pd.read_csv(full_path, dtype = dtype, encoding = encoding, quoting = csv.QUOTE_ALL)

    def write_csv(self, df, file_path, filename, encoding = 'utf-8'):
        """
        DataFrame을 CSV 파일로 저장합니다.
        """
        encoding = self.cdm_encoding
        if self.diag_condition  :
            df.to_csv(os.path.join(file_path, self.diag_condition , filename + ".csv"), encoding = encoding, index = False)
        else :
            df.to_csv(os.path.join(file_path, filename + ".csv"), encoding = encoding, index = False)

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
            # save_path = os.path.join(self.cdm_path, self.output_filename)
            self.write_csv(transformed_data, self.cdm_path, self.output_filename)

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
            source_data = self.read_csv(self.source_data, path_type = self.source_flag, dtype = self.source_dtype)
            logging.debug(f"원천 데이터 row수: {len(source_data)}")

            location = self.read_csv(self.location_data, path_type = self.source_flag, dtype = self.source_dtype)
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
            logging.debug(f"요약:\n{cdm.describe(include = 'all').T.to_string()}")
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
        self.care_site_source_value = self.cdm_config["columns"]["care_site_source_value"]
        self.specialty_source_value = self.cdm_config["columns"]["specialty_source_value"]
        self.year_of_birth = self.cdm_config["columns"]["year_of_birth"]
        self.provider_name = self.cdm_config["columns"]["provider_name"]
        self.gender_source_value = self.cdm_config["columns"]["gender_source_value"]
        self.provider_source_value = self.cdm_config["columns"]["provider_source_value"]
        self.specialty_source_value_name = self.cdm_config["columns"]["specialty_source_value_name"]

    def transform(self):
        """
        소스 데이터를 읽어들여 CDM 형식으로 변환하고 결과를 CSV 파일로 저장하는 메소드.
        """
        try:
            source_data = self.process_source()
            transformed_data = self.transform_cdm(source_data)
            # save_path = os.path.join(self.cdm_path, self.output_filename)
            self.write_csv(transformed_data, self.cdm_path, self.output_filename)

            logging.info(f"{self.table} 테이블 변환 완료")
            logging.info(f"============================")

        except Exception as e :
            logging.error(f"{self.table} 테이블 변환 중 오류: {e}", exc_info=True)

    def process_source(self):
        """
        소스 데이터와 care site 데이터를 읽어들이고 병합하는 메소드.
        """
        try :
            source_data = self.read_csv(self.source_data, path_type = self.source_flag, dtype = self.source_dtype)
            care_site = self.read_csv(self.care_site_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            concept_etc = self.read_csv(self.concept_etc, path_type = self.source_flag, dtype = self.source_dtype)
            logging.debug(f"원천 데이터 row수: {len(source_data)}")

            source = pd.merge(source_data,
                            care_site,
                            left_on = [self.care_site_source_value, self.hospital],
                            right_on = ["care_site_source_value", "place_of_service_source_value"],
                            how = "left")
            logging.debug(f"care_site와 결합 후 원천 데이터 row수: {len(source_data)}")

            specialty_conditions = [
            source[self.specialty_source_value].str.contains("간호", case = False, na = False),
            source[self.specialty_source_value].str.contains("의사", case = False, na = False)
            ]
            specialty_concept_id = [32581, 32577] # Nurse, Physician
            concept_etc["concept_id"] = concept_etc["concept_id"].astype(int)

            source["specialty_concept_id"] = np.select(specialty_conditions, specialty_concept_id, default = self.no_matching_concept[0])

            gender_conditions = [
                source_data[self.gender_source_value].isin(['남']),
                source_data[self.gender_source_value].isin(['여'])
            ]
            gender_concept_id = [8507, 8532]
            source["gender_concept_id"] = np.select(gender_conditions, gender_concept_id, default = self.no_matching_concept[0])
            
            source = pd.merge(source, concept_etc, left_on = "specialty_concept_id", right_on="concept_id", how="left")
            logging.debug(f"concept_etc와 결합 후 원천 데이터 row수: {len(source)}")
        
            return source

        except Exception as e :
            logging.error(f"{self.table} 테이블 원천 데이터 처리 중 오류:\n {e}", exc_info=True)

    def transform_cdm(self, source_data):
        """
        주어진 소스 데이터를 CDM 형식에 맞게 변환하는 메소드.
        변환된 데이터는 새로운 DataFrame으로 구성됩니다.
        """
        try :
            
            cdm = pd.DataFrame({
                "provider_id" : source_data.index + 1,
                "provider_name": self.provider_name,
                "npi": None,
                "dea": None,
                "specialty_concept_id": source_data["specialty_concept_id"],
                "specialty_concept_id_name": source_data["concept_name"],
                "care_site_id": source_data["care_site_id"],
                "care_site_name": source_data["care_site_name"],
                "year_of_birth": source_data[self.year_of_birth],
                "gender_concept_id": source_data["gender_concept_id"],
                "provider_source_value": source_data[self.provider_source_value],
                "specialty_source_value": source_data[self.specialty_source_value],
                "specialty_source_value_name": source_data[self.specialty_source_value_name],
                "specialty_source_concept_id": self.no_matching_concept[0],
                "gender_source_value": source_data[self.gender_source_value], #np.select([source[gender_source_value].isna()], None, default = None),
                "gender_source_concept_id": source_data["gender_concept_id"],
                "병원구분": source_data[self.hospital]
                })

            logging.debug(f"CDM 데이터 row수: {len(cdm)}")
            logging.debug(f"요약:\n{cdm.describe(include = 'all').T.to_string()}")
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
        self.source_condition = self.cdm_config["data"]["source_condition"]
        self.output_filename = self.cdm_config["data"]["output_filename"]
        self.location_source_value = self.cdm_config["columns"]["location_source_value"]
        self.gender_source_value = self.cdm_config["columns"]["gender_source_value"]
        self.death_datetime = self.cdm_config["columns"]["death_datetime"]
        self.death_data = self.cdm_config["data"]["death_data"]
        self.race_source_value = self.cdm_config["columns"]["race_source_value"]
        self.birth_date = self.cdm_config["columns"]["birth_date"]
        self.person_name = self.cdm_config["columns"]["person_name"]
        self.abotyp = self.cdm_config["columns"]["abotyp"]
        self.rhtyp = self.cdm_config["columns"]["rhtyp"]
        self.diagcode = self.cdm_config["columns"]["diagcode"]

    def transform(self):
        """
        소스 데이터를 읽어들여 CDM 형식으로 변환하고 결과를 CSV 파일로 저장하는 메소드.
        """
        try:
            source_data = self.process_source()
            transformed_data = self.transform_cdm(source_data)

            # save_path = os.path.join(self.cdm_path, self.output_filename)
            self.write_csv(transformed_data, self.cdm_path, self.output_filename)

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
            location_data = self.read_csv(self.location_data, path_type = self.source_flag, dtype = self.source_dtype)
            death_data = self.read_csv(self.death_data, path_type = self.source_flag, dtype = self.source_dtype)
            logging.debug(f"원천 데이터 row수: {len(source_data)}")
            
            source_data[self.birth_date] = pd.to_datetime(source_data[self.birth_date])
            death_data[self.death_datetime] = pd.to_datetime(death_data[self.death_datetime])
            
            source_data = pd.merge(source_data, death_data, left_on=self.person_source_value, right_on = self.person_source_value, how = "left")
            logging.debug(f"death 테이블과 결합 후 원천 데이터1 row수: {len(source_data)}")
            
            source_data = pd.merge(source_data, location_data, left_on = self.location_source_value, right_on="LOCATION_SOURCE_VALUE", how = "left")
            # source_data.loc[source_data["LOCATION_ID"].isna(), "LOCATION_ID"] = 0
            logging.debug(f"location 테이블과 결합 후 원천 데이터1 row수: {len(source_data)}")

            # 상병조건이 있는 경우
            if self.diag_condition:
                condition = self.read_csv(self.source_condition, path_type=self.source_flag, dtype=self.source_dtype)
                condition = condition[condition[self.diagcode].str.startswith(self.diag_condition, na=False)]
                condition = condition[self.person_source_value].drop_duplicates()
                
                source_data = pd.merge(source_data, condition, on=self.person_source_value, how = "inner", suffixes=('', '_diag'))

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
                source[self.race_source_value].isin(["외국인"]),
                source[self.race_source_value].isnull()
            ]
            race_concept_id = [38003585, 8552, self.no_matching_concept[0]]

            gender_conditions = [
                source[self.gender_source_value].isin(['남']),
                source[self.gender_source_value].isin(['여']),
                source[self.gender_source_value].isnull(),
            ]
            gender_concept_id = [8507, 8532, self.no_matching_concept[0]]

            cdm = pd.DataFrame({
                "person_id" : source.index + 1,
                "gender_concept_id": np.select(gender_conditions, gender_concept_id, default = self.no_matching_concept[0]),
                "year_of_birth": source[self.birth_date].dt.year,
                "month_of_birth": source[self.birth_date].dt.month,
                "day_of_birth": source[self.birth_date].dt.day,
                "birth_datetime": pd.to_datetime(source[self.birth_date], errors='coerce'),
                "death_datetime": pd.to_datetime(source[self.death_datetime], errors='coerce'),
                "race_concept_id": np.select(race_conditions, race_concept_id, default = self.no_matching_concept[0]),
                "ethnicity_concept_id": self.no_matching_concept[0],
                "location_id": source["LOCATION_ID"],
                "provider_id": None,
                "care_site_id": None, 
                "person_source_value": source[self.person_source_value],
                "환자명": self.person_name,
                "gender_source_value": source[self.gender_source_value],
                "gender_source_concept_id": np.select(gender_conditions, gender_concept_id, default = self.no_matching_concept[0]),
                "race_source_value": source[self.race_source_value],
                "race_source_concept_id": self.no_matching_concept[0],
                "ethnicity_source_value": None,
                "ethnicity_source_concept_id": self.no_matching_concept[0],
                "혈액형(ABO)": source[self.abotyp],
                "혈액형(RH)": source[self.rhtyp]
                })
            
            cdm["birth_datetime"] = cdm["birth_datetime"].dt.strftime('%Y-%m-%d %H:%M:%S')
            cdm["death_datetime"] = cdm["death_datetime"].dt.strftime('%Y-%m-%d %H:%M:%S')

            logging.debug(f"CDM 데이터 row수: {len(cdm)}")
            logging.debug(f"요약:\n{cdm.describe(include = 'all').T.to_string()}")
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
        self.columns = ["visit_occurrence_id", "person_id", "환자명", "visit_concept_id", "visit_start_date", "visit_start_datetime"
                    , "visit_end_date", "visit_end_datetime", "visit_type_concept_id", "visit_type_concept_id_name", "provider_id"
                    , "care_site_id", "visit_source_value", "visit_source_concept_id", "admitted_from_concept_id"
                    , "admitted_from_source_value", "discharge_to_source_value", "discharge_to_concept_id"
                    , "preceding_visit_occurrence_id", "visit_source_key", "진료과", "진료과명"]

        # 컬럼 변수 재정의
        self.source_data = self.cdm_config["data"]["source_data"]
        self.output_filename = self.cdm_config["data"]["output_filename"]

        self.meddept = self.cdm_config["columns"]["meddept"]
        self.meddr = self.cdm_config["columns"]["meddr"]
        self.visit_start_datetime = self.cdm_config["columns"]["visit_start_datetime"]
        self.visit_end_datetime = self.cdm_config["columns"]["visit_end_datetime"]
        self.admitted_from_source_value = self.cdm_config["columns"]["admitted_from_source_value"]
        self.discharge_to_source_value = self.cdm_config["columns"]["discharge_to_source_value"]
        self.visit_source_value = self.cdm_config["columns"]["visit_source_value"]

    def transform(self):
        """
        소스 데이터를 읽어들여 CDM 형식으로 변환하고 결과를 CSV 파일로 저장하는 메소드입니다.
        """
        try : 
            source_data = self.process_source()
            transformed_data = self.transform_cdm(source_data)

            # save_path = os.path.join(self.config["CDM_path"], self.output_filename)
            self.write_csv(transformed_data, self.cdm_path, self.output_filename)

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
            provider_data = self.read_csv(self.provider_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            care_site_data = self.read_csv(self.care_site_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            concept_etc = self.read_csv(self.concept_etc, path_type = self.source_flag, dtype = self.source_dtype)
            logging.debug(f"원천 데이터 row수: source: {len(source)}")

            # 원천 데이터 범위 설정
            source["visit_source_key"] = source[self.person_source_value] + ';' + source[self.source_key]
            source[self.visit_start_datetime] = pd.to_datetime(source[self.visit_start_datetime], errors = "coerce")
            source[self.visit_end_datetime] = pd.to_datetime(source[self.visit_end_datetime], errors = "coerce")
            source = source[source[self.visit_start_datetime] <= pd.to_datetime(self.data_range)]
            logging.debug(f"데이터 범위 조건 적용 후 원천 데이터 row수: {len(source)}")

            # 불러온 원천 전처리
            source = pd.merge(source, person_data, left_on = self.person_source_value, right_on="person_source_value", how="inner")
            source = source.drop(columns = ["care_site_id", "provider_id"])
            logging.debug(f"person 테이블과 결합 후 원천 데이터1 row수: {len(source)}")

            source = pd.merge(source, care_site_data, left_on = [self.meddept, self.hospital], right_on=["care_site_source_value", "place_of_service_source_value"], how="left")
            logging.debug(f"care_site 테이블과 결합 후 원천 데이터1 row수: {len(source)}")

            source = pd.merge(source, provider_data, left_on = [self.meddr, self.hospital], right_on=["provider_source_value", self.hospital], how="left", suffixes=('', '_y'))
            # source.loc[source["care_site_id"].isna(), "care_site_id"] = 0
            logging.debug(f"provider 테이블과 결합 후 원천 데이터1 row수: {len(source)}")

            source["visit_type_concept_id"] = np.select([source[self.meddept] == "CTC"], [44818519], default = 44818518)
            concept_etc["concept_id"] = concept_etc["concept_id"].astype(int)
            source = pd.merge(source, concept_etc, left_on = "visit_type_concept_id", right_on="concept_id", how="left")
            logging.debug(f"concept_etc 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            return source

        except Exception as e :
            logging.error(f"{self.table} 테이블 소스 데이터 처리 중 오류: {e}", exc_info = True)

    def transform_cdm(self, source):
        """
        주어진 소스 데이터를 CDM 형식에 맞게 변환하는 메소드.
        변환된 데이터는 새로운 DataFrame으로 구성됩니다.
        """
        try : 
            # cdm 생성
            visit_condition = [
                source[self.visit_source_value] == "외래",
                source[self.visit_source_value] == "입원",
                source[self.visit_source_value] == "응급"
            ]
            visit_concept_id = [9202, 9201, 9203]

            # admit_condition = [
            #     source[self.admitted_from_source_value].isin(["1", "6"]),
            #     source[self.admitted_from_source_value].isin(["3"]),
            #     source[self.admitted_from_source_value].isin(["7"]),
            #     source[self.admitted_from_source_value].isin(["9"])
            #     ]
            # admit_concept_id = [8765, 8892, 8870, 8844]

            # discharge_condition = [
            #     source[self.discharge_to_source_value].isin(["1"]),
            #     source[self.discharge_to_source_value].isin(["2"]),
            #     source[self.discharge_to_source_value].isin(["3"]),
            #     source[self.discharge_to_source_value].isin(["8"]),
            #     source[self.discharge_to_source_value].isin(["9"])
            #     ]
            # discharge_concept_id = [44790567, 4061268, 8536, 44814693, 8844]

            cdm = pd.DataFrame({
                "person_id": source["person_id"],
                "환자명": source["환자명"],
                "visit_concept_id": np.select(visit_condition, visit_concept_id, default = self.no_matching_concept[0]),
                "visit_start_date": source[self.visit_start_datetime].dt.date ,
                "visit_start_datetime": source[self.visit_start_datetime],
               "visit_end_date": source[self.visit_end_datetime].dt.date,
                "visit_end_datetime": source[self.visit_end_datetime],
                "visit_type_concept_id": np.select([source["visit_type_concept_id"].notna()], [source["visit_type_concept_id"]], default = self.no_matching_concept[0]),
                "visit_type_concept_id_name": np.select([source["concept_name"].notna()], [source["concept_name"]], default=self.no_matching_concept[1]),
                "provider_id": source["provider_id"],
                "care_site_id": source["care_site_id"],
                "visit_source_value": source[self.visit_source_value],
                "visit_source_concept_id": np.select(visit_condition, visit_concept_id, default = self.no_matching_concept[0]),
                "admitted_from_concept_id": self.no_matching_concept[0], # np.select(admit_condition, admit_concept_id, default = self.no_matching_concept[0]),
                "admitted_from_source_value": source[self.admitted_from_source_value],
                "discharge_to_concept_id": self.no_matching_concept[0], # np.select(discharge_condition, discharge_concept_id, default = self.no_matching_concept[0]),
                "discharge_to_source_value": source[self.discharge_to_source_value],
                "visit_source_key": source["visit_source_key"],
                "진료과": source[self.meddept],
                "진료과명": source["care_site_name"]
                })
            
            # cdm.reset_index(drop=True, inplace = True)
            cdm["visit_occurrence_id"] = cdm.index + 1
            cdm.sort_values(by=["person_id", "visit_start_datetime"], inplace = True)
            cdm["preceding_visit_occurrence_id"] = cdm.groupby("person_id")["visit_occurrence_id"].shift(1)
            cdm["preceding_visit_occurrence_id"] = cdm["preceding_visit_occurrence_id"].apply(lambda x : x if pd.isna(x) else str(int(x)))

            cdm = cdm[self.columns]
            
            logging.debug(f"CDM 데이터 row수: {len(cdm)}")
            logging.debug(f"요약(문자형_data):\n{cdm.describe(include = 'all').T.to_string()}")
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
        self.columns = ["visit_detail_id", "person_id", "환자명", "visit_detail_concept_id", "visit_detail_start_date"
                        , "visit_detail_start_datetime", "visit_detail_end_date", "visit_detail_end_datetime"
                        , "visit_detail_type_concept_id", "visit_detail_type_concept_id_name", "provider_id", "care_site_id", "visit_detail_source_value"
                        , "visit_detail_source_concept_id", "admitted_from_concept_id", "admitted_from_source_value"
                        , "discharge_to_source_value", "discharge_to_concept_id", "preceding_visit_detail_id"
                        , "visit_detail_parent_id", "visit_occurrence_id", "진료과", "진료과명", "병동번호", "병동명", "visit_detail_source_key"]

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

            # save_path = os.path.join(self.cdm_path, self.output_filename)
            self.write_csv(transformed_data, self.cdm_path, self.output_filename)

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
            # provider_data = self.read_csv(self.provider_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            care_site_data = self.read_csv(self.care_site_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_data = self.read_csv(self.visit_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            concept_etc = self.read_csv(self.concept_etc, path_type = self.source_flag, dtype = self.source_dtype)
            logging.debug(f"원천 데이터 row수: {len(source)}")

            # 원천에서 조건걸기
            source["visit_detail_source_key"] = source[self.person_source_value] + ';' + source[self.source_key]
            source[self.visit_detail_start_datetime] = pd.to_datetime(source[self.visit_detail_start_datetime])
            source[self.visit_detail_end_datetime] = pd.to_datetime(source[self.visit_detail_end_datetime], errors = "coerce")
            source = source[source[self.visit_detail_start_datetime] <= pd.to_datetime(self.data_range)]
            logging.debug(f"조건 적용 후 원천 데이터 row수: {len(source)}")

            # person table과 병합
            source = pd.merge(source, person_data, left_on=self.person_source_value, right_on="person_source_value", how="inner")
            source = source.drop(columns = ["care_site_id", "provider_id"])
            logging.debug(f"person 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            # care_site table과 병합
            source = pd.merge(source, care_site_data, left_on=[self.visit_detail_source_value, self.hospital], right_on=["care_site_source_value", "place_of_service_source_value"], how="left")
            logging.debug(f"care_site 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            # # provider table과 병합
            # source = pd.merge(source, provider_data, left_on=[self.provider, self.hospital], right_on=["provider_source_value", "병원구분"], how="left", suffixes=('', '_y'))
            # logging.debug(f"provider 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            # visit_occurrence테이블에서 I, E에 해당하는 데이터만 추출
            visit_data = visit_data[visit_data["visit_source_value"].isin(["I", "E"])]
            # visit_occurrence table과 병합
            source = pd.merge(source, visit_data, left_on="visit_detail_source_key", right_on="visit_source_key", how="left", suffixes=('', '_y'))
            logging.debug(f"visit_occurrence 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            source["visit_detail_type_concept_id"] = 44818518
            concept_etc["concept_id"] = concept_etc["concept_id"].astype(int)
            source = pd.merge(source, concept_etc, left_on="visit_detail_type_concept_id", right_on='concept_id')
            logging.debug(f"CDM 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            # 컬럼을 datetime형태로 변경
            source["visit_start_datetime"] = pd.to_datetime(source["visit_start_datetime"])
            source["visit_end_datetime"] = pd.to_datetime(source["visit_end_datetime"])

            # source = source[(source[self.visit_detail_start_datetime] >= source["visit_start_datetime"]) & (source[self.visit_detail_start_datetime] <= source["visit_end_datetime"])]
            logging.debug(f"날짜 조건 적용 후 원천 데이터 row수: {len(source)}")

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
                "환자명": source["환자명"],
                "visit_detail_concept_id": 32037,
                "visit_detail_start_date": source[self.visit_detail_start_datetime].dt.date,
                "visit_detail_start_datetime": source[self.visit_detail_start_datetime],
                "visit_detail_end_date": source[self.visit_detail_end_datetime].dt.date,
                "visit_detail_end_datetime": pd.to_datetime(source[self.visit_detail_end_datetime], errors="coerce"),
                "visit_detail_type_concept_id": np.select([source["visit_detail_type_concept_id"].notna()], [source["visit_detail_type_concept_id"]], default=self.no_matching_concept[0]),
                "visit_detail_type_concept_id_name": np.select([source["concept_name"].notna()], [source["concept_name"]], default=self.no_matching_concept[1]),
                "provider_id": source["provider_id"],
                "care_site_id": source["care_site_id"],
                "visit_detail_source_value": self.visit_detail_source_value,
                "visit_detail_source_concept_id": self.no_matching_concept[0],
                "admitted_from_concept_id": self.no_matching_concept[0],
                "admitted_from_source_value": self.admitted_from_source_value,
                "discharge_to_source_value": self.discharge_to_source_value,
                "discharge_to_concept_id": self.no_matching_concept[0],
                "visit_detail_parent_id": None,
                "visit_occurrence_id": source["visit_occurrence_id"],
                "진료과": source[self.visit_detail_source_value],
                "진료과명": source["care_site_name"],
                "병동번호": None,
                "병동명": None,
                "visit_detail_source_key": source["visit_detail_source_key"]
                })

            # 컬럼 생성
            cdm.reset_index(drop = True, inplace = True)
            cdm["visit_detail_id"] = cdm.index + 1
            cdm.sort_values(by=["person_id", "visit_detail_start_datetime"], inplace = True)
            cdm["preceding_visit_detail_id"] = cdm.groupby("person_id")["visit_detail_id"].shift(1)
            cdm["preceding_visit_detail_id"] = cdm["preceding_visit_detail_id"].apply(lambda x : str(int(x)) if not pd.isna(x) else x)

            cdm = cdm[self.columns]

            logging.debug(f"CDM 데이터 row수: {len(cdm)}")
            logging.debug(f"요약:\n{cdm.describe(include = 'all').T.to_string()}")
            logging.debug(f"컬럼별 null 개수:\n{cdm.isnull().sum().to_string()}")

            return cdm   

        except Exception as e :
            logging.error(f"{self.table} 테이블 CDM 데이터 변환 중 오류: {e}", exc_info = True)


class LocalKCDTransformer(DataTransformer):
    def __init__(self, config_path):
        super().__init__(config_path)
        self.table = "local_kcd"
        self.cdm_config = self.config[self.table]

        # 컬럼 변수 재정의      
        self.source = self.cdm_config["data"]["source"]
        self.output_filename = self.cdm_config["data"]["output_filename"]

        self.diagcode = self.cdm_config["columns"]["diagcode"]
        self.fromdate = self.cdm_config["columns"]["fromdate"]
        self.todate = self.cdm_config["columns"]["todate"]
        self.korname = self.cdm_config["columns"]["korname"]
        # columns in concept table
        self.concept_id = self.cdm_config["columns"]["concept_id"]
        self.concept_name = self.cdm_config["columns"]["concept_name"]
        self.domain_id = self.cdm_config["columns"]["domain_id"]
        self.vocabulary_id = self.cdm_config["columns"]["vocabulary_id"]
        self.concept_class_id = self.cdm_config["columns"]["concept_class_id"]
        self.standard_concept = self.cdm_config["columns"]["standard_concept"]
        self.concept_code = self.cdm_config["columns"]["concept_code"]
        self.valid_start_date = self.cdm_config["columns"]["valid_start_date"]
        self.valid_end_date = self.cdm_config["columns"]["valid_end_date"]
        self.invalid_reason = self.cdm_config["columns"]["invalid_reason"]
        
    def transform(self):
        """
        소스 데이터를 읽어들여 CDM 형식으로 변환하고 결과를 CSV 파일로 저장하는 메소드입니다.
        """
        try:
            transformed_data = self.process_source()

            # save_path = os.path.join(self.cdm_path, self.output_filename)
            self.write_csv(transformed_data, self.cdm_path, self.output_filename)

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
            source = self.read_csv(self.source, path_type = self.source_flag, dtype = self.source_dtype)
            concept_kcd = self.read_csv(self.concept_kcd, path_type = self.source_flag, dtype = self.source_dtype)
            logging.debug(f'원천 데이터 row수: order: {len(source)}, concept: {len(concept_kcd)}')

            # 2. diag 테이블 처리
            source['changed_diagcode'] = source[self.diagcode].str.split('-').str[0]
            logging.debug(f'조건 적용 후 row수: {len(source)}')

            # concept 테이블 처리
            concept_kcd[self.concept_code] = concept_kcd[self.concept_code].str.replace('.', '')
            concept_kcd = concept_kcd[concept_kcd[self.vocabulary_id] == 'KCD7']

            # 3. 매칭 함수 정의
            def match_diagcode(diagcode, concept_kcd):
                for i in range(len(diagcode), 0, -1):
                    sub_diagcode = diagcode[:i]
                    match = concept_kcd[concept_kcd[self.concept_code] == sub_diagcode]
                    if not match.empty:
                        return match.iloc[0]
                return pd.Series([None]*len(concept_kcd.columns), index=concept_kcd.columns)

            # 4. 매칭 수행
            matched_df = source.apply(lambda row: match_diagcode(row['changed_diagcode'], concept_kcd), axis=1)
            logging.debug(f'matched_df row수: {len(matched_df)}')

            # 5. 원래 source 매칭 결과 합치기
            local_kcd = pd.concat([source.reset_index(drop=True), matched_df.reset_index(drop=True)], axis=1)
            logging.debug(f'원천 데이터와 합친 후 row수: {len(matched_df)}')

            # 6. 필요한 컬럼 선택 및 기본값 설정
            local_kcd = local_kcd[[self.diagcode, 'changed_diagcode', self.fromdate, self.todate, self.korname,
                                self.concept_id, self.concept_name, self.domain_id, self.vocabulary_id, self.concept_class_id, 
                                self.standard_concept, self.concept_code, self.valid_start_date, self.valid_end_date, self.invalid_reason]]

            # local_kcd[['concept_id', 'concept_name', 'domain_id', 'vocabulary_id', 'concept_class_id',
            #         'standard_concept', 'concept_code', 'valid_start_date', 'valid_end_date', 'invalid_reason']] = \
            #     local_kcd[['concept_id', 'concept_name', 'domain_id', 'vocabulary_id', 'concept_class_id',
            #             'standard_concept', 'concept_code', 'valid_start_date', 'valid_end_date', 'invalid_reason']].fillna('')

            local_kcd = local_kcd.sort_values(self.diagcode)

            logging.debug(f'local_kcd row수: {len(local_kcd)}')
            logging.debug(f"요약:\n{local_kcd.describe(include = 'all').T.to_string()}")
            logging.debug(f"컬럼별 null 개수:\n{local_kcd.isnull().sum().to_string()}")

            return local_kcd

        except Exception as e :
            logging.error(f"{self.table} 테이블 소스 데이터 처리 중 오류:\n {e}", exc_info = True)


class ConditionOccurrenceTransformer(DataTransformer):
    def __init__(self, config_path):
        super().__init__(config_path)
        self.table = "condition_occurrence"
        self.cdm_config = self.config[self.table]

        # 컬럼 변수 재정의     
        self.source_data = self.cdm_config["data"]["source_data"]
        self.output_filename = self.cdm_config["data"]["output_filename"]
        self.meddept = self.cdm_config["columns"]["meddept"]
        self.provider = self.cdm_config["columns"]["provider"]
        self.condition_start_datetime = self.cdm_config["columns"]["condition_start_datetime"]
        self.condition_type = self.cdm_config["columns"]["condition_type"]
        self.condition_source_value = self.cdm_config["columns"]["condition_source_value"]
        self.condition_source_value_name = self.cdm_config["columns"]["condition_source_value_name"]
        self.condition_status_source_value = self.cdm_config["columns"]["condition_status_source_value"]
        self.patfg = self.cdm_config["columns"]["patfg"]
        self.fromdate = self.cdm_config["columns"]["fromdate"]
        self.todate = self.cdm_config["columns"]["todate"]
        
    def transform(self):
        """
        소스 데이터를 읽어들여 CDM 형식으로 변환하고 결과를 CSV 파일로 저장하는 메소드입니다.
        """
        try:
            source_data = self.process_source()
            logging.info(f"{self.table} 테이블: {len(source_data)}건")
            transformed_data = self.transform_cdm(source_data)

            # save_path = os.path.join(self.cdm_path, self.output_filename)
            self.write_csv(transformed_data, self.cdm_path, self.output_filename)

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
            provider_data = self.read_csv(self.provider_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            care_site_data = self.read_csv(self.care_site_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_data = self.read_csv(self.visit_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_detail = self.read_csv(self.visit_detail, path_type = self.cdm_flag, dtype = self.source_dtype)
            concept_etc = self.read_csv(self.concept_etc, path_type = self.source_flag, dtype = self.source_dtype)
            local_kcd = self.read_csv(self.local_kcd_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            logging.debug(f"원천 데이터 row수: {len(source)}")

            # visit_source_key 생성
            source["visit_source_key"] = source[self.person_source_value] + ';' + source[self.source_key]

            # 원천에서 조건걸기
            source[self.condition_start_datetime] = pd.to_datetime(source[self.condition_start_datetime])
            source = source[source[self.condition_start_datetime] <= pd.to_datetime(self.data_range)]
            source = source[source[self.condition_start_datetime].notna()]
            logging.debug(f"조건 적용후 원천 데이터 row수: {len(source)}")
        
            # person table과 병합
            source = pd.merge(source, person_data, left_on=self.person_source_value, right_on="person_source_value", how="inner")
            source = source.drop(columns = ["care_site_id", "provider_id"])
            logging.debug(f"person 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            # local_kcd와 병합
            local_kcd[self.fromdate] = pd.to_datetime(local_kcd[self.fromdate], errors = "coerce")
            # local_kcd[self.fromdate].fillna(pd.Timestamp('1900-01-01'), inplace = True)
            local_kcd[self.todate] = pd.to_datetime(local_kcd[self.todate],  errors = "coerce")
            # local_kcd[self.todate].fillna(pd.Timestamp('2099-12-31'), inplace = True)
            logging.debug(f"local_kcd 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            source = pd.merge(source, local_kcd, on = self.condition_source_value, how = "left", suffixes=('', '_kcd'))
            source[self.fromdate] = source[self.fromdate].fillna(pd.to_datetime('1900-01-01'))
            source[self.todate] = source[self.todate].fillna(pd.to_datetime('2099-12-31'))
            source = source[(source[self.condition_start_datetime].dt.date >= source[self.fromdate]) & (source[self.condition_start_datetime].dt.date <= source[self.todate])]
            logging.debug(f"local_kcd 테이블의 날짜 조건 적용 후 원천 데이터 row수: {len(source)}")

            # care_site table과 병합
            source = pd.merge(source, care_site_data, left_on=[self.meddept, self.hospital], right_on=["care_site_source_value", "place_of_service_source_value"], how="left")
            logging.debug(f"care_site 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            # provider table과 병합
            source = pd.merge(source, provider_data, left_on=[self.provider, self.hospital], right_on=["provider_source_value", self.hospital], how="left", suffixes=('', '_y'))
            logging.debug(f"provider 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            # visit_start_datetime 형태 변경
            visit_data["visit_start_datetime"] = pd.to_datetime(visit_data["visit_start_datetime"], errors = "coerce")
            visit_data["visit_end_datetime"] = pd.to_datetime(visit_data["visit_end_datetime"], errors = "coerce")
            
            # visit_occurrence table과 병합
            source = pd.merge(source, visit_data, left_on=["visit_source_key"], right_on=["visit_source_key"], how="left", suffixes=('', '_y'))
            logging.debug(f"visit_occurrence 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            # visit_detail table과 병합
            visit_detail = visit_detail[["visit_detail_id", "visit_detail_start_datetime", "visit_detail_end_datetime", "visit_detail_source_key"]]
            # visit_detail["visit_detail_start_datetime"] = pd.to_datetime(visit_detail["visit_detail_start_datetime"])
            # visit_detail["visit_detail_end_datetime"] = pd.to_datetime(visit_detail["visit_detail_end_datetime"])
            source = pd.merge(source, visit_detail, left_on=["visit_source_key"], right_on=["visit_detail_source_key"], how="left", suffixes=('', '_y'))
            logging.debug(f"visit_detail 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            # source["visit_detail_start_datetime"] = source["visit_detail_start_datetime"].fillna(pd.to_datetime('1900-01-01'))
            # source["visit_detail_end_datetime"] = source["visit_detail_end_datetime"].fillna(pd.to_datetime('2099-12-31'))
            # source = source[(source[self.condition_start_datetime] >= source["visit_detail_start_datetime"]) & (source[self.condition_start_datetime] <= source["visit_detail_end_datetime"])]
            # source["visit_detail_id"] = source.apply(lambda row: row['visit_detail_id'] if pd.notna(row['visit_detail_start_datetime']) and row['visit_detail_start_datetime'] <= row[self.condition_start_datetime] <= row['visit_detail_end_datetime'] else pd.NA, axis=1)
            source = source.drop(columns = ["visit_detail_start_datetime", "visit_detail_end_datetime"])
            # source = source.drop_duplicates()
            logging.debug(f"visit_detail 테이블과 결합 후 조건 적용 후 원천 데이터 row수: {len(source)}")

            # concept_etc table과 병합
            type_condition = [
                source[self.condition_type] == "Y",
                source[self.condition_type] == "N"
            ]
            type_id = [44786627, 44786629]

            source["condition_type_concept_id"] = np.select(type_condition, type_id, default = 0)
            concept_etc["concept_id"] = concept_etc["concept_id"].astype(int)
            source = pd.merge(source, concept_etc, left_on=["condition_type_concept_id"], right_on=["concept_id"], how="left", suffixes=("", "_type_concept"))
            logging.debug(f"concept_etc 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            source = source.drop_duplicates()
            logging.debug(f"중복제거 후 데이터 row수: {len(source)}")

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
            status_condition = [
                source[self.condition_status_source_value] == "Y",
                source[self.condition_status_source_value] == "N"
            ]
            status_id = [4230359, 4033240] 

            cdm = pd.DataFrame({
                "condition_occurrence_id": source.index + 1,
                "person_id": source["person_id"],
                "환자명": source["환자명"],
                "condition_concept_id": np.select([source["concept_id"].notna()],[source["concept_id"]], default = self.no_matching_concept[0]),
                "condition_start_date": source[self.condition_start_datetime].dt.date,
                "condition_start_datetime": source[self.condition_start_datetime],
                "condition_end_date": source["visit_end_datetime"].dt.date,
                "condition_end_datetime": source["visit_end_datetime"],
                "condition_type_concept_id": np.select([source["condition_type_concept_id"].notna()], [source["condition_type_concept_id"]], default= self.no_matching_concept[0]),
                "condition_type_concept_id_name": np.select([source["concept_name_type_concept"].notna()], [source["concept_name_type_concept"]], default = self.no_matching_concept[1]),
                "condition_status_concept_id": np.select(status_condition, status_id, default = self.no_matching_concept[0]),
                "stop_reason": None,
                "provider_id": source["provider_id"],
                "주치의명": source["provider_name"],
                "visit_occurrence_id": source["visit_occurrence_id"],
                "visit_detail_id": source["visit_detail_id"],
                "condition_source_value": source[self.condition_source_value],
                "진단명": source[self.condition_source_value_name],
                "condition_source_concept_id": np.select([source["concept_id"].notna()],[source["concept_id"]], default = self.no_matching_concept[0]),
                "condition_status_source_value": source[self.condition_status_source_value],
                "visit_source_key": source["visit_source_key"],
                "환자구분": source[self.patfg],
                "진료과": source[self.meddept],
                "진료과명": source["care_site_name"],
                "상병구분": None
                })

            # # datetime format 형식 맞춰주기, ns로 표기하는 값이 들어갈 수 있어서 처리함
            # cdm["condition_end_date"] = pd.to_datetime(cdm["condition_end_date"],errors = "coerce").dt.date
            # cdm["condition_end_datetime"] = pd.to_datetime(cdm["condition_end_datetime"], errors = "coerce")

            logging.debug(f"CDM 데이터 row수: {len(cdm)}")
            logging.debug(f"요약:\n{cdm.describe(include = 'all').T.to_string()}")
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

        
    def transform(self):
        """
        소스 데이터를 읽어들여 CDM 형식으로 변환하고 결과를 CSV 파일로 저장하는 메소드입니다.
        """
        try:
            transformed_data = self.process_source()

            # save_path = os.path.join(self.cdm_path, self.output_filename)
            self.write_csv(transformed_data, self.cdm_path, self.output_filename)

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
            order_data = self.read_csv(self.order_data, path_type = self.source_flag, dtype = self.source_dtype)
            concept_data = self.read_csv(self.concept_data, path_type = self.source_flag, dtype = self.source_dtype)
            logging.debug(f'원천 데이터 row수: order: {len(order_data)}')

            # 처방코드 마스터와 수가코드 매핑
            source = order_data
            logging.debug(f'처방코드, 수가코드와 결합 후 데이터 row수: {len(source)}')

            # concept_id 매핑
            source = pd.merge(source, concept_data, left_on=self.edicode, right_on="concept_code", how="left")
            logging.debug(f'concept merge후 데이터 row수: {len(source)}')

            # 코드 사용기간 없는 경우 사용기간 임의 부여
            source[self.fromdate] = source[self.fromdate].fillna(pd.to_datetime("1900-01-01").date())
            source[self.todate] = source[self.todate].fillna(pd.to_datetime("2099-12-31").date())
            
            # drug의 경우 KCD, EDI 순으로 매핑
            source = source.sort_values(by = [self.ordcode, self.fromdate, "vocabulary_id"], ascending=[True, True, False])
            source['Sequence'] = source.groupby([self.hospital_code, self.ordcode, self.fromdate]).cumcount() + 1
            source = source[source["Sequence"] == 1]
            logging.debug(f'중복되는 concept_id 제거 후 데이터 row수: {len(source)}')
            

            logging.debug(f'EDI매핑 후 데이터 row수: {len(source)}')
            # local_edi[self.edi_fromdate] = local_edi[(local_edi["FROMDATE"] >= local_edi[self.edi_fromdate]) & (local_edi["FROMDATE"] <= local_edi[self.edi_todate])]
            local_edi = source

            logging.debug(f'local_edi row수: {len(local_edi)}')
            logging.debug(f"요약:\n{local_edi.describe(include = 'all').T.to_string()}")
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
        self.meddept = self.cdm_config["columns"]["meddept"]
        self.provider = self.cdm_config["columns"]["provider"]
        self.drug_exposure_start_datetime = self.cdm_config["columns"]["drug_exposure_start_datetime"]
        self.drug_source_value = self.cdm_config["columns"]["drug_source_value"]
        self.days_supply = self.cdm_config["columns"]["days_supply"]
        self.qty = self.cdm_config["columns"]["qty"]
        self.cnt = self.cdm_config["columns"]["cnt"]
        self.route_source_value = self.cdm_config["columns"]["route_source_value"]
        self.dose_unit_source_value = self.cdm_config["columns"]["dose_unit_source_value"]
        self.patfg = self.cdm_config["columns"]["patfg"]
        self.drug_source_value_name = self.cdm_config["columns"]["drug_source_value_name"]
        self.ordseqno = self.cdm_config["columns"]["ordseqno"]
        self.atccode = self.cdm_config["columns"]["atccode"]
        self.atccodename = self.cdm_config["columns"]["atccodename"]


    def transform(self):
        """
        소스 데이터를 읽어들여 CDM 형식으로 변환하고 결과를 CSV 파일로 저장하는 메소드입니다.
        """
        try:
            source_data = self.process_source()
            transformed_data = self.transform_cdm(source_data)

            # save_path = os.path.join(self.cdm_path, self.output_filename)
            self.write_csv(transformed_data, self.cdm_path, self.output_filename)

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
            local_edi = self.read_csv(self.local_edi_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            person_data = self.read_csv(self.person_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            provider_data = self.read_csv(self.provider_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            care_site_data = self.read_csv(self.care_site_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_data = self.read_csv(self.visit_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_detail = self.read_csv(self.visit_detail, path_type = self.cdm_flag, dtype = self.source_dtype)
            concept_etc = self.read_csv(self.concept_etc, path_type = self.source_flag, dtype = self.source_dtype)

            person_data = person_data[["person_id", "person_source_value", "환자명"]]
            care_site_data = care_site_data[["care_site_id", "care_site_source_value", "care_site_name", "place_of_service_source_value"]]
            provider_data = provider_data[["provider_id", "provider_source_value", "provider_name", self.hospital]]
            visit_data = visit_data[["visit_occurrence_id", "visit_start_datetime", "care_site_id", "visit_source_value", "person_id", "visit_source_key"]]
            visit_detail = visit_detail[["visit_detail_id", "visit_detail_start_datetime", "visit_detail_end_datetime", "visit_detail_source_key"]]
            logging.info(f"원천 데이터 row수:, {len(source)}")

            # 원천에서 조건걸기
            source[self.drug_exposure_start_datetime] = pd.to_datetime(source[self.drug_exposure_start_datetime])
            source = source[(source[self.drug_exposure_start_datetime] <= pd.to_datetime(self.data_range))]
            source = source[[self.person_source_value, self.drug_source_value, self.drug_exposure_start_datetime,
                             self.meddept, self.provider, self.patfg, self.days_supply, self.qty, self.cnt,
                             self.dose_unit_source_value, self.drug_source_value_name, self.ordseqno, 
                             self.atccode, self.atccodename, self.hospital, self.source_key
                             ]]
            # visit_source_key 생성
            source["visit_source_key"] = source[self.person_source_value] + ';' + source[self.source_key]
            logging.info(f"조건 적용후 원천 데이터 row수:, {len(source)}")

            # person table과 병합
            source = pd.merge(source, person_data, left_on=self.person_source_value, right_on="person_source_value", how="inner")
            logging.info(f"person 테이블과 결합 후 데이터 row수: {len(source)}")
            
            local_edi = local_edi[[self.ordcode, self.fromdate, self.todate, self.edicode, "concept_id", self.hospital_code]]
            local_edi[self.fromdate] = pd.to_datetime(local_edi[self.fromdate] , errors="coerce")
            local_edi[self.todate] = pd.to_datetime(local_edi[self.todate] , errors="coerce")

            # LOCAL코드와 EDI코드 매핑 테이블과 병합
            source = pd.merge(source, local_edi, left_on=[self.drug_source_value, self.hospital], right_on=[self.ordcode, self.hospital_code], how="left")
            source[self.fromdate] = source[self.fromdate].fillna(pd.Timestamp('1900-01-01'))
            source[self.todate] = source[self.todate].fillna(pd.Timestamp('2099-12-31'))
            logging.info(f"local_edi와 병합 후 데이터 row수:, {len(source)}")
            source = source[(source[self.drug_exposure_start_datetime] >= source[self.fromdate]) & (source[self.drug_exposure_start_datetime] <= source[self.todate])]
            logging.info(f"local_edi날짜 조건 적용 후 데이터 row수: {len(source)}")

            # care_site table과 병합
            source = pd.merge(source, care_site_data, left_on=[self.meddept, self.hospital], right_on=["care_site_source_value", "place_of_service_source_value"], how="left")
            logging.info(f"care_site 테이블과 결합 후 데이터 row수: {len(source)}")

            # provider table과 병합
            source = pd.merge(source, provider_data, left_on=[self.provider, self.hospital], right_on=["provider_source_value", self.hospital], how="left", suffixes=('', '_y'))
            logging.info(f"provider 테이블과 결합 후 데이터 row수: {len(source)}")

            # visit_start_datetime 형태 변경
            visit_data["visit_start_datetime"] = pd.to_datetime(visit_data["visit_start_datetime"])
            
            # visit_occurrence table과 병합
            source = pd.merge(source, visit_data, left_on=["visit_source_key"], right_on=["visit_source_key"], how="left", suffixes=('', '_y'))
            logging.info(f"visit_occurrence 테이블과 결합 후 데이터 row수: {len(source)}")

            # visit_detail table과 병합
            # visit_detail["visit_detail_start_datetime"] = pd.to_datetime(visit_detail["visit_detail_start_datetime"])
            # visit_detail["visit_detail_end_datetime"] = pd.to_datetime(visit_detail["visit_detail_end_datetime"])
            source = pd.merge(source, visit_detail, left_on=["visit_source_key"], right_on=["visit_detail_source_key"], how="left", suffixes=('', '_y'))
            logging.debug(f"visit_detail 테이블과 결합 후 원천 데이터 row수: {len(source)}")
            
            # source["visit_detail_start_datetime"] = source["visit_detail_start_datetime"].fillna(pd.to_datetime('1900-01-01'))
            # source["visit_detail_end_datetime"] = source["visit_detail_end_datetime"].fillna(pd.to_datetime('2099-12-31'))
            # source = source[(source[self.drug_exposure_start_datetime] >= source["visit_detail_start_datetime"]) & (source[self.drug_exposure_start_datetime] <= source["visit_detail_end_datetime"])]
            # source["visit_detail_id"] = source.apply(lambda row: row['visit_detail_id'] if pd.notna(row['visit_detail_start_datetime']) and row['visit_detail_start_datetime'] <= row[self.drug_exposure_start_datetime] <= row['visit_detail_end_datetime'] else pd.NA, axis=1)
            source = source.drop(columns = ["visit_detail_start_datetime", "visit_detail_end_datetime"])
            # source = source.drop_duplicates(subset=["person_id", self.drug_exposure_start_datetime, self.patfg, self.drug_source_value, self.medtime, self.ordseqno, self.meddept])
            logging.debug(f"visit_detail 테이블과 결합 후 조건 적용 후 원천 데이터 row수: {len(source)}")

            # # care_site_id가 없는 경우 0으로 값 입력
            # source.loc[source["care_site_id"].isna(), "care_site_id"] = 0
            source.loc[source["concept_id"].isna(), "concept_id"] = 0

            # drug_type_concept_id_name가져오기
            source["drug_type_concept_id"] = 38000177
            concept_etc["concept_id"] = concept_etc["concept_id"].astype(int)
            source = pd.merge(source, concept_etc, left_on = "drug_type_concept_id", right_on="concept_id", how="left", suffixes=('', '_drug_type'))
            logging.info(f"concept_etc 테이블과 결합 후 데이터 row수: {len(source)}")
            
            source = source.drop_duplicates()
            logging.debug(f"중복제거 후 데이터 row수: {len(source)}")

            
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
            "환자명": source["환자명"],
            "drug_concept_id": np.select([source["concept_id"].notna()], [source["concept_id"]], default=self.no_matching_concept[0]),
            "drug_exposure_start_date": source[self.drug_exposure_start_datetime].dt.date,
            "drug_exposure_start_datetime": source[self.drug_exposure_start_datetime],
            "drug_exposure_end_date": source[self.drug_exposure_start_datetime].dt.date + pd.to_timedelta(source[self.days_supply].astype(int) - 1, unit="days"),
            "drug_exposure_end_datetime": source[self.drug_exposure_start_datetime] + pd.to_timedelta(source[self.days_supply].astype(int) - 1, unit="days"),
            "verbatim_end_date": None,
            "drug_type_concept_id": np.select([source["drug_type_concept_id"].notna()], [source["drug_type_concept_id"]], default= self.no_matching_concept[0]),
            "drug_type_concept_id_name": np.select([source["concept_name"].notna()], [source["concept_name"]], default = self.no_matching_concept[1]),
            "stop_reason": None,
            "refills": None,
            "quantity": source[self.days_supply].astype(int) * source[self.qty].astype(float) * source[self.cnt].astype(float),
            "days_supply": source[self.days_supply].astype(int),
            "sig": None,
            "route_concept_id": self.no_matching_concept[0],
            "lot_number": None,
            "provider_id": source["provider_id"],
            "처방의명": source["provider_name"],
            "visit_occurrence_id": source["visit_occurrence_id"],
            "visit_detail_id": source["visit_detail_id"],
            "drug_source_value": source[self.drug_source_value],
            "drug_source_value_name": source[self.drug_source_value_name],
            "EDI코드": source[self.edicode],
            "drug_source_concept_id": np.select([source["concept_id"].notna()], [source["concept_id"]], default=self.no_matching_concept[0]),
            "route_source_value": None,
            "dose_unit_source_value": source[self.dose_unit_source_value],
            "vocabulary_id": "EDI",
            "visit_source_key": source["visit_source_key"],
            "환자구분": source[self.patfg],
            "진료과": source[self.meddept],
            "진료과명": source["care_site_name"],
            "진료일시": None,
            "나이": None,
            "투여량": source[self.qty],
            "함량단위": source[self.dose_unit_source_value],
            "횟수": source[self.cnt],
            "일수": source[self.days_supply],
            "용법코드": None,
            "처방순번": source[self.ordseqno],
            "ATC코드": source[self.atccode],
            "ATC 코드명": source[self.atccodename]
            # ,"dcyn": source[self.dcyn]
            })

            logging.info(f"CDM테이블 row수: {len(cdm)}")
            logging.debug(f"요약:\n{cdm.describe(include = 'all').T.to_string()}")
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
        self.meddept = self.cdm_config["columns"]["meddept"]
        self.provider = self.cdm_config["columns"]["provider"]
        self.orddate = self.cdm_config["columns"]["orddate"]
        self.exectime = self.cdm_config["columns"]["exectime"]
        self.measurement_date = self.cdm_config["columns"]["measurement_date"]
        self.measurement_source_value = self.cdm_config["columns"]["measurement_source_value"]
        self.value_source_value = self.cdm_config["columns"]["value_source_value"]
        self.unit_source_value = self.cdm_config["columns"]["unit_source_value"]
        self.result_range = self.cdm_config["columns"]["result_range"]
        self.patfg = self.cdm_config["columns"]["patfg"]
        self.ordseqno = self.cdm_config["columns"]["ordseqno"]
        self.처방명 = self.cdm_config["columns"]["처방명"]
        self.acptdt = self.cdm_config["columns"]["acptdt"]
        self.reptdt = self.cdm_config["columns"]["reptdt"]
        
    def transform(self):
        """
        소스 데이터를 읽어들여 CDM 형식으로 변환하고 결과를 CSV 파일로 저장하는 메소드입니다.
        """
        try : 
            source_data = self.process_source()
            transformed_data = self.transform_cdm(source_data)

            # save_path = os.path.join(self.cdm_path, self.output_filename)
            self.write_csv(transformed_data, self.cdm_path, self.output_filename)

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
            local_edi = self.read_csv(self.local_edi_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            person_data = self.read_csv(self.person_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            provider_data = self.read_csv(self.provider_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            care_site_data = self.read_csv(self.care_site_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_data = self.read_csv(self.visit_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_detail = self.read_csv(self.visit_detail, path_type = self.cdm_flag, dtype = self.source_dtype)
            unit_data = self.read_csv(self.concept_unit, path_type = self.source_flag , dtype = self.source_dtype)
            concept_etc = self.read_csv(self.concept_etc, path_type = self.source_flag , dtype = self.source_dtype)
            unit_concept_synonym = self.read_csv(self.unit_concept_synonym, path_type = self.source_flag , dtype = self.source_dtype)
            logging.debug(f'원천 데이터 row수: {len(source)}')

            # 원천에서 조건걸기
            source = source[[self.source_key, self.hospital, self.person_source_value, self.measurement_date,
                            self.measurement_source_value, self.value_source_value, self.unit_source_value,
                            self.result_range, self.meddept, self.provider, self.orddate, self.patfg, 
                            self.ordseqno, self.처방명, self.acptdt, self.exectime, self.reptdt
                            ]]
            source["실시일시"] = source[self.measurement_date]
            source["처방일"] = source[self.orddate]
            source[self.measurement_date] = pd.to_datetime(source[self.measurement_date])
            source[self.orddate] = pd.to_datetime(source[self.orddate])
            source = source[source[self.measurement_date] <= pd.to_datetime(self.data_range)]
            
            source = source[source[self.measurement_date] <= pd.to_datetime(self.data_range)]
            
            # visit_source_key 생성
            source["visit_source_key"] = source[self.person_source_value] + ';' + source[self.source_key]
           
            # value_as_number float형태로 저장되게 값 변경
            # source["value_as_number"] = source[self.value_source_value].str.extract('(-?\d+\.\d+|\d+)')
            source["value_as_number"] = source[self.value_source_value].apply(convert_to_numeric)
            source["value_as_number"] = source["value_as_number"].astype(float)
            # source[self.range_low] = source[self.range_low].str.extract('(-?\d+\.\d+|\d+)')
            # source[self.range_high] = source[self.range_high].str.extract('(-?\d+\.\d+|\d+)')
            source["range_low"] = source[self.result_range].apply(lambda x: str(x).split('~')[0] if isinstance(x, str) and '~' in x else None)
            source["range_low"] = pd.to_numeric(source["range_low"], errors = "coerce")
            source["range_high"] = source[self.result_range].apply(lambda x: str(x).split('~')[1] if isinstance(x, str) and '~' in x else None)
            source["range_high"] = pd.to_numeric(source["range_high"], errors='coerce')

            logging.debug(f'조건적용 후 원천 데이터 row수: {len(source)}')
            
            # person table과 병합
            person_data = person_data[["person_id", "person_source_value", "환자명"]]
            source = pd.merge(source, person_data, left_on=self.person_source_value, right_on="person_source_value", how="inner")
            del person_data
            logging.debug(f'person 테이블과 결합 후 데이터 row수: {len(source)}')

            # local_edi 전처리
            local_edi = local_edi[[self.ordcode, self.fromdate, self.todate, self.edicode, "concept_id", self.ordname, self.hospital_code]]
            local_edi[self.fromdate] = pd.to_datetime(local_edi[self.fromdate], errors="coerce")
            local_edi[self.todate] = pd.to_datetime(local_edi[self.todate], errors="coerce")
            
            # LOCAL코드와 EDI코드 매핑 테이블과 병합
            source = pd.merge(source, local_edi, left_on=[self.measurement_source_value, self.hospital], right_on=[self.ordcode, self.hospital_code], how="left", suffixes=["", "_local_edi"])
            del local_edi
            source[self.fromdate] = source[self.fromdate].fillna(pd.to_datetime('1900-01-01').date())
            source[self.todate] = source[self.todate].fillna(pd.to_datetime('2099-12-31').date())
            logging.debug(f'EDI코드 테이블과 병합 후 데이터 row수: {len(source)}')
            source = source[(source[self.orddate] >= source[self.fromdate]) & (source[self.orddate] <= source[self.todate])]
            logging.debug(f"EDI코드 사용기간별 필터 적용 후 데이터 row수: {len(source)}")

            # care_site table과 병합
            care_site_data = care_site_data[["care_site_id", "care_site_source_value", "care_site_name", "place_of_service_source_value"]]
            source = pd.merge(source, care_site_data, left_on=[self.meddept, self.hospital], right_on=["care_site_source_value", "place_of_service_source_value"], how="left")
            del care_site_data
            logging.debug(f'care_site 테이블과 결합 후 데이터 row수: {len(source)}')

            # provider table과 병합
            provider_data = provider_data[["provider_id", "provider_source_value", "provider_name", self.hospital]]
            source = pd.merge(source, provider_data, left_on=[self.provider, self.hospital], right_on=["provider_source_value", self.hospital], how="left", suffixes=('', '_y'))
            logging.debug(f'provider 테이블과 결합 후 데이터 row수: {len(source)}')

            # visit_start_datetime 형태 변경
            visit_data["visit_start_datetime"] = pd.to_datetime(visit_data["visit_start_datetime"])

            # visit_occurrence table과 병합
            visit_data = visit_data[["visit_occurrence_id", "visit_start_datetime", "care_site_id", "visit_source_value", "person_id", "visit_source_key"]]
            source = pd.merge(source, visit_data, left_on=["visit_source_key"], right_on=["visit_source_key"], how="left", suffixes=('', '_y'))
            del visit_data
            logging.debug(f'visit_occurrence 테이블과 결합 후 데이터 row수: {len(source)}')

            # visit_detail table과 병합
            visit_detail = visit_detail[["visit_detail_id", "visit_detail_start_datetime", "visit_detail_end_datetime", "visit_detail_source_key"]]
            source = pd.merge(source, visit_detail, left_on=["visit_source_key"], right_on=["visit_detail_source_key"], how="left", suffixes=('', '_y'))
            logging.debug(f"visit_detail 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            ### unit매핑 작업 ###
            # concept_unit과 병합
            unit_data = unit_data[["concept_id", "concept_name", "concept_code"]]
            source = pd.merge(source, unit_data, left_on=self.unit_source_value, right_on="concept_code", how="left", suffixes=["", "_unit"])
            logging.debug(f'unit 테이블과 결합 후 데이터 row수: {len(source)}')
            # unit 동의어 적용
            source = pd.merge(source, unit_concept_synonym, left_on = self.unit_source_value, right_on = "concept_synonym_name", how = "left", suffixes=["", "_synonym"])
            logging.debug(f'unit synonym 테이블과 결합 후 데이터 row수: {len(source)}')
            

            ### concept_etc테이블과 병합 ###
            concept_etc = concept_etc[["concept_id", "concept_name"]]
            concept_etc["concept_id"] = concept_etc["concept_id"].astype(int)            

            # type_concept_id 만들고 type_concept_id_name 기반 만들기
            source["measurement_type_concept_id"] = 44818702
            source = pd.merge(source, concept_etc, left_on = "measurement_type_concept_id", right_on="concept_id", how="left", suffixes=('', '_measurement_type'))
            logging.debug(f'concept_etc: type_concept_id 테이블과 결합 후 데이터 row수: {len(source)}')

            # operator_concept_id 만들고 operator_concept_id_name 기반 만들기
            operator_condition = [
                source[self.value_source_value].isin([">"])
                , source[self.value_source_value].isin([">="])
                , source[self.value_source_value].isin(["="])
                , source[self.value_source_value].isin(["<="])
                , source[self.value_source_value].isin(["<"])
                    # source[self.value_source_value].str.contains(">", case = False, na = False)
                    # , source[self.value_source_value].str.contains(">=", case = False, na = False)
                    # , source[self.value_source_value].str.contains("=", case = False, na = False)
                    # , source[self.value_source_value].str.contains("<=", case = False, na = False)
                    # , source[self.value_source_value].str.contains("<", case = False, na = False)
            ]
            operator_value = [
                    4172704
                    , 4171755
                    , 4172703
                    , 4171754
                    , 4171756
            ]

            source["operator_concept_id"] = np.select(operator_condition, operator_value)
            source = pd.merge(source, concept_etc, left_on = "operator_concept_id", right_on="concept_id", how="left", suffixes=('', '_operator'))
            logging.debug(f'concept_etc: operator_concept_id 테이블과 결합 후 데이터 row수: {len(source)}')

            # value_as_concept_id 만들고 value_as_concept_id_name 기반 만들기
            value_concept_condition = [
                source[self.value_source_value] == "+",
                source[self.value_source_value] == "++",
                source[self.value_source_value] == "+++",
                source[self.value_source_value] == "++++",
                source[self.value_source_value].str.lower().str.strip() == "negative",
                source[self.value_source_value].str.lower().str.strip() == "positive"
            ]
            value_concept_value = [
                4123508
                , 4126673
                , 4125547
                , 4126674
                , 9189
                , 9191
            ]
            source["value_as_concept_id"] = np.select(value_concept_condition, value_concept_value)
            source = pd.merge(source, concept_etc, left_on = "value_as_concept_id", right_on="concept_id", how="left", suffixes=('', '_value_as_concept'))
            logging.debug(f'concept_etc: value_as_concept_id 테이블과 결합 후 데이터 row수: {len(source)}')
            
            source = source.drop_duplicates()
            logging.debug(f"중복제거 후 데이터 row수: {len(source)}")

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
            unit_id_condition = [
                source["concept_id_unit"].notna(),
                source["concept_id_synonym"].notna()
            ]

            unit_id_value = [
                source["concept_id_unit"],
                source["concept_id_synonym"]
            ]

            unit_name_condition = [
                source["concept_id_unit"].notna(),
                source["concept_id_synonym"].notna()
            ]

            unit_name_value = [
                source["concept_name"],
                source["concept_name_synonym"]
            ]

            cdm = pd.DataFrame({
                "measurement_id": source.index + 1,
                "person_id": source["person_id"],
                "환자명": source["환자명"],
                "measurement_concept_id": np.select([source["concept_id"].notna()], [source["concept_id"]], default=self.no_matching_concept[0]),
                "measurement_date": source[self.measurement_date].dt.date,
                "measurement_datetime": source[self.measurement_date],
                "measurement_time": source[self.measurement_date].dt.time,
                # "measurement_date_type": np.select(measurement_date_condition, ["검사일"], default="처방일"),
                "measurement_type_concept_id": source["measurement_type_concept_id"],
                "measurement_type_concept_id_name": source["concept_name_measurement_type"],
                "operator_concept_id": np.select([source["operator_concept_id"].notna()], [source["operator_concept_id"]], default = self.no_matching_concept[0]),
                "operator_concept_id_name": np.select([source["concept_name_operator"].notna()], [source["concept_name_operator"]], default=self.no_matching_concept[1]) ,
                "value_as_number": source["value_as_number"],
                "value_as_concept_id": np.select([source["value_as_concept_id"].notna()], [source["value_as_concept_id"]], default = self.no_matching_concept[0]),
                "value_as_concept_id_name": np.select([source["concept_name_value_as_concept"].notna()], [source["concept_name_value_as_concept"]], default=self.no_matching_concept[1]) ,
                "unit_concept_id": np.select(unit_id_condition, unit_id_value, default=self.no_matching_concept[0]),
                "unit_concept_id_name": np.select(unit_name_condition, unit_name_value, default=self.no_matching_concept[1]),
                "range_low": source["range_low"],
                "range_high": source["range_high"],
                "provider_id": source["provider_id"],
                "처방의명": source["provider_name"],
                "visit_occurrence_id": source["visit_occurrence_id"],
                "visit_detail_id": source["visit_detail_id"],
                "measurement_source_value": source[self.measurement_source_value],
                "measurement_source_value_name": source[self.ordname],
                "EDI코드": source[self.edicode],
                "measurement_source_concept_id": source["concept_id"],
                "unit_source_value": source[self.unit_source_value],
                "value_source_value": source[self.value_source_value],
                "vocabulary_id": "EDI",
                "visit_source_key": source["visit_source_key"],
                "처방코드": source[self.measurement_source_value],
                "처방명": source[self.처방명],
                "환자구분": source[self.patfg],
                "진료과": source[self.meddept],
                "진료과명": source["care_site_name"],
                "처방일": source["처방일"],
                "진료일시": None,
                "접수일시": source[self.acptdt],
                "실시일시": source["실시일시"],
                "판독일시": source[self.reptdt],
                "보고일시": source[self.reptdt],
                "처방순번": source[self.ordseqno],
                "정상치(상)": source[self.result_range],
                "정상치(하)": source[self.result_range],
                # "나이": source[self.age],
                "결과내역": source[self.value_source_value]
                })

            logging.debug(f'CDM 데이터 row수: {len(cdm)}')
            logging.debug(f"요약:\n{cdm.describe(include = 'all').T.to_string()}")
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
        self.meddept = self.cdm_config["columns"]["meddept"]
        self.provider = self.cdm_config["columns"]["provider"]
        self.orddate = self.cdm_config["columns"]["orddate"]
        self.opdate = self.cdm_config["columns"]["opdate"]
        self.procedure_date = self.cdm_config["columns"]["procedure_date"]
        self.procedure_source_value = self.cdm_config["columns"]["procedure_source_value"]
        self.procedure_source_value_name = self.cdm_config["columns"]["procedure_source_value_name"]
        self.patfg = self.cdm_config["columns"]["patfg"]
    
        
    def transform(self):
        """
        소스 데이터를 읽어들여 CDM 형식으로 변환하고 결과를 CSV 파일로 저장하는 메소드입니다.
        """
        try:
            source_data = self.process_source()
            transformed_data = self.transform_cdm(source_data)

            # save_path = os.path.join(self.cdm_path, self.output_filename)
            self.write_csv(transformed_data, self.cdm_path, self.output_filename)

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
            local_edi = self.read_csv(self.local_edi_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            person_data = self.read_csv(self.person_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            provider_data = self.read_csv(self.provider_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            care_site_data = self.read_csv(self.care_site_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_data = self.read_csv(self.visit_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_detail = self.read_csv(self.visit_detail, path_type = self.cdm_flag, dtype = self.source_dtype)
            concept_etc = self.read_csv(self.concept_etc, path_type = self.source_flag, dtype = self.source_dtype)
            logging.debug(f"원천 데이터 row수: {len(source)}")

            # 원천에서 조건걸기
            source = source[[self.person_source_value, self.orddate, self.opdate, self.procedure_source_value,
                              self.meddept, self.provider, self.patfg, self.procedure_source_value_name,
                              self.hospital, self.source_key, self.procedure_date]]
            # visit_source_key 생성
            source["visit_source_key"] = source[self.person_source_value] + ';' + source[self.source_key]

            source["처방일"] = source[self.orddate]
            source["수술일"] = source[self.opdate]
            
            source[self.orddate] = pd.to_datetime(source[self.orddate])
            source[self.opdate] = pd.to_datetime(source[self.opdate])
            source[self.procedure_date] = pd.to_datetime(source[self.procedure_date])
            source = source[(source[self.orddate] <= pd.to_datetime(self.data_range))]

            logging.debug(f"조건적용 후 원천 데이터 row수: {len(source)}")
    
            local_edi = local_edi[[self.ordcode, self.fromdate, self.todate, self.edicode, "concept_id", self.hospital_code]]
            local_edi[self.fromdate] = pd.to_datetime(local_edi[self.fromdate], errors="coerce")
            local_edi[self.todate] = pd.to_datetime(local_edi[self.todate], errors="coerce")
            
            # person table과 병합
            source = pd.merge(source, person_data, left_on=self.person_source_value, right_on="person_source_value", how="inner")
            source = source.drop(columns = ["care_site_id", "provider_id"])
            logging.debug(f'person 테이블과 결합 후 데이터 row수, {len(source)}')
            
            # LOCAL코드와 EDI코드 매핑 테이블과 병합
            source = pd.merge(source, local_edi, left_on=[self.procedure_source_value, self.hospital], right_on=[self.ordcode, self.hospital_code], how="left")
            source[self.fromdate] = source[self.fromdate].fillna(pd.to_datetime('1900-01-01'))
            source[self.todate] = source[self.todate].fillna(pd.to_datetime('2099-12-31'))
            logging.debug(f'local_edi 테이블과 결합 후 데이터 row수: {len(source)}')
            source = source[(source[self.orddate] >= source[self.fromdate]) & (source[self.orddate] <= source[self.todate])]
            logging.debug(f"local_edi 사용기간별 필터 적용 후 데이터 row수: {len(source)}")

            # care_site table과 병합
            source = pd.merge(source, care_site_data, left_on=[self.meddept, self.hospital], right_on=["care_site_source_value", "place_of_service_source_value"], how="left")
            logging.debug(f'care_site 테이블과 결합 후 데이터 row수, {len(source)}')

            # provider table과 병합
            source = pd.merge(source, provider_data, left_on=[self.provider, self.hospital], right_on=["provider_source_value", self.hospital], how="left", suffixes=('', '_y'))
            logging.debug(f'provider 테이블과 결합 후 데이터 row수, {len(source)}')

            # visit_start_datetime 형태 변경
            visit_data = visit_data[["visit_occurrence_id", "visit_start_datetime", "visit_source_key"]]
            visit_data["visit_start_datetime"] = pd.to_datetime(visit_data["visit_start_datetime"])

            # visit_occurrence table과 병합
            source = pd.merge(source, visit_data, left_on=["visit_source_key"], right_on=["visit_source_key"], how="left", suffixes=('', '_y'))
            logging.debug(f'visit_occurrence 테이블과 결합 후 데이터 row수, {len(source)}')

            # visit_detail table과 병합
            visit_detail = visit_detail[["visit_detail_id", "visit_detail_source_key"]]
            source = pd.merge(source, visit_detail, left_on=["visit_source_key"], right_on=["visit_detail_source_key"], how="left", suffixes=('', '_y'))
            logging.debug(f"visit_detail 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            ### concept_etc테이블과 병합 ###
            concept_etc["concept_id"] = concept_etc["concept_id"].astype(int)            

            # type_concept_id 만들고 type_concept_id_name 기반 만들기
            source["procedure_type_concept_id"] = 38000275
            source = pd.merge(source, concept_etc, left_on = "procedure_type_concept_id", right_on="concept_id", how="left", suffixes=('', '_procedure_type'))

            source = source.drop_duplicates()
            logging.debug(f"중복제거 후 데이터 row수: {len(source)}")

            # 값이 없는 경우 0으로 값 입력
            # source.loc[source["care_site_id"].isna(), "care_site_id"] = 0
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
            procedure_date_condition = [ source[self.procedure_date].notna()
                                        , source[self.opdate].notna()
                                        , source[self.orddate].notna()
                                        ]
            procedure_date_value = [ source[self.procedure_date].dt.date
                                    , source[self.opdate].dt.date
                                    , source[self.orddate].dt.date
                                    ]
            procedure_datetime_value = [ source[self.procedure_date]
                                    , source[self.opdate]
                                    , source[self.orddate]
                                    ]
                                    
            cdm = pd.DataFrame({
                "procedure_occurrence_id": source.index + 1,
                "person_id": source["person_id"],
                "환자명": source["환자명"],
                "procedure_concept_id": np.select([source["concept_id"].notna()], [source["concept_id"]], default=self.no_matching_concept[0]), 
                "procedure_date": np.select(procedure_date_condition, procedure_date_value, default = source[self.orddate].dt.date),
                "procedure_datetime": np.select(procedure_date_condition, procedure_datetime_value, default = source[self.orddate]),
                "procedure_date_type": np.select(procedure_date_condition, [self.procedure_date, self.opdate, self.orddate], default = self.orddate),
                "procedure_type_concept_id": np.select([source["procedure_type_concept_id"].notna()], [source["procedure_type_concept_id"]], default=self.no_matching_concept[0]),
                "procedure_type_concept_id_name": np.select([source["concept_name"].notna()], [source["concept_name"]], default=self.no_matching_concept[1]),
                "modifier_concept_id": None,
                "quantity": None,
                "provider_id": source["provider_id"],
                "처방의명": source["provider_name"],
                "visit_occurrence_id": source["visit_occurrence_id"],
                "visit_detail_id": source["visit_detail_id"],
                "procedure_source_value": source[self.procedure_source_value],
                "procedure_source_value_name": source[self.procedure_source_value_name],
                "EDI코드": source[self.edicode],
                "procedure_source_concept_id": source["concept_id"],
                "modifier_source_value": None ,
                "vocabulary_id": "EDI",
                "visit_source_key": source["visit_source_key"],
                "처방코드": source[self.procedure_source_value],
                "처방명": source[self.procedure_source_value_name],
                "환자구분": source[self.patfg],
                "진료과": source[self.meddept],
                "진료과명": source["care_site_name"],
                "나이": None,
                "처방일": source["처방일"],
                "수술일": source["수술일"],
                "진료일시": None,
                "접수일시": None,
                "실시일시": source[self.procedure_date],
                "판독일시": None,
                "보고일시": None,
                "결과내역": None, 
                "결론 및 진단": None,
                "결과단위": None
                })

            cdm["procedure_date"] = pd.to_datetime(cdm["procedure_date"])
            cdm["procedure_datetime"] = pd.to_datetime(cdm["procedure_datetime"])

            logging.debug(f'CDM 데이터 row수, {len(cdm)}')
            logging.debug(f"요약:\n{cdm.describe(include = 'all').T.to_string()}")
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

        # save_path = os.path.join(self.cdm_path, self.output_filename)
        self.write_csv(transformed_data, self.cdm_path, self.output_filename)

        logging.info(f"{self.table} 테이블 변환 완료")
        logging.info(f"============================")

    def process_source(self):
        """
        소스 데이터를 로드하고 전처리 작업을 수행하는 메소드입니다.
        """
        try:
            visit_data = self.read_csv(self.visit, path_type = self.cdm_flag, dtype = self.source_dtype)
            condition_data = self.read_csv(self.condition, path_type = self.cdm_flag, dtype = self.source_dtype)
            drug_data = self.read_csv(self.drug, path_type = self.cdm_flag, dtype = self.source_dtype)
            measurement_data = self.read_csv(self.measurement, path_type = self.cdm_flag, dtype = self.source_dtype)
            procedure_data = self.read_csv(self.procedure, path_type = self.cdm_flag, dtype = self.source_dtype)

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
            logging.debug(f"요약:\n{cdm.describe(include = 'all').T.to_string()}")
            logging.debug(f"컬럼별 null 개수:\n{cdm.isnull().sum().to_string()}")

            return cdm

        except Exception as e :
            logging.error(f"{self.table} 테이블 소스 데이터 처리 중 오류: {e}", exc_info = True)