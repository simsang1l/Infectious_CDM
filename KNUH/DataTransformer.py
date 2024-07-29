import pandas as pd
import numpy as np
import yaml
import os
from datetime import datetime
import logging
import warnings
import inspect

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
        self.measurement_edi_data = self.config["measurement_edi_data"]
        self.procedure_edi_data = self.config["procedure_edi_data"]
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
        self.hospital = self.config["hospital"]
        self.drug_edi_data = self.config["drug_edi_data"]
        # self.sugacode = self.config["sugacode"]
        self.edicode = self.config["edicode"]
        self.fromdate = self.config["fromdate"]
        self.todate = self.config["todate"]
        self.frstrgstdt = self.config["frstrgstdt"]
        self.concept_etc = self.config["concept_etc"]
        self.unit_concept_synonym = self.config["unit_concept_synonym"]
        self.visit_no = self.config["visit_no"]
        self.diag_condition = self.config["diag_condition"]
        self.no_matching_concept = self.config["no_matching_concept"]
        self.concept_kcd = self.config["concept_kcd"]
        self.local_kcd_data = self.config["local_kcd_data"]
        self.hospital_code = self.config["hospital_code"]
        self.care_site_fromdate = self.config["care_site_fromdate"]
        self.care_site_todate = self.config["care_site_todate"]

        # 의료기관이 여러개인 경우 의료기관 코드 폴더 생성
        os.makedirs(os.path.join(self.cdm_path, self.hospital_code), exist_ok = True)
        # 상병조건이 있다면 조건에 맞는 폴더 생성
        os.makedirs(os.path.join(self.cdm_path, self.hospital_code, self.diag_condition), exist_ok = True)

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
        hospital_code = self.hospital_code
        if path_type == "source":
            full_path = os.path.join(self.config["source_path"], file_name + ".csv")
            default_encoding = self.source_encoding
            
        elif path_type == "CDM":
            if hospital_code :
                full_path = os.path.join(self.config["CDM_path"], hospital_code, self.diag_condition, file_name + ".csv")
            elif self.diag_condition:
                full_path = os.path.join(self.config["CDM_path"], self.diag_condition, file_name + ".csv")
            else :
                full_path = os.path.join(self.config["CDM_path"], file_name + ".csv")
            default_encoding = self.cdm_encoding
        else :
            raise ValueError(f"Invalid path type: {path_type}")
        
        encoding = encoding if encoding else default_encoding

        return pd.read_csv(full_path, dtype = dtype, encoding = encoding)

    def write_csv(self, df, file_path, filename, encoding = 'utf-8', hospital_code = None):
        """
        DataFrame을 CSV 파일로 저장합니다.
        """
        encoding = self.cdm_encoding
        hospital_code = self.hospital_code
        if self.diag_condition:
            df.to_csv(os.path.join(file_path, hospital_code, self.diag_condition, filename + ".csv"), encoding = encoding, index = False)
        else:
            df.to_csv(os.path.join(file_path, hospital_code, filename + ".csv"), encoding = encoding, index = False)

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

            location = self.read_csv(self.location_data, path_type = self.source_flag, dtype = self.source_dtype, encoding=self.cdm_encoding)
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
                "place_of_service_source_value": source_data[self.place_of_service_source_value],
                self.care_site_fromdate: source_data[self.care_site_fromdate],
                self.care_site_todate: source_data[self.care_site_todate]
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
            # concept_etc = self.read_csv(self.concept_etc, path_type = self.cdm_flag, dtype = self.source_dtype)
            logging.debug(f"원천 데이터 row수: {len(source_data)}")

            source = pd.merge(source_data,
                            care_site,
                            left_on = self.care_site_source_value,
                            right_on = "care_site_source_value",
                            how = "left")
            
            logging.debug(f"CDM 결합 후 원천 데이터 row수: {len(source_data)}")
        
            return source

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
                "provider_id" : 1, #source_data.index + 1,
                "provider_name": source_data[self.provider_name],
                "npi": None,
                "dea": None,
                "specialty_concept_id": None,
                "specialty_concept_id_name": None,
                "care_site_id": source_data["care_site_id"],
                "care_site_id_name": source_data["care_site_name"],
                "year_of_birth": None,
                "gender_concept_id": np.select(gender_conditions, gender_concept_id, default = 0),
                "provider_source_value": source_data[self.provider_source_value],
                "specialty_source_value": source_data[self.specialty_source_value],
                "specialty_source_value_name": None,
                "specialty_source_concept_id": 0,
                "gender_source_value": source_data[self.gender_source_value],
                "gender_source_concept_id": np.select(gender_conditions, gender_concept_id, default = 0),
                })
            cdm = cdm.drop_duplicates()
            cdm.reset_index(drop=True, inplace = True)

            cdm["provider_id"] = cdm.index +  1

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
        self.output_filename = self.cdm_config["data"]["output_filename"]
        self.location_source_value = self.cdm_config["columns"]["location_source_value"]
        self.gender_source_value = self.cdm_config["columns"]["gender_source_value"]
        self.death_datetime = self.cdm_config["columns"]["death_datetime"]
        self.birth_datetime = self.cdm_config["columns"]["birth_datetime"]
        self.race_source_value = self.cdm_config["columns"]["race_source_value"]
        self.person_name = self.cdm_config["columns"]["person_name"]
        self.abotyp = self.cdm_config["columns"]["abotyp"]
        self.rhtyp = self.cdm_config["columns"]["rhtyp"]
        self.source_condition = self.cdm_config["data"]["source_condition"]
        self.diagcode = self.cdm_config["columns"]["diagcode"]
        self.ruleout = self.cdm_config["columns"]["ruleout"]


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
            location_data = self.read_csv(self.location_data, path_type = self.source_flag, dtype = self.source_dtype, encoding=self.cdm_encoding)
            logging.debug(f"원천 데이터 row수: {len(source_data)}")
            
            source_data = pd.merge(source_data, location_data, left_on = self.location_source_value, right_on="LOCATION_SOURCE_VALUE", how = "left")
            source_data.loc[source_data["LOCATION_ID"].isna(), "LOCATION_ID"] = None
            logging.debug(f"location 테이블과 결합 후 원천 데이터1 row수: {len(source_data)}")

            # 상병조건이 있는 경우
            if self.diag_condition:
                condition = self.read_csv(self.source_condition, path_type=self.source_flag, dtype=self.source_dtype)
                # self.ruleout 컬럼이 R인경우 RULEOUT
                condition = condition[condition[self.diagcode].str.startswith(self.diag_condition, na=False) & (condition[self.ruleout] == 'C')] 
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
                source[self.race_source_value] == 'N',
                source[self.race_source_value] == 'Y'
            ]
            race_concept_id = [38003585, 8552]

            gender_conditions = [
                source[self.gender_source_value].isin(['M']),
                source[self.gender_source_value].isin(['F'])
            ]
            gender_concept_id = [8507, 8532]

            cdm = pd.DataFrame({
                "person_id" : source.index + 1,
                "gender_concept_id": np.select(gender_conditions, gender_concept_id, default = self.no_matching_concept[0]),
                "year_of_birth": source[self.birth_datetime].str[:4],
                "month_of_birth": source[self.birth_datetime].str[4:6],
                "day_of_birth": source[self.birth_datetime].str[6:8],
                "birth_datetime": pd.to_datetime(source[self.birth_datetime], format = "%Y%m%d", errors='coerce'),
                "death_datetime": pd.to_datetime(source[self.death_datetime], format = "%Y%m%d", errors='coerce'),
                "race_concept_id": np.select(race_conditions, race_concept_id, default = self.no_matching_concept[0]),
                "ethnicity_concept_id": self.no_matching_concept[0],
                "location_id": source["LOCATION_ID"],
                "provider_id": None,
                "care_site_id": None, 
                "person_source_value": source[self.person_source_value],
                "환자명": source[self.person_name],
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
        self.source_data2 = self.cdm_config["data"]["source_data2"]
        self.output_filename = self.cdm_config["data"]["output_filename"]

        self.meddept = self.cdm_config["columns"]["meddept"]
        self.meddate = self.cdm_config["columns"]["meddate"]
        self.medtime = self.cdm_config["columns"]["medtime"]
        self.admdate = self.cdm_config["columns"]["admdate"]
        self.admtime = self.cdm_config["columns"]["admtime"]
        self.dschdate = self.cdm_config["columns"]["dschdate"]
        self.dschtime = self.cdm_config["columns"]["dschtime"]
        self.admitted_from_source_value = self.cdm_config["columns"]["admitted_from_source_value"]
        self.discharge_to_source_value = self.cdm_config["columns"]["discharge_to_source_value"]
        self.visit_source_value = self.cdm_config["columns"]["visit_source_value"]
        self.orddr = self.cdm_config["columns"]["orddr"]
        self.chadr = self.cdm_config["columns"]["chadr"]
        

    def transform(self):
        """
        소스 데이터를 읽어들여 CDM 형식으로 변환하고 결과를 CSV 파일로 저장하는 메소드입니다.
        """
        try : 
            source_data, source_data2 = self.process_source()
            transformed_data = self.transform_cdm(source_data, source_data2)

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
            source2 = self.read_csv(self.source_data2, path_type = self.source_flag , dtype = self.source_dtype)
            person_data = self.read_csv(self.person_data, path_type = self.cdm_flag , dtype = self.source_dtype)
            provider_data = self.read_csv(self.provider_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            care_site_data = self.read_csv(self.care_site_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            concept_etc = self.read_csv(self.concept_etc, path_type = self.source_flag, dtype = self.source_dtype, encoding=self.cdm_encoding)
            logging.debug(f"원천 데이터 row수: source: {len(source)}, source2: {len(source2)}")

            # 원천 데이터 범위 설정
            source["visit_start_datetime"] = source[self.meddate] + source[self.medtime]
            source[self.meddate] = pd.to_datetime(source[self.meddate])
            source[self.frstrgstdt] = pd.to_datetime(source[self.frstrgstdt])
            source["visit_source_key"] = source[self.person_source_value] + ';' + source[self.meddate].dt.strftime("%Y%m%d") + ';' + source[self.visit_no] + ';' + source[self.hospital]
            source = source[source[self.meddate] <= self.data_range]
            logging.debug(f"데이터1 범위 조건 적용 후 원천 데이터 row수: {len(source)}")

            # 불러온 원천 전처리
            source = pd.merge(source, person_data, left_on = self.person_source_value, right_on="person_source_value", how="inner")
            source = source.drop(columns = ["care_site_id", "provider_id"])
            logging.debug(f"person 테이블과 결합 후 원천 데이터1 row수: {len(source)}")

            source = pd.merge(source, care_site_data, left_on = [self.meddept, self.hospital], right_on=["care_site_source_value", "place_of_service_source_value"], how="left")
            logging.debug(f"care_site 테이블과 결합 후 원천 데이터1 row수: {len(source)}")
            
            source[self.care_site_fromdate] = pd.to_datetime(source[self.care_site_fromdate], errors="coerce")
            source[self.care_site_todate] = pd.to_datetime(source[self.care_site_todate], errors="coerce")
            source[self.care_site_fromdate] = source[self.care_site_fromdate].fillna("19000101")
            source[self.care_site_todate] = source[self.care_site_todate].fillna("20991231")
            source= source[(source[self.frstrgstdt] >= source[self.care_site_fromdate] ) & (source[self.frstrgstdt] <= source[self.care_site_todate])]
            logging.debug(f"care_site 사용 기간 조건 설정 후 원천 데이터1 row수: {len(source)}")

            source = pd.merge(source, provider_data, left_on = self.orddr, right_on="provider_source_value", how="left", suffixes=('', '_y'))
            # source.loc[source["care_site_id"].isna(), "care_site_id"] = 0
            logging.debug(f"provider 테이블과 결합 후 원천 데이터1 row수: {len(source)}")

            source["visit_type_concept_id"] = np.select([source[self.meddept] == "CTC"], [44818519], default = 44818518)
            concept_etc["concept_id"] = concept_etc["concept_id"].astype(int)
            source = pd.merge(source, concept_etc, left_on = "visit_type_concept_id", right_on="concept_id", how="left")

            # 원천 데이터2 범위 설정
            source2["visit_start_datetime"] = source2[self.admdate] + source2[self.admtime]
            source2[self.admdate] = pd.to_datetime(source2[self.admdate])
            source2[self.frstrgstdt] = pd.to_datetime(source2[self.frstrgstdt])
            source2["visit_source_key"] = source2[self.person_source_value] + ';' + source2[self.admdate].dt.strftime("%Y%m%d") + ';' + source2[self.visit_no] + ';' + source2[self.hospital]
            source2 = source2[source2[self.admdate] <= self.data_range]
            logging.debug(f"데이터2 범위 조건 적용 후 원천 데이터2 row수: {len(source2)}")

            # 불러온 원천2 전처리    
            source2 = pd.merge(source2, person_data, left_on=self.person_source_value, right_on="person_source_value", how="inner")
            source2 = source2.drop(columns = ["care_site_id", "provider_id"])
            logging.debug(f"person 테이블과 결합 후 원천 데이터2 row수: {len(source2)}")

            source2 = pd.merge(source2, care_site_data, left_on = [self.meddept, self.hospital], right_on=["care_site_source_value", "place_of_service_source_value"], how="left")
            logging.debug(f"care_site 테이블과 결합 후 원천 데이터2 row수: {len(source2)}")

            source2[self.care_site_fromdate] = pd.to_datetime(source2[self.care_site_fromdate], errors="coerce")
            source2[self.care_site_todate] = pd.to_datetime(source2[self.care_site_todate], errors="coerce")
            source2[self.care_site_fromdate] = source2[self.care_site_fromdate].fillna("19000101")
            source2[self.care_site_todate] = source2[self.care_site_todate].fillna("20991231")
            source2 = source2[(source2[self.frstrgstdt] >= source2[self.care_site_fromdate]) & (source2[self.frstrgstdt] <= source2[self.care_site_todate])]
            logging.debug(f"care_site 사용 기간 조건 설정 후 원천 데이터2 row수: {len(source2)}")

            source2 = pd.merge(source2, provider_data, left_on = self.chadr, right_on="provider_source_value", how="left", suffixes=('', '_y'))
            source2.loc[source2["care_site_id"].isna(), "care_site_id"] = 0
            logging.debug(f"provider 테이블과 결합 후 원천 데이터1 row수: {len(source2)}")

            source2["visit_type_concept_id"] = np.select([source2[self.meddept] == "CTC"], [44818519], default = 44818518)
            source2 = pd.merge(source2, concept_etc, left_on = "visit_type_concept_id", right_on="concept_id", how="left")
            logging.debug(f"concept_etc 테이블과 결합 후 원천 데이터2 row수: {len(source2)}")

            logging.debug(f"CDM 테이블과 결합 후 원천 데이터 row수: {len(source2)}")

            return source, source2

        except Exception as e :
            logging.error(f"{self.table} 테이블 소스 데이터 처리 중 오류: {e}", exc_info = True)

    def transform_cdm(self, source, source2):
        """
        주어진 소스 데이터를 CDM 형식에 맞게 변환하는 메소드.
        변환된 데이터는 새로운 DataFrame으로 구성됩니다.
        """
        try : 
            # cdm_o 생성
            cdm_o = pd.DataFrame({
                "person_id": source["person_id"],
                "환자명": source["환자명"],
                "visit_concept_id": np.select([source[self.visit_source_value] == "O"], [9202], default = self.no_matching_concept[0]),
                "visit_start_date": source[self.meddate],
                "visit_start_datetime": pd.to_datetime(source["visit_start_datetime"], format="%Y%m%d%H%M%S"),
                "visit_end_date": source[self.meddate],
                "visit_end_datetime": pd.to_datetime(source["visit_start_datetime"], format="%Y%m%d%H%M%S"),
                "visit_type_concept_id": np.select([source["visit_type_concept_id"].notna()], [source["visit_type_concept_id"]], default = self.no_matching_concept[0]),
                "visit_type_concept_id_name": np.select([source["concept_name"].notna()], [source["concept_name"]], default=self.no_matching_concept[1]),
                "provider_id": source["provider_id"],
                "care_site_id": source["care_site_id"],
                "visit_source_value": source[self.visit_source_value],
                "visit_source_concept_id": 9202,
                "admitted_from_concept_id": self.no_matching_concept[0],
                "admitted_from_source_value": None,
                "discharge_to_concept_id": self.no_matching_concept[0],
                "discharge_to_source_value": None,
                "visit_source_key": source["visit_source_key"],
                "진료과": source[self.meddept],
                "진료과명": source["care_site_name"]
                })

            # cdm_ie 생성
            visit_condition = [
                source2[self.visit_source_value] == "I",
                source2[self.visit_source_value] == "E",
            ]
            visit_concept_id = [9201, 9203]

            cdm_ie = pd.DataFrame({
                "person_id": source2["person_id"],
                "환자명": source2["환자명"],
                "visit_concept_id": np.select(visit_condition, visit_concept_id, default = 0),
                "visit_start_date": source2[self.admdate],
                "visit_start_datetime": pd.to_datetime(source2["visit_start_datetime"], format="%Y%m%d%H%M%S"),
                "visit_end_date": pd.to_datetime(source2[self.dschdate], format="%Y%m%d"),
                "visit_end_datetime": (source2[self.dschdate] + source2[self.dschtime]).apply(lambda x: datetime.strptime(x, '%Y%m%d%H%M%S')),
                "visit_type_concept_id": np.select([source2["visit_type_concept_id"].notna()], [source2["visit_type_concept_id"]], default = self.no_matching_concept[0]),
                "visit_type_concept_id_name": np.select([source2["concept_name"].notna()], [source2["concept_name"]], default=self.no_matching_concept[1]),
                "provider_id": source2["provider_id"],
                "care_site_id": source2["care_site_id"],
                "visit_source_value": source2[self.visit_source_value],
                "visit_source_concept_id": np.select(visit_condition, visit_concept_id, default = self.no_matching_concept[0]),
                "admitted_from_concept_id": self.no_matching_concept[0],
                "admitted_from_source_value": source2[self.admitted_from_source_value],
                "discharge_to_concept_id": self.no_matching_concept[0],
                "discharge_to_source_value": source2[self.discharge_to_source_value],
                "visit_source_key": source2["visit_source_key"],
                "진료과": source2[self.meddept],
                "진료과명": source2["care_site_name"]
                })
            cdm = pd.concat([cdm_o, cdm_ie], axis = 0)
            
            cdm.reset_index(drop=True, inplace = True)
            cdm["visit_occurrence_id"] = cdm.index + 1
            cdm.sort_values(by=["person_id", "visit_start_datetime"], inplace = True)
            cdm["preceding_visit_occurrence_id"] = cdm.groupby("person_id")["visit_occurrence_id"].shift(1)
            cdm["preceding_visit_occurrence_id"] = cdm["preceding_visit_occurrence_id"].apply(lambda x : x if pd.isna(x) else str(int(x)))

            cdm = cdm[self.columns]
            
            logging.debug(f"CDM 데이터 row수: {len(cdm)}")
            logging.debug(f"요약:\n{cdm.describe(include = 'all').T.to_string()}")
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
                        , "visit_detail_parent_id", "visit_occurrence_id", "진료과", "진료과명", "병동번호", "병동명"]

        # 컬럼 변수 재정의    
        self.source_data = self.cdm_config["data"]["source_data"]
        self.output_filename = self.cdm_config["data"]["output_filename"]
        self.meddept = self.cdm_config["columns"]["meddept"]
        self.provider = self.cdm_config["columns"]["provider"]
        self.visit_detail_start_datetime = self.cdm_config["columns"]["visit_detail_start_datetime"]
        self.visit_detail_end_datetime = self.cdm_config["columns"]["visit_detail_end_datetime"]
        self.visit_detail_source_value = self.cdm_config["columns"]["visit_detail_source_value"]
        self.admitted_from_source_value = self.cdm_config["columns"]["admitted_from_source_value"]
        self.discharge_to_source_value = self.cdm_config["columns"]["discharge_to_source_value"]
        self.wardno = self.cdm_config["columns"]["wardno"]
        self.admdate = self.cdm_config["columns"]["admdate"]

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
            provider_data = self.read_csv(self.provider_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            care_site_data = self.read_csv(self.care_site_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_data = self.read_csv(self.visit_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            concept_etc = self.read_csv(self.concept_etc, path_type = self.source_flag, dtype = self.source_dtype, encoding=self.cdm_encoding)
            logging.debug(f"원천 데이터 row수: {len(source)}")

            # visit_source_key 생성
            source[self.admdate] = pd.to_datetime(source[self.admdate])
            source[self.frstrgstdt] = pd.to_datetime(source[self.frstrgstdt])
            source["visit_source_key"] = source[self.person_source_value] +  ';' + source[self.admdate].dt.strftime("%Y%m%d") + ';' + source[self.visit_no] + ';' + source[self.hospital]
            
            # # 201903081045같은 데이터가 2019-03-08 10:04:05로 바뀌는 문제 발견 
            # def convert_datetime_format(x):
            #     if pd.isna(x):  # x가 NaN인지 확인
            #         return x
            #     else:
            #         return x[:4] + "-" + x[4:6] + "-" + x[6:8] + " " + x[8:10] + ":" + x[10:]
            # source[self.visit_detail_start_datetime] = source[self.visit_detail_start_datetime].apply(convert_datetime_format)
            # source[self.visit_detail_end_datetime] = source[self.visit_detail_end_datetime].apply(convert_datetime_format)

            # person table과 병합
            source = pd.merge(source, person_data, left_on=self.person_source_value, right_on="person_source_value", how="inner")
            source = source.drop(columns = ["care_site_id", "provider_id"])
            logging.debug(f"person 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            # care_site table과 병합
            source = pd.merge(source, care_site_data, left_on = [self.meddept, self.hospital], right_on=["care_site_source_value", "place_of_service_source_value"], how="left")
            logging.debug(f"care_site 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            source[self.care_site_fromdate] = pd.to_datetime(source[self.care_site_fromdate], errors="coerce")
            source[self.care_site_todate] = pd.to_datetime(source[self.care_site_todate], errors="coerce")
            source[self.care_site_fromdate] = source[self.care_site_fromdate].fillna("19000101")
            source[self.care_site_todate] = source[self.care_site_todate].fillna("20991231")
            source= source[( source[self.frstrgstdt] >= source[self.care_site_fromdate]) & (source[self.frstrgstdt] <= source[self.care_site_todate])]
            logging.debug(f"care_site 사용 기간 조건 설정 후 원천 데이터 row수: {len(source)}")

            # # 병동명을 위한 care_site table과 병합
            # source = pd.merge(source, care_site_data, left_on=self.wardno, right_on="care_site_source_value", how="left", suffixes=('', '_wardno'))
            # logging.debug(f"병동명을 위한 care_site 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            # provider table과 병합
            source = pd.merge(source, provider_data, left_on=self.provider, right_on="provider_source_value", how="left", suffixes=('', '_y'))
            logging.debug(f"provider 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            # visit_occurrence테이블에서 I에 해당하는 데이터만 추출
            visit_data = visit_data[visit_data["visit_source_value"].isin(["I"])]
            # visit_occurrence table과 병합
            source = pd.merge(source, visit_data, left_on=["visit_source_key"], right_on=["visit_source_key"], how="left", suffixes=('', '_y'))
            logging.debug(f"visit_occurrence 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            source["visit_detail_type_concept_id"] = 44818518
            concept_etc["concept_id"] = concept_etc["concept_id"].astype(int)
            source = pd.merge(source, concept_etc, left_on="visit_detail_type_concept_id", right_on='concept_id')
            logging.debug(f"concept_etc 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            # 컬럼을 datetime형태로 변경
            source[self.visit_detail_start_datetime] = pd.to_datetime(source[self.visit_detail_start_datetime])
            source[self.visit_detail_end_datetime] = pd.to_datetime(source[self.visit_detail_end_datetime], errors="coerce")
            source["visit_start_datetime"] = pd.to_datetime(source["visit_start_datetime"])
            source["visit_end_datetime"] = pd.to_datetime(source["visit_end_datetime"])
            
            # 에러 발생하는 부분을 최대값으로 처리
            # 최대 Timestamp 값
            max_timestamp = pd.Timestamp.max

            # NaT 값을 최대 Timestamp 값으로 대체
            source["visit_end_datetime"] = source["visit_end_datetime"].fillna(max_timestamp)

            source = source[(source[self.visit_detail_start_datetime] >= source["visit_start_datetime"]) & (source[self.visit_detail_start_datetime] <= source["visit_end_datetime"])]
            # source.loc[source["care_site_id"].isna(), "care_site_id"] = 0
            logging.debug(f"visit_detail 날짜조건 적용 후 데이터 row수: {len(source)}")

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
                "visit_detail_source_value": source[self.visit_detail_source_value],
                "visit_detail_source_concept_id": self.no_matching_concept[0],
                "admitted_from_concept_id": self.no_matching_concept[0],
                "admitted_from_source_value": self.admitted_from_source_value,
                "discharge_to_source_value": self.discharge_to_source_value,
                "discharge_to_concept_id": self.no_matching_concept[0],
                "visit_detail_parent_id": None,
                "visit_occurrence_id": source["visit_occurrence_id"],
                "진료과": source[self.meddept],
                "진료과명": source["care_site_name"],
                "병동번호": None, # source[self.wardno],
                "병동명": None, # source["care_site_name_wardno"]
                })

            cdm = cdm[cdm["visit_detail_start_datetime"] <= self.data_range]
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
        self.engname = self.cdm_config["columns"]["engname"]
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
            concept_kcd = self.read_csv(self.concept_kcd, path_type = self.source_flag, dtype = self.source_dtype, encoding=self.cdm_encoding)
            logging.debug(f'원천 데이터 row수: order: {len(source)}, concept: {len(concept_kcd)}')

            # 2. diag 테이블 처리
            source['changed_diagcode'] = source[self.diagcode].str.split('.').str[0]
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
            local_kcd = local_kcd[[self.diagcode, 'changed_diagcode', self.fromdate, self.todate, self.engname, self.korname, self.hospital,
                                self.concept_id, self.concept_name, self.domain_id, self.vocabulary_id, self.concept_class_id, 
                                self.standard_concept, self.concept_code, self.valid_start_date, self.valid_end_date, self.invalid_reason]]

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
        self.condition_status_source_value = self.cdm_config["columns"]["condition_status_source_value"]
        self.orddd = self.cdm_config["columns"]["orddd"]
        self.diagfg = self.cdm_config["columns"]["diagfg"]
        self.fromdate = self.cdm_config["columns"]["fromdate"]
        self.todate = self.cdm_config["columns"]["todate"]
        self.engname = self.cdm_config["columns"]["engname"]
        self.diagcode = self.cdm_config["columns"]["diagcode"]
        
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
            local_kcd = self.read_csv(self.local_kcd_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            logging.debug(f"원천 데이터 row수: {len(source)}")

            # 원천에서 조건걸기
            source[self.condition_start_datetime] = pd.to_datetime(source[self.condition_start_datetime], format="%Y%m%d")
            source[self.orddd] = pd.to_datetime(source[self.orddd])
            source[self.frstrgstdt] = pd.to_datetime(source[self.frstrgstdt])
            
            # visit_source_key 생성
            source["visit_source_key"] = source[self.person_source_value] + ';' + source[self.orddd].dt.strftime("%Y%m%d") + ';' + source[self.visit_no] + ';' + source[self.hospital]
            source = source[source[self.condition_start_datetime] <= self.data_range]
            source = source[source[self.condition_start_datetime].notna()]
            logging.debug(f"조건 적용후 원천 데이터 row수: {len(source)}")

            # person table과 병합
            source = pd.merge(source, person_data, left_on=self.person_source_value, right_on="person_source_value", how="inner")
            source = source.drop(columns = ["care_site_id", "provider_id"])
            logging.debug(f"person 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            # local_kcd와 병합
            local_kcd[self.fromdate] = pd.to_datetime(local_kcd[self.fromdate], format="%Y%m%d", errors = "coerce")
            # local_kcd[self.fromdate].fillna(pd.Timestamp('1900-01-01'), inplace = True)
            local_kcd[self.todate] = pd.to_datetime(local_kcd[self.todate], format="%Y%m%d", errors = "coerce")
            # local_kcd[self.todate].fillna(pd.Timestamp('2099-12-31'), inplace = True)
            logging.debug(f"local_kcd 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            source = pd.merge(source, local_kcd, left_on = [self.condition_source_value, self.hospital], right_on = [self.diagcode, self.hospital], how = "left", suffixes=('', '_kcd'))
            source[self.fromdate] = source[self.fromdate].fillna(pd.Timestamp('1900-01-01'))
            source[self.todate] = source[self.todate].fillna(pd.Timestamp('2099-12-31'))
            source = source[(source[self.condition_start_datetime].dt.date >= source[self.fromdate]) & (source[self.condition_start_datetime].dt.date <= source[self.todate])]
            logging.debug(f"local_kcd 테이블의 날짜 조건 적용 후 원천 데이터 row수: {len(source)}")

            # care_site table과 병합
            source = pd.merge(source, care_site_data, left_on=[self.meddept, self.hospital], right_on=["care_site_source_value", "place_of_service_source_value"], how="left")
            logging.debug(f"care_site 테이블과 결합 후 원천 데이터 row수: {len(source)}")
            
            source[self.care_site_fromdate] = pd.to_datetime(source[self.care_site_fromdate], errors="coerce")
            source[self.care_site_todate] = pd.to_datetime(source[self.care_site_todate], errors="coerce")
            source[self.care_site_fromdate] = source[self.care_site_fromdate].fillna("19000101")
            source[self.care_site_todate] = source[self.care_site_todate].fillna("20991231")
            source= source[(source[self.frstrgstdt] >= source[self.care_site_fromdate]) & (source[self.frstrgstdt] <= source[self.care_site_todate])]
            logging.debug(f"care_site 사용 기간 조건 설정 후 원천 데이터 row수: {len(source)}")

            # provider table과 병합
            source = pd.merge(source, provider_data, left_on=self.provider, right_on="provider_source_value", how="left", suffixes=('', '_y'))
            logging.debug(f"provider 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            # visit_start_datetime 형태 변경
            visit_data["visit_start_date"] = pd.to_datetime(visit_data["visit_start_date"], errors = "coerce")
            visit_data["visit_end_datetime"] = pd.to_datetime(visit_data["visit_end_datetime"], errors = "coerce")
            # visit_occurrence table과 병합
            source = pd.merge(source, visit_data, left_on=["visit_source_key"], right_on=["visit_source_key"], how="left", suffixes=('', '_y'))
            logging.debug(f"visit_occurrence 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            visit_detail = visit_detail[["visit_detail_id", "visit_detail_start_datetime", "visit_detail_end_datetime", "visit_occurrence_id"]]
            visit_detail["visit_detail_start_datetime"] = pd.to_datetime(visit_detail["visit_detail_start_datetime"])
            visit_detail["visit_detail_end_datetime"] = pd.to_datetime(visit_detail["visit_detail_end_datetime"])
            source = pd.merge(source, visit_detail, left_on=["visit_occurrence_id"], right_on=["visit_occurrence_id"], how="left", suffixes=('', '_y'))
            logging.debug(f"visit_detail 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            source["visit_detail_id"] = source.apply(lambda row: row['visit_detail_id'] if pd.notna(row['visit_detail_start_datetime']) and row['visit_detail_start_datetime'] <= row[self.condition_start_datetime] <= row['visit_detail_end_datetime'] else pd.NA, axis=1)
            source = source.drop(columns = ["visit_detail_start_datetime", "visit_detail_end_datetime"])
            source = source.drop_duplicates(subset=[self.person_source_value, self.condition_start_datetime, self.condition_source_value, self.hospital, self.visit_no, "DIAGHISTNO", self.meddept])
            logging.debug(f"visit_detail 테이블과 결합 후 조건 적용 후 원천 데이터 row수: {len(source)}")

            # care_site_id가 없는 경우 0으로 값 입력
            # source.loc[source["care_site_id"].isna(), "care_site_id"] = 0

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
            cdm = pd.DataFrame({
                "condition_occurrence_id": source.index + 1,
                "person_id": source["person_id"],
                "condition_concept_id": np.select([source["concept_id"].notna()], [source["concept_id"]], self.no_matching_concept[0]),
                "condition_start_date": source[self.condition_start_datetime].dt.date,
                "condition_start_datetime": source[self.condition_start_datetime],
                "condition_end_date": source["visit_end_datetime"].dt.date,
                "condition_end_datetime": source["visit_end_datetime"],
                "condition_type_concept_id": None,
                "condition_type_concept_id_name": None,
                "condition_status_concept_id": self.no_matching_concept[0],
                "stop_reason": None,
                "provider_id": source["provider_id"],
                "주치의명": source["provider_name"],
                "visit_occurrence_id": source["visit_occurrence_id"],
                "visit_detail_id": source["visit_detail_id"],
                "condition_source_value": source[self.condition_source_value],
                "진단명": source[self.engname],
                "condition_source_concept_id": np.select([source["concept_id"].notna()], [source["concept_id"]], self.no_matching_concept[0]),
                "condition_status_source_value": self.condition_status_source_value,
                "visit_source_key": source["visit_source_key"],
                "환자구분": source["visit_source_value"],
                "진료과": source[self.meddept],
                "진료과명": source["care_site_name"],
                "상병구분": source[self.diagfg]
                })

            # datetime format 형식 맞춰주기, ns로 표기하는 값이 들어갈 수 있어서 처리함
            cdm["condition_end_date"] = pd.to_datetime(cdm["condition_end_date"],errors = "coerce").dt.date
            cdm["condition_end_datetime"] = pd.to_datetime(cdm["condition_end_datetime"], errors = "coerce")

            logging.debug(f"CDM 데이터 row수: {len(cdm)}")
            logging.debug(f"요약:\n{cdm.describe(include = 'all').T.to_string()}")
            logging.debug(f"컬럼별 null 개수:\n{cdm.isnull().sum().to_string()}")

            return cdm   

        except Exception as e :
            logging.error(f"{self.table} 테이블 CDM 데이터 변환 중 오류: {e}", exc_info = True)

class DrugEDITransformer(DataTransformer):
    def __init__(self, config_path):
        super().__init__(config_path)
        self.table = "drug_edi"
        self.cdm_config = self.config[self.table]

        # 컬럼 변수 재정의      
        self.order_data = self.cdm_config["data"]["order_data"]
        self.concept_data = self.cdm_config["data"]["concept_data"]
        self.atc_data = self.cdm_config["data"]["edi_atc"]
        self.output_filename = self.cdm_config["data"]["output_filename"]

        self.ordercode = self.cdm_config["columns"]["ordercode"]
        self.edicode = self.cdm_config["columns"]["edicode"]
        self.fromdate = self.cdm_config["columns"]["fromdate"]
        self.todate = self.cdm_config["columns"]["todate"]
        self.standard_code = self.cdm_config["columns"]["standard_code"]
        self.atccode = self.cdm_config["columns"]["atccode"]
        self.atcname = self.cdm_config["columns"]["atcname"]
        self.edi_fromdate = self.cdm_config["columns"]["edi_fromdate"]
        self.edi_todate = self.cdm_config["columns"]["edi_todate"]
        
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
            source = self.read_csv(self.order_data, path_type = self.source_flag, dtype = self.source_dtype)
            concept_data = self.read_csv(self.concept_data, path_type = self.source_flag, dtype = self.source_dtype, encoding=self.cdm_encoding)
            atc_data = self.read_csv(self.atc_data, path_type = self.source_flag, dtype = self.source_dtype, encoding=self.cdm_encoding)
            logging.debug(f'원천 데이터 row수: order: {len(source)}')

            # concept_id 매핑
            source[self.fromdate].fillna("19000101")
            source[self.todate].fillna("20991231")

            concept_data = concept_data.sort_values(by = ["vocabulary_id"], ascending=[False])
            concept_data['Sequence'] = concept_data.groupby(["concept_code"]).cumcount() + 1
            concept_data = concept_data[concept_data["Sequence"] == 1]

            source = pd.merge(source, concept_data, left_on=self.edicode, right_on="concept_code", how="left")
            logging.debug(f'concept merge후 데이터 row수: {len(source)}')

            # ATC코드 매핑
            atc_data[self.standard_code] = atc_data[self.standard_code].str[3:-1]

            source = pd.merge(source, atc_data, left_on=self.edicode, right_on=self.standard_code, how = "left")
            logging.debug(f'ATC 매핑 후 데이터 row수: {len(source)}')
            # local_edi = source[[self.ordercode, self.fromdate, self.todate, self.edicode, "concept_id", "concept_name",
            #                     "domain_id", "vocabulary_id", "concept_class_id", "standard_concept",
            #                     "concept_code", "valid_start_date", "valid_end_date", "invalid_reason"]]
            # logging.debug(f'중복되는 concept_id 제거 후 데이터 row수: {len(source)}')
        
            logging.debug(f'local_edi row수: {len(source)}')
            logging.debug(f"요약:\n{source.describe(include = 'all').T.to_string()}")
            logging.debug(f"컬럼별 null 개수:\n{source.isnull().sum().to_string()}")

            return source

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
        self.drugcd = self.cdm_config["columns"]["drugcd"]
        self.fromdate = self.cdm_config["columns"]["fromdate"]
        self.todate = self.cdm_config["columns"]["todate"]
        self.edicode = self.cdm_config["columns"]["edicode"]
        self.meddept = self.cdm_config["columns"]["meddept"]
        self.provider = self.cdm_config["columns"]["provider"]
        self.drug_exposure_start_datetime = self.cdm_config["columns"]["drug_exposure_start_datetime"]
        self.drug_source_value = self.cdm_config["columns"]["drug_source_value"]
        self.drug_source_value_name = self.cdm_config["columns"]["drug_source_value_name"]
        self.days_supply = self.cdm_config["columns"]["days_supply"]
        self.qty = self.cdm_config["columns"]["qty"]
        self.cnt = self.cdm_config["columns"]["cnt"]
        self.route_source_value = self.cdm_config["columns"]["route_source_value"]
        self.dose_unit_source_value = self.cdm_config["columns"]["dose_unit_source_value"]
        self.orddd = self.cdm_config["columns"]["orddd"]
        
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
            drug_edi = self.read_csv(self.drug_edi_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            person_data = self.read_csv(self.person_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            provider_data = self.read_csv(self.provider_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            care_site_data = self.read_csv(self.care_site_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_data = self.read_csv(self.visit_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_detail = self.read_csv(self.visit_detail, path_type = self.cdm_flag, dtype = self.source_dtype)
            concept_etc = self.read_csv(self.concept_etc, path_type = self.source_flag, dtype = self.source_dtype, encoding=self.cdm_encoding)

            logging.info(f"원천 데이터 row수:, {len(source)}")

            # 원천에서 조건걸기
            source = source[[self.person_source_value, self.drug_source_value, self.drug_exposure_start_datetime,
                             self.meddept, self.days_supply, self.qty, self.cnt, self.provider,
                             self.dose_unit_source_value, self.hospital, self.route_source_value, self.orddd, self.visit_no, "PRCPNO", self.frstrgstdt]]
            source["진료일시"] = source[self.orddd]
            source[self.drug_exposure_start_datetime] = pd.to_datetime(source[self.drug_exposure_start_datetime])
            source[self.orddd] = pd.to_datetime(source[self.orddd])
            source[self.frstrgstdt] = pd.to_datetime(source[self.frstrgstdt])
            # visit_source_key 생성
            source["visit_source_key"] = source[self.person_source_value] + ';' + source[self.orddd].dt.strftime("%Y%m%d") + ';' + source[self.visit_no] + ';' + source[self.hospital]
            source = source[(source[self.drug_exposure_start_datetime] <= self.data_range)]
            logging.info(f"조건 적용후 원천 데이터 row수:, {len(source)}")
            
            person_data = person_data[["person_id", "person_source_value", "환자명"]]
            # person table과 병합
            source = pd.merge(source, person_data, left_on=self.person_source_value, right_on="person_source_value", how="inner")
            logging.info(f"person 테이블과 결합 후 데이터 row수: {len(source)}")

            drug_edi = drug_edi[[self.drugcd, self.fromdate, self.todate, self.edicode, "concept_id", "ATC코드", "ATC 코드명", self.drug_source_value_name]]
            drug_edi[self.fromdate] = pd.to_datetime(drug_edi[self.fromdate] , format="%Y%m%d", errors="coerce")
            drug_edi[self.todate] = pd.to_datetime(drug_edi[self.todate] , format="%Y%m%d", errors="coerce")

            # LOCAL코드와 EDI코드 매핑 테이블과 병합
            source = pd.merge(source, drug_edi, left_on=self.drug_source_value, right_on=self.drugcd, how="left")
            logging.info(f"local_edi와 병합 후 데이터 row수:, {len(source)}")
            source[self.fromdate] = source[self.fromdate].fillna(pd.Timestamp('1900-01-01'))
            source[self.todate] = source[self.todate].fillna(pd.Timestamp('2099-12-31'))
            source = source[(source[self.drug_exposure_start_datetime] >= source[self.fromdate]) & (source[self.drug_exposure_start_datetime] <= source[self.todate])]
            logging.info(f"local_edi날짜 조건 적용 후 데이터 row수: {len(source)}")

            
            care_site_data = care_site_data[["care_site_id", "care_site_source_value", "place_of_service_source_value", "care_site_name", self.care_site_fromdate, self.care_site_todate]]
            provider_data = provider_data[["provider_id", "provider_source_value", "provider_name"]]
            visit_data = visit_data[["visit_occurrence_id", "visit_start_date", "care_site_id", "visit_source_value", "person_id", "visit_source_key"]]

            # care_site table과 병합
            source = pd.merge(source, care_site_data, left_on=[self.meddept, self.hospital], right_on=["care_site_source_value", "place_of_service_source_value"], how="left")
            logging.info(f"care_site 테이블과 결합 후 데이터 row수: {len(source)}")

            source[self.care_site_fromdate] = pd.to_datetime(source[self.care_site_fromdate], errors = "coerce")
            source[self.care_site_todate] = pd.to_datetime(source[self.care_site_todate], errors = "coerce")
            source[self.care_site_fromdate] = source[self.care_site_fromdate].fillna("19000101")
            source[self.care_site_todate] = source[self.care_site_todate].fillna("20991231")
            source= source[(source[self.frstrgstdt] >= source[self.care_site_fromdate]) & (source[self.frstrgstdt] <= source[self.care_site_todate])]
            logging.debug(f"care_site 사용 기간 조건 설정 후 원천 데이터 row수: {len(source)}")

            # provider table과 병합
            source = pd.merge(source, provider_data, left_on=self.provider, right_on="provider_source_value", how="left", suffixes=('', '_y'))
            logging.info(f"provider 테이블과 결합 후 데이터 row수: {len(source)}")

            # visit_start_datetime 형태 변경
            visit_data["visit_start_date"] = pd.to_datetime(visit_data["visit_start_date"])
            # visit_occurrence table과 병합
            source = pd.merge(source, visit_data, left_on=["visit_source_key"], right_on=["visit_source_key"], how="left", suffixes=('', '_y'))
            logging.info(f"visit_occurrence 테이블과 결합 후 데이터 row수: {len(source)}")

            visit_detail = visit_detail[["visit_detail_id", "visit_detail_start_datetime", "visit_detail_end_datetime", "visit_occurrence_id"]]
            visit_detail["visit_detail_start_datetime"] = pd.to_datetime(visit_detail["visit_detail_start_datetime"])
            visit_detail["visit_detail_end_datetime"] = pd.to_datetime(visit_detail["visit_detail_end_datetime"])
            source = pd.merge(source, visit_detail, left_on=["visit_occurrence_id"], right_on=["visit_occurrence_id"], how="left", suffixes=('', '_y'))
            logging.debug(f"visit_detail 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            source["visit_detail_id"] = source.apply(lambda row: row['visit_detail_id'] if pd.notna(row['visit_detail_start_datetime']) and row['visit_detail_start_datetime'] <= row[self.drug_exposure_start_datetime] <= row['visit_detail_end_datetime'] else pd.NA, axis=1)
            source = source.drop(columns = ["visit_detail_start_datetime", "visit_detail_end_datetime"])
            source = source.drop_duplicates(subset=[self.person_source_value, self.drug_exposure_start_datetime, "PRCPNO", self.hospital, self.drug_source_value, self.orddd, self.meddept])
            logging.debug(f"visit_detail 테이블과 결합 후 조건 적용 후 원천 데이터 row수: {len(source)}")

            # care_site_id가 없는 경우 0으로 값 입력
            # source.loc[source["care_site_id"].isna(), "care_site_id"] = 0
            # source.loc[source["concept_id"].isna(), "concept_id"] = 0

            # drug_type_concept_id_name가져오기
            source["drug_type_concept_id"] = 38000177
            concept_etc["concept_id"] = concept_etc["concept_id"].astype(int)
            source = pd.merge(source, concept_etc, left_on = "drug_type_concept_id", right_on="concept_id", how="left", suffixes=('', '_drug_type'))

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
            "drug_type_concept_id": np.select([source["drug_type_concept_id"].notna()], [source["drug_type_concept_id"]], default = self.no_matching_concept[0]),
            "drug_type_concept_id_name": np.select([source["concept_name"].notna()], [source["concept_name"]], default = self.no_matching_concept[1]),
            "stop_reason": None,
            "refills": None,
            "quantity": source[self.days_supply].astype(int) * source[self.qty].astype(float) * source[self.cnt].astype(float),
            "days_supply": source[self.days_supply].astype(int),
            "sig": None,
            "route_concept_id": self.no_matching_concept[0],
            "lot_number": None,
            "provider_id": source["provider_id"],
            "visit_occurrence_id": source["visit_occurrence_id"],
            "visit_detail_id": source["visit_detail_id"],
            "drug_source_value": source[self.drug_source_value],
            "drug_source_value_name": source[self.drug_source_value_name],
            "drug_source_concept_id": np.select([source["concept_id"].notna()], [source["concept_id"]], default=self.no_matching_concept[0]),
            "EDI코드": source[self.edicode],
            "route_source_value": source[self.route_source_value],
            "dose_unit_source_value": source[self.dose_unit_source_value],
            "vocabulary_id": "EDI",
            "visit_source_key": source["visit_source_key"],
            "환자구분": source["visit_source_value"],
            "진료과": source[self.meddept],
            "진료과명": source["care_site_name"],
            "진료일시": source["진료일시"],
            "나이": None,
            "투여량": source[self.qty],
            "함량단위": source[self.dose_unit_source_value],
            "횟수": source[self.cnt],
            "일수": source[self.days_supply],
            "용법코드": None,
            "처방순번": source[self.visit_no],
            "ATC코드": source["ATC코드"],
            "ATC 코드명": source["ATC 코드명"],
            "dcyn": None
            })

            logging.info(f"CDM테이블 row수: {len(cdm)}")
            logging.debug(f"요약:\n{cdm.describe(include = 'all').T.to_string()}")
            logging.debug(f"컬럼별 null 개수:\n{cdm.isnull().sum().to_string()}")

            return cdm   

        except Exception as e :
            logging.error(f"{self.table} 테이블 CDM 데이터 변환 중 오류: {e}", exc_info = True)


class MeasurementEDITransformer(DataTransformer):
    def __init__(self, config_path):
        super().__init__(config_path)
        self.table = "measurement_edi"
        self.cdm_config = self.config[self.table]

        # 컬럼 변수 재정의      
        self.order_data = self.cdm_config["data"]["order_data"]
        self.edi_data = self.cdm_config["data"]["edi_data"]
        self.concept_data = self.cdm_config["data"]["concept_data"]
        self.output_filename = self.cdm_config["data"]["output_filename"]

        self.ordercode = self.cdm_config["columns"]["ordercode"]
        self.sugacode = self.cdm_config["columns"]["sugacode"]
        self.ordnm = self.cdm_config["columns"]["ordnm"]
        self.tclsnm = self.cdm_config["columns"]["tclsnm"]
        self.fromdd = self.cdm_config["columns"]["fromdd"]
        self.todd = self.cdm_config["columns"]["todd"]
        self.order_fromdate = self.cdm_config["columns"]["order_fromdate"]
        self.order_todate = self.cdm_config["columns"]["order_todate"]
        
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
            edi_data = self.read_csv(self.edi_data, path_type = self.source_flag, dtype = self.source_dtype)
            concept_data = self.read_csv(self.concept_data, path_type = self.source_flag, dtype = self.source_dtype, encoding=self.cdm_encoding)
            logging.debug(f'원천 데이터 row수: order: {len(order_data)}, edi: {len(edi_data)}')
            
            # 처방코드 마스터와 수가코드 매핑
            source = pd.merge(order_data, edi_data, left_on=[self.ordercode, self.hospital], right_on=[self.sugacode, self.hospital], how="left")
            logging.debug(f'처방코드, 수가코드와 결합 후 데이터 row수: {len(source)}')
            
            source = source[(source[self.order_fromdate] >= source[self.fromdd]) &  (source[self.order_fromdate] <= source[self.todd])]
            logging.debug(f'조건 적용 후 데이터 row수: {len(source)}')

            concept_data = concept_data.sort_values(by = ["vocabulary_id"], ascending=[False])
            concept_data['Sequence'] = concept_data.groupby(["concept_code"]).cumcount() + 1
            concept_data = concept_data[concept_data["Sequence"] == 1]

            # concept_id 매핑
            source = pd.merge(source, concept_data, left_on=self.edicode, right_on="concept_code", how="left")
            logging.debug(f'concept merge후 데이터 row수: {len(source)}')

            source[self.fromdate] = source[self.fromdd].where(source[self.fromdd].notna(), source[self.order_fromdate])
            source[self.todate] = source[self.todd].where(source[self.todd].notna(), source[self.order_todate])


            # drug의 경우 KCD, EDI 순으로 매핑
            logging.debug(f'중복되는 concept_id 제거 후 데이터 row수: {len(source)}')
        
            logging.debug(f'local_edi row수: {len(source)}')
            logging.debug(f"요약:\n{source.describe(include = 'all').T.to_string()}")
            logging.debug(f"컬럼별 null 개수:\n{source.isnull().sum().to_string()}")

            return source

        except Exception as e :
            logging.error(f"{self.table} 테이블 소스 데이터 처리 중 오류:\n {e}", exc_info = True)


class MeasurementDiagTransformer(DataTransformer):
    def __init__(self, config_path):
        super().__init__(config_path)
        self.table = "measurement_diag"
        self.cdm_config = self.config[self.table]

        # 컬럼 변수 재정의   
        self.source_data1 = self.cdm_config["data"]["source_data1"]
        self.source_data2 = self.cdm_config["data"]["source_data2"]
        self.source_data3 = self.cdm_config["data"]["source_data3"]
        self.source_data4 = self.cdm_config["data"]["source_data4"]
        self.output_filename = self.cdm_config["data"]["output_filename"]
        self.meddept = self.cdm_config["columns"]["meddept"]
        self.provider = self.cdm_config["columns"]["provider"]
        self.orddate = self.cdm_config["columns"]["orddate"]
        self.measurement_date = self.cdm_config["columns"]["measurement_date"]
        self.measurement_source_value = self.cdm_config["columns"]["measurement_source_value"]
        self.value_source_value = self.cdm_config["columns"]["value_source_value"]
        self.range_low = self.cdm_config["columns"]["range_low"]
        self.range_high = self.cdm_config["columns"]["range_high"]
        self.unit_source_value = self.cdm_config["columns"]["unit_source_value"]
        self.ordcode = self.cdm_config["columns"]["ordcode"]
        self.orddd = self.cdm_config["columns"]["orddd"]
        self.spccd = self.cdm_config["columns"]["spccd"]
                
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
            source1 = self.read_csv(self.source_data1, path_type = self.source_flag, dtype = self.source_dtype)
            source2 = self.read_csv(self.source_data2, path_type = self.source_flag, dtype = self.source_dtype)
            source3 = self.read_csv(self.source_data3, path_type = self.source_flag, dtype = self.source_dtype)
            source4 = self.read_csv(self.source_data4, path_type = self.source_flag, dtype = self.source_dtype)
            local_edi = self.read_csv(self.measurement_edi_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            person_data = self.read_csv(self.person_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            provider_data = self.read_csv(self.provider_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            care_site_data = self.read_csv(self.care_site_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_data = self.read_csv(self.visit_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_detail = self.read_csv(self.visit_detail, path_type = self.cdm_flag, dtype = self.source_dtype)
            unit_data = self.read_csv(self.concept_unit, path_type = self.source_flag , dtype = self.source_dtype, encoding=self.cdm_encoding)
            concept_etc = self.read_csv(self.concept_etc, path_type = self.source_flag, dtype = self.source_dtype, encoding=self.cdm_encoding)
            unit_concept_synonym = self.read_csv(self.unit_concept_synonym, path_type = self.source_flag, dtype = self.source_dtype, encoding=self.cdm_encoding)
            logging.debug(f'원천 데이터 row수: {len(source1)}, {len(source2)}, {len(source3)}, {len(source4)}')

            # 원천에서 조건걸기
            source1 = source1[[self.hospital, self.orddate, self.person_source_value, "PRCPHISTNO", "ORDDD", "CRETNO", "PRCPCLSCD", "LASTUPDTDT", "ORDDRID", "PRCPNM", "PRCPCD", "PRCPHISTCD", "PRCPNO", "ORDDEPTCD"]]
            source1[self.orddate] = pd.to_datetime(source1[self.orddate])
            source1["ORDDD"] = pd.to_datetime(source1["ORDDD"])
            source1 = source1[(source1[self.orddate] <= self.data_range)]
            source1 = source1[(source1["PRCPHISTCD"] == "O") & (source1[self.hospital] == self.hospital_code) ]
            
            source2 = source2[[self.hospital, self.orddate, "PRCPNO", "PRCPHISTNO", "EXECPRCPUNIQNO", "ORDDD", self.unit_source_value, "EXECDD", "EXECTM"]]
            source2[self.orddate] = pd.to_datetime(source2[self.orddate])
            source2 = source2[(source2[self.orddate] <= self.data_range)]

            source3 = source3[[self.hospital, self.orddate, "EXECPRCPUNIQNO", "BCNO", "TCLSCD", "SPCCD", "ORDDD"]]
            source3[self.orddate] = pd.to_datetime(source3[self.orddate])
            source3 = source3[(source3[self.orddate] <= self.data_range)]

            source4 = source4[[self.hospital, "BCNO", "TCLSCD", self.spccd, "RSLTFLAG", self.measurement_source_value, self.measurement_date, self.range_low, self.range_high, self.value_source_value, "RSLTSTAT", "SPCACPTDT", self.frstrgstdt]]
            source4 = source4[(source4["RSLTFLAG"] == "O") & (source4["RSLTSTAT"].isin(["4", "5"]))]

            logging.debug(f'조건적용 후 원천 데이터 row수: {len(source1)}, {len(source2)}, {len(source3)}, {len(source4)}')

            source = pd.merge(source2, source1, left_on=[self.hospital, self.orddate, "PRCPNO", "PRCPHISTNO"], right_on=[self.hospital, self.orddate, "PRCPNO", "PRCPHISTNO"], how="inner", suffixes=("", "_diag1"))
            logging.debug(f'source1, source2 병합 후 데이터 개수:, {len(source)}')
            del source1
            del source2

            source = pd.merge(source, source3, left_on=[self.hospital, self.orddate, "EXECPRCPUNIQNO"], right_on=[self.hospital, self.orddate, "EXECPRCPUNIQNO"], how="inner", suffixes=("", "_diag3"))
            logging.debug(f'source, source3 병합 후 데이터 개수:, {len(source)}')

            source = pd.merge(source, source4, left_on=[self.hospital, "BCNO", "TCLSCD", self.spccd], right_on=[self.hospital, "BCNO", "TCLSCD", self.spccd], how="inner", suffixes=("", "_diag4"))
            logging.debug(f'source, source4 병합 후 데이터 개수:, {len(source)}')
            del source3
            del source4

            # visit_source_key 생성
            source["진료일시"] = source[self.orddd]
            source["처방일"] = source[self.orddate]
            source["보고일시"] = source[self.measurement_date]
            source["실시일시"] = source["EXECDD"] + source["EXECTM"]
            source["접수일시"] = source["SPCACPTDT"]

            source[self.orddd] = pd.to_datetime(source[self.orddd])
            source["visit_source_key"] = source[self.person_source_value] + ';' + source[self.orddd].dt.strftime("%Y%m%d") + ';' + source[self.visit_no] + ';' + source[self.hospital]
            source[self.measurement_date] = pd.to_datetime(source[self.measurement_date])

            # value_as_number float형태로 저장되게 값 변경
            # source["value_as_number"] = source[self.value_source_value].str.extract('(-?\d+\.\d+|\d+)')
            source["value_as_number"] = source[self.value_source_value].apply(convert_to_numeric)
            source["value_as_number"].astype(float)
            # source[self.range_low] = source[self.range_low].str.extract('(-?\d+\.\d+|\d+)')
            source[self.range_low] = source[self.range_low].apply(convert_to_numeric)
            source[self.range_low].astype(float)
            # source[self.range_high] = source[self.range_high].str.extract('(-?\d+\.\d+|\d+)')
            source[self.range_high] = source[self.range_high].apply(convert_to_numeric)
            source[self.range_high].astype(float)
            
            person_data = person_data[["person_id", "person_source_value", "환자명"]]
            # person table과 병합
            source = pd.merge(source, person_data, left_on=self.person_source_value, right_on="person_source_value", how="inner")
            del person_data
            logging.debug(f'person 테이블과 결합 후 데이터 row수: {len(source)}')
            
            # local_edi 전처리
            local_edi = local_edi[[self.ordcode, self.fromdate, self.todate, self.edicode, "concept_id", self.hospital, "TCLSNM", self.spccd, "ORDNM"]]
            local_edi[self.fromdate] = pd.to_datetime(local_edi[self.fromdate] , format="%Y%m%d", errors="coerce")
            local_edi[self.todate] = pd.to_datetime(local_edi[self.todate] , format="%Y%m%d", errors="coerce")

            source = pd.merge(source, local_edi, left_on=[self.measurement_source_value, self.spccd, self.hospital], right_on=[self.ordcode, self.spccd, self.hospital], how="left", suffixes=('', '_testcd'))
            logging.debug(f"EDI코드 사용기간별 필터 적용 전 데이터 row수: {len(source)}")

            source[self.fromdate] = source[self.fromdate].fillna(pd.Timestamp('1900-01-01'))
            source[self.todate] = source[self.todate].fillna(pd.Timestamp('2099-12-31'))

            source = pd.merge(source, local_edi, left_on=[self.ordcode, self.spccd, self.hospital], right_on=[self.ordcode, self.spccd, self.hospital], how="left", suffixes=('', '_order'))
            source = source[(source[self.orddate] >= source[self.fromdate]) & (source[self.orddate] <= source[self.todate])]
            del local_edi
            logging.debug(f'EDI코드 테이블과 병합 후 데이터 row수:, {len(source)}')
    
            # 데이터 컬럼 줄이기
            care_site_data = care_site_data[["care_site_id", "care_site_source_value", "place_of_service_source_value", "care_site_name", self.care_site_fromdate, self.care_site_todate]]
            provider_data = provider_data[["provider_id", "provider_source_value", "provider_name"]]
            visit_data = visit_data[["visit_occurrence_id", "visit_start_date", "care_site_id", "visit_source_value", "person_id", "visit_source_key"]]

            # care_site table과 병합
            source = pd.merge(source, care_site_data, left_on=[self.meddept, self.hospital], right_on=["care_site_source_value", "place_of_service_source_value"], how="left")
            del care_site_data
            logging.debug(f'care_site 테이블과 결합 후 데이터 row수: {len(source)}')

            source[self.care_site_fromdate] = pd.to_datetime(source[self.care_site_fromdate], errors = "coerce")
            source[self.care_site_todate] = pd.to_datetime(source[self.care_site_todate], errors = "coerce")
            source[self.care_site_fromdate] = source[self.care_site_fromdate].fillna("19000101")
            source[self.care_site_todate] = source[self.care_site_todate].fillna("20991231")
            source= source[(source[self.frstrgstdt] >= source[self.care_site_fromdate]) & (source[self.frstrgstdt] <= source[self.care_site_todate])]
            logging.debug(f"care_site 사용 기간 조건 설정 후 원천 데이터 row수: {len(source)}")

            # provider table과 병합
            source = pd.merge(source, provider_data, left_on=self.provider, right_on="provider_source_value", how="left", suffixes=('', '_y'))
            logging.debug(f'provider 테이블과 결합 후 데이터 row수: {len(source)}')

            # visit_start_datetime 형태 변경
            # source["ORDDD"] = pd.to_datetime(source["ORDDD"])
            visit_data["visit_start_date"] = pd.to_datetime(visit_data["visit_start_date"])

            # visit_occurrence table과 병합
            source = pd.merge(source, visit_data, left_on=["visit_source_key"], right_on=["visit_source_key"], how="left", suffixes=('', '_y'))
            del visit_data
            logging.debug(f'visit_occurrence 테이블과 결합 후 데이터 row수: {len(source)}')

            visit_detail = visit_detail[["visit_detail_id", "visit_detail_start_datetime", "visit_detail_end_datetime", "visit_occurrence_id"]]
            visit_detail["visit_detail_start_datetime"] = pd.to_datetime(visit_detail["visit_detail_start_datetime"])
            visit_detail["visit_detail_end_datetime"] = pd.to_datetime(visit_detail["visit_detail_end_datetime"])
            source = pd.merge(source, visit_detail, left_on=["visit_occurrence_id"], right_on=["visit_occurrence_id"], how="left", suffixes=('', '_y'))
            logging.debug(f"visit_detail 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            source["visit_detail_id"] = source.apply(lambda row: row['visit_detail_id'] if pd.notna(row['visit_detail_start_datetime']) and row['visit_detail_start_datetime'] <= row[self.orddate] <= row['visit_detail_end_datetime'] else pd.NA, axis=1)
            source = source.drop(columns = ["visit_detail_start_datetime", "visit_detail_end_datetime"])
            # visit_detail로 인해 중복되는 항목 제거를 위함.
            source = source.drop_duplicates(subset=[self.hospital, self.person_source_value, self.orddd, self.orddate, self.visit_no, self.measurement_source_value, self.ordcode, self.spccd])
            logging.debug(f"visit_detail 테이블과 결합 후 조건 적용 후 원천 데이터 row수: {len(source)}")

            # 값이 없는 경우 0으로 값 입력
            # source.loc[source["care_site_id"].isna(), "care_site_id"] = 0
            source.loc[source["concept_id"].isna(), "concept_id"] = 0
            
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
            source["value_as_concept_id"] = np.select(value_concept_condition, value_concept_value)
            source = pd.merge(source, concept_etc, left_on = "value_as_concept_id", right_on="concept_id", how="left", suffixes=('', '_value_as_concept'))
            logging.debug(f'concept_etc: value_as_concept_id 테이블과 결합 후 데이터 row수: {len(source)}')

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
            measurement_date_condition = [source[self.measurement_date].notna()]
            measurement_date_value = [source[self.measurement_date].dt.date]
            measurement_datetime_value = [source[self.measurement_date]]
            measurement_time_value = [source[self.measurement_date].dt.time]

            unit_id_condition = [
                source["concept_id_unit"].notnull(),
                source["concept_id_synonym"].notnull()
            ]

            unit_id_value = [
                source["concept_id_unit"],
                source["concept_id_synonym"]
            ]

            unit_name_condition = [
                source["concept_id_unit"].notnull(),
                source["concept_id_synonym"].notnull()
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
                "measurement_date": np.select(measurement_date_condition, measurement_date_value, default=source[self.orddate].dt.date),
                "measurement_datetime": np.select(measurement_date_condition, measurement_datetime_value, default=source[self.orddate]),
                "measurement_time": np.select(measurement_date_condition, measurement_time_value, default=source[self.orddate].dt.time),
                # "measurement_date_type": np.select(measurement_date_condition, ["보고일"], default="처방일"),
                "measurement_type_concept_id": np.select([source["measurement_type_concept_id"].notna()], [source["measurement_type_concept_id"]], default=self.no_matching_concept[0]),
                "measurement_type_concept_id_name": np.select([source["concept_name_measurement_type"].notna()], [source["concept_name_measurement_type"]], default=self.no_matching_concept[1]),
                "operator_concept_id": np.select([source["operator_concept_id"].notna()], [source["operator_concept_id"]], default=self.no_matching_concept[0]),
                "operator_concept_id_name": np.select([source["concept_name_operator"].notna()], [source["concept_name_operator"]], default=self.no_matching_concept[1]) ,
                "value_as_number": source["value_as_number"],
                "value_as_concept_id": np.select([source["value_as_concept_id"].notna()], [source["value_as_concept_id"]], default=self.no_matching_concept[0]),
                "value_as_concept_id_name": np.select([source["concept_name_value_as_concept"].notna()], [source["concept_name_value_as_concept"]], default=self.no_matching_concept[1]),
                "unit_concept_id": np.select(unit_id_condition, unit_id_value, default=self.no_matching_concept[0]),
                "unit_concept_id_name": np.select(unit_name_condition, unit_name_value, default=self.no_matching_concept[1]),
                "range_low": source[self.range_low],
                "range_high": source[self.range_high],
                "provider_id": source["provider_id"],
                "provider_name": source["provider_name"],
                "visit_occurrence_id": source["visit_occurrence_id"],
                "visit_detail_id": source["visit_detail_id"],
                "measurement_source_value": source[self.measurement_source_value],
                "measurement_source_value_name": source["TCLSNM"],
                "measurement_source_concept_id": np.select([source["concept_id"].notna()], [source["concept_id"]], default=self.no_matching_concept[0]),
                "EDI코드": source[self.edicode],
                "unit_source_value": source[self.unit_source_value],
                "value_source_value": source[self.value_source_value].str[:50],
                "vocabulary_id": "EDI",
                "visit_source_key": source["visit_source_key"],
                "처방코드": source[self.ordcode],
                "처방명": source["ORDNM"],
                "환자구분": source["visit_source_value"],
                "진료과": source[self.meddept],
                "진료과명": source["care_site_name"],
                "처방일": source["처방일"],
                "진료일시": source["진료일시"],
                "접수일시": source["접수일시"],
                "실시일시": source["실시일시"],
                "판독일시": None,
                "보고일시": source["보고일시"],
                "처방순번": source[self.visit_no],
                "정상치(상)": source[self.range_high],
                "정상치(하)": source[self.range_low],
                # "나이": None,
                "결과내역": source[self.value_source_value]
                })

            logging.debug(f'CDM 데이터 row수: {len(cdm)}')
            logging.debug(f"요약:\n{cdm.describe(include = 'all').T.to_string()}")
            logging.debug(f"컬럼별 null 개수:\n{cdm.isnull().sum().to_string()}")

            return cdm   

        except Exception as e :
            logging.error(f"{self.table} 테이블 CDM 데이터 변환 중 오류:\n {e}", exc_info = True)


class MeasurementpthTransformer(DataTransformer):
    def __init__(self, config_path):
        super().__init__(config_path)
        self.table = "measurement_pth"
        self.cdm_config = self.config[self.table]

        # 컬럼 변수 재정의   
        self.source_data1 = self.cdm_config["data"]["source_data1"]
        self.source_data2 = self.cdm_config["data"]["source_data2"]
        self.source_data3 = self.cdm_config["data"]["source_data3"]
        self.source_data4 = self.cdm_config["data"]["source_data4"]
        self.output_filename = self.cdm_config["data"]["output_filename"]
        self.meddept = self.cdm_config["columns"]["meddept"]
        self.provider = self.cdm_config["columns"]["provider"]
        self.orddate = self.cdm_config["columns"]["orddate"]
        self.measurement_date = self.cdm_config["columns"]["measurement_date"]
        self.measurement_source_value = self.cdm_config["columns"]["measurement_source_value"]
        self.value_source_value = self.cdm_config["columns"]["value_source_value"]
        self.range_low = self.cdm_config["columns"]["range_low"]
        self.range_high = self.cdm_config["columns"]["range_high"]
        self.orddd = self.cdm_config["columns"]["orddd"]
        self.unit_source_value = self.cdm_config["columns"]["unit_source_value"]
        self.ordcode = self.cdm_config["columns"]["ordcode"]
                
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
            source1 = self.read_csv(self.source_data1, path_type = self.source_flag, dtype = self.source_dtype)
            source2 = self.read_csv(self.source_data2, path_type = self.source_flag, dtype = self.source_dtype)
            source3 = self.read_csv(self.source_data3, path_type = self.source_flag, dtype = self.source_dtype)
            source4 = self.read_csv(self.source_data4, path_type = self.source_flag, dtype = self.source_dtype)
            local_edi = self.read_csv(self.procedure_edi_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            person_data = self.read_csv(self.person_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            provider_data = self.read_csv(self.provider_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            care_site_data = self.read_csv(self.care_site_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_data = self.read_csv(self.visit_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_detail = self.read_csv(self.visit_detail, path_type = self.cdm_flag, dtype = self.source_dtype)
            # unit_data = self.read_csv(self.concept_unit, path_type = self.source_flag , dtype = self.source_dtype, encoding=self.cdm_encoding)
            concept_etc = self.read_csv(self.concept_etc, path_type = self.source_flag , dtype = self.source_dtype, encoding=self.cdm_encoding)
            # unit_concept_synonym = self.read_csv(self.unit_concept_synonym, path_type = self.source_flag , dtype = self.source_dtype, encoding=self.cdm_encoding)
            logging.debug(f'원천 데이터 row수: {len(source1)}, {len(source2)}, {len(source3)}, {len(source4)}')

            # 원천에서 조건걸기
            source1 = source1[[self.hospital, self.person_source_value, "PTNO", "RSLTRGSTDD", "RSLTRGSTNO", "RSLTRGSTHISTNO", "DELFLAGCD", "HISTNO"]]
            source1 = source1[(source1["RSLTRGSTHISTNO"] == "1") & (source1["DELFLAGCD"] == "0") & (source1["HISTNO"] == "1") ]
            
            source2 = source2[[self.hospital, self.person_source_value, "PTNO", "RSLTRGSTDD", "RSLTRGSTNO", "RSLTRGSTHISTNO", self.value_source_value]]

            source3 = source3[[self.hospital, "PTNO", "PRCPDD", "PRCPNO", "ACPTSTATCD", self.measurement_source_value, "SPCCD", "SPCACPTDD", "READDD", "READTM", "ACPTDD", "ACPTTM"]]
            source3[self.orddate] = pd.to_datetime(source3[self.orddate])
            source3 = source3[(source3[self.orddate] <= self.data_range)]
            source3 = source3[source3["ACPTSTATCD"].isin(["3", "4"])]

            source4 = source4[[self.hospital, self.orddate, self.person_source_value, "PRCPHISTNO", "ORDDD", "CRETNO", "PRCPCLSCD", "LASTUPDTDT", "ORDDRID", "PRCPNM", "PRCPCD", "PRCPHISTCD", "PRCPNO", "ORDDEPTCD", self.frstrgstdt]]
            source4[self.orddate] = pd.to_datetime(source4[self.orddate])
            source4 = source4[(source4[self.orddate] <= self.data_range)]
            source4 = source4[(source4["PRCPHISTCD"] == "O")]

            logging.debug(f'조건적용 후 원천 데이터 row수: {len(source1)}, {len(source2)}, {len(source3)}, {len(source4)}')

            source = pd.merge(source1, source2, left_on=[self.hospital, self.person_source_value, "PTNO", "RSLTRGSTNO", "RSLTRGSTDD", "RSLTRGSTHISTNO"], right_on=[self.hospital, self.person_source_value, "PTNO", "RSLTRGSTNO", "RSLTRGSTDD", "RSLTRGSTHISTNO"], how="inner", suffixes=("", "_pth2"))
            logging.debug(f'source1, source2 병합 후 데이터 개수:, {len(source)}')
            del source1
            del source2

            source = pd.merge(source, source3, left_on=[self.hospital, "PTNO"], right_on=[self.hospital, "PTNO"], how="inner", suffixes=("", "_pth3"))
            logging.debug(f'source, source3 병합 후 데이터 개수:, {len(source)}')

            source = pd.merge(source, source4, left_on=[self.hospital, "PRCPDD", "PRCPNO"], right_on=[self.hospital, "PRCPDD", "PRCPNO"], how="inner", suffixes=("", "_pth4"))
            logging.debug(f'source, source4 병합 후 데이터 개수:, {len(source)}')
            del source3
            del source4

            # visit_source_key 생성
            source["처방일"] = source[self.orddate]
            source["진료일시"] = source[self.orddd]
            source["접수일시"] = source["SPCACPTDD"]
            source["실시일시"] = source["ACPTDD"] + source["ACPTTM"]
            source["판독일시"] = source["READDD"] + source["READTM"]
            source["보고일시"] = source[self.measurement_date]

            source[self.orddd] = pd.to_datetime(source[self.orddd])
            source["visit_source_key"] = source[self.person_source_value] + ';' + source[self.orddd].dt.strftime("%Y%m%d") + ';' + source[self.visit_no] + ';' + source[self.hospital]
            source[self.measurement_date] = pd.to_datetime(source[self.measurement_date])

            person_data = person_data[["person_id", "person_source_value", "환자명"]]
            # person table과 병합
            source = pd.merge(source, person_data, left_on=self.person_source_value, right_on="person_source_value", how="inner")
            del person_data
            logging.debug(f'person 테이블과 결합 후 데이터 row수: {len(source)}')
            
            # local_edi 전처리
            local_edi = local_edi[[self.ordcode, self.fromdate, self.todate, self.edicode, "concept_id", self.hospital, "ORDNM", "PRCPNM"]]
            local_edi[self.fromdate] = pd.to_datetime(local_edi[self.fromdate] , format="%Y%m%d", errors="coerce")
            # local_edi[self.fromdate].fillna(pd.Timestamp('1900-01-01'), inplace = True)
            local_edi[self.todate] = pd.to_datetime(local_edi[self.todate] , format="%Y%m%d", errors="coerce")
            # local_edi[self.todate].fillna(pd.Timestamp('2099-12-31'), inplace = True)

            # source = pd.merge(source, local_edi, left_on=[self.measurement_source_value, self.hospital], right_on=[self.ordcode, self.hospital], how="left")
            # del local_edi
            # logging.debug(f'EDI코드 테이블과 병합 후 데이터 row수:, {len(source)}')

            source = pd.merge(source, local_edi, left_on=[self.ordcode, self.hospital], right_on=[self.ordcode, self.hospital], how="left", suffixes=('', '_order'))
            source[self.fromdate] = source[self.fromdate].fillna(pd.Timestamp('1900-01-01'))
            source[self.todate] = source[self.todate].fillna(pd.Timestamp('2099-12-31'))
            source = source[(source[self.orddate] >= source[self.fromdate]) & (source[self.orddate] <= source[self.todate])]
            del local_edi
            logging.debug(f'EDI코드 테이블과 병합 후 데이터 row수:, {len(source)}')

            # 데이터 컬럼 줄이기
            
            care_site_data = care_site_data[["care_site_id", "care_site_source_value", "place_of_service_source_value", "care_site_name", self.care_site_fromdate, self.care_site_todate]]
            provider_data = provider_data[["provider_id", "provider_source_value", "provider_name"]]
            visit_data = visit_data[["visit_occurrence_id", "visit_start_date", "care_site_id", "visit_source_value", "person_id", "visit_source_key"]]


            # care_site table과 병합
            source = pd.merge(source, care_site_data, left_on=[self.meddept, self.hospital], right_on=["care_site_source_value", "place_of_service_source_value"], how="left")
            del care_site_data
            logging.debug(f'care_site 테이블과 결합 후 데이터 row수: {len(source)}')

            source[self.care_site_fromdate] = pd.to_datetime(source[self.care_site_fromdate], errors = "coerce")
            source[self.care_site_todate] = pd.to_datetime(source[self.care_site_todate], errors = "coerce")
            source[self.care_site_fromdate] = source[self.care_site_fromdate].fillna("19000101")
            source[self.care_site_todate] = source[self.care_site_todate].fillna("20991231")
            source= source[(source[self.frstrgstdt] >= source[self.care_site_fromdate]) & (source[self.frstrgstdt] <= source[self.care_site_todate])]
            logging.debug(f"care_site 사용 기간 조건 설정 후 원천 데이터 row수: {len(source)}")

            # provider table과 병합
            source = pd.merge(source, provider_data, left_on=self.provider, right_on="provider_source_value", how="left", suffixes=('', '_y'))
            logging.debug(f'provider 테이블과 결합 후 데이터 row수: {len(source)}')

            # visit_start_datetime 형태 변경
            source[self.orddd] = pd.to_datetime(source[self.orddd])
            visit_data["visit_start_date"] = pd.to_datetime(visit_data["visit_start_date"])

            # visit_occurrence table과 병합
            source = pd.merge(source, visit_data, left_on=["visit_source_key"], right_on=["visit_source_key"], how="left", suffixes=('', '_y'))
            del visit_data
            logging.debug(f'visit_occurrence 테이블과 결합 후 데이터 row수: {len(source)}')

            visit_detail = visit_detail[["visit_detail_id", "visit_detail_start_datetime", "visit_detail_end_datetime", "visit_occurrence_id"]]
            visit_detail["visit_detail_start_datetime"] = pd.to_datetime(visit_detail["visit_detail_start_datetime"])
            visit_detail["visit_detail_end_datetime"] = pd.to_datetime(visit_detail["visit_detail_end_datetime"])
            source = pd.merge(source, visit_detail, left_on=["visit_occurrence_id"], right_on=["visit_occurrence_id"], how="left", suffixes=('', '_y'))
            logging.debug(f"visit_detail 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            source["visit_detail_id"] = source.apply(lambda row: row['visit_detail_id'] if pd.notna(row['visit_detail_start_datetime']) and row['visit_detail_start_datetime'] <= row[self.orddate] <= row['visit_detail_end_datetime'] else pd.NA, axis=1)
            source = source.drop(columns = ["visit_detail_start_datetime", "visit_detail_end_datetime"])
            source = source.drop_duplicates()
            logging.debug(f"visit_detail 테이블과 결합 후 조건 적용 후 원천 데이터 row수: {len(source)}")

            # 값이 없는 경우 0으로 값 입력
            # source.loc[source["care_site_id"].isna(), "care_site_id"] = 0
            source.loc[source["concept_id"].isna(), "concept_id"] = 0
            
            ### unit매핑 작업 ###
            # concept_unit과 병합
            # unit_data = unit_data[["concept_id", "concept_name", "concept_code"]]
            # source = pd.merge(source, unit_data, left_on=self.unit_source_value, right_on="concept_code", how="left", suffixes=["", "_unit"])
            # logging.debug(f'unit 테이블과 결합 후 데이터 row수: {len(source)}')
            # unit 동의어 적용
            # source = pd.merge(source, unit_concept_synonym, left_on = self.unit_source_value, right_on = "concept_synonym_name", how = "left", suffixes=["", "_synonym"])
            # logging.debug(f'unit synonym 테이블과 결합 후 데이터 row수: {len(source)}')
            

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
            source["value_as_concept_id"] = np.select(value_concept_condition, value_concept_value)
            source = pd.merge(source, concept_etc, left_on = "value_as_concept_id", right_on="concept_id", how="left", suffixes=('', '_value_as_concept'))
            logging.debug(f'concept_etc: value_as_concept_id 테이블과 결합 후 데이터 row수: {len(source)}')

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
            measurement_date_condition = [source[self.measurement_date].notna()]
            measurement_date_value = [source[self.measurement_date].dt.date]
            measurement_datetime_value = [source[self.measurement_date]]
            measurement_time_value = [source[self.measurement_date].dt.time]

            # unit_id_condition = [
            #     source["concept_id_unit"].notnull(),
            #     source["concept_id_synonym"].notnull()
            # ]

            # unit_id_value = [
            #     source["concept_id_unit"],
            #     source["concept_id_synonym"]
            # ]

            # unit_name_condition = [
            #     source["concept_id_unit"].notnull(),
            #     source["concept_id_synonym"].notnull()
            # ]

            # unit_name_value = [
            #     source["concept_name"],
            #     source["concept_name_synonym"]
            # ]

            cdm = pd.DataFrame({
                "measurement_id": 1,
                "person_id": source["person_id"],
                "measurement_concept_id": np.select([source["concept_id"].notna()], [source["concept_id"]], default=self.no_matching_concept[0]),
                "measurement_date": np.select(measurement_date_condition, measurement_date_value, default=source[self.orddate].dt.date),
                "measurement_datetime": np.select(measurement_date_condition, measurement_datetime_value, default=source[self.orddate]),
                "measurement_time": np.select(measurement_date_condition, measurement_time_value, default=source[self.orddate].dt.time),
                # "measurement_date_type": np.select(measurement_date_condition, ["결과등록일자"], default="처방일"),
                "measurement_type_concept_id": np.select([source["measurement_type_concept_id"].notna()],[source["measurement_type_concept_id"]], default=self.no_matching_concept[1]),
                "measurement_type_concept_id_name": np.select([source["concept_name"].notna()], [source["concept_name"]], default=self.no_matching_concept[1]), #source["concept_name_measurement_type"],
                "operator_concept_id": np.select([source["operator_concept_id"].notna()], [source["operator_concept_id"]], default=self.no_matching_concept[1]),
                "operator_concept_id_name": np.select([source["concept_name_operator"].notna()], [source["concept_name_operator"]], default=self.no_matching_concept[1]) ,
                "value_as_number": None,
                "value_as_concept_id": np.select([source["value_as_concept_id"].notna()], [source["value_as_concept_id"]], default=self.no_matching_concept[0]),
                "value_as_concept_id_name": np.select([source["concept_name_value_as_concept"].notna()], [source["concept_name_value_as_concept"]], default=self.no_matching_concept[1]) ,
                "unit_concept_id": self.no_matching_concept[0], 
                "unit_concept_id_name": self.no_matching_concept[1],
                "range_low": self.range_low,
                "range_high": self.range_high,
                "provider_id": source["provider_id"],
                "provider_name": source["provider_name"],
                "visit_occurrence_id": source["visit_occurrence_id"],
                "visit_detail_id": source["visit_detail_id"],
                "measurement_source_value": source[self.measurement_source_value],
                "measurement_source_value_name": source["PRCPNM"],
                "measurement_source_concept_id": np.select([source["concept_id"].notna()], [source["concept_id"]], default=self.no_matching_concept[0]),
                "EDI코드": source[self.edicode],
                "unit_source_value": self.unit_source_value,
                "value_source_value": source[self.value_source_value].str[:50],
                "vocabulary_id": "EDI",
                "visit_source_key": source["visit_source_key"],
                "처방코드": source[self.measurement_source_value],
                "처방명": source["PRCPNM"],
                "환자구분": source["visit_source_value"],
                "진료과": source[self.meddept],
                "진료과명": source["care_site_name"],
                "처방일": source["처방일"],
                "진료일시": source["진료일시"],
                "접수일시": source["접수일시"],
                "실시일시": source["실시일시"],
                "판독일시": source["판독일시"],
                "보고일시": source["보고일시"],
                "처방순번": source[self.visit_no],
                "정상치(상)": self.range_high,
                "정상치(하)": self.range_low,
                # "나이": None,
                "결과내역": source[self.value_source_value]
                })
                
            cdm = cdm.drop_duplicates()
            cdm.reset_index(drop=True, inplace=True)

            cdm["measurement_id"] = cdm.index + 1

            logging.debug(f'CDM 데이터 row수: {len(cdm)}')
            logging.debug(f"요약:\n{cdm.describe(include = 'all').T.to_string()}")
            logging.debug(f"컬럼별 null 개수:\n{cdm.isnull().sum().to_string()}")

            return cdm   

        except Exception as e :
            logging.error(f"{self.table} 테이블 CDM 데이터 변환 중 오류:\n {e}", exc_info = True)


class MeasurementVSTransformer(DataTransformer):
    def __init__(self, config_path):
        super().__init__(config_path)
        self.table = "measurement_vs"
        self.cdm_config = self.config[self.table]

        self.source_data = self.cdm_config["data"]["source_data"]
        self.output_filename = self.cdm_config["data"]["output_filename"]

        self.orddd = self.cdm_config["columns"]["orddd"]
        self.measurement_date = self.cdm_config["columns"]["measurement_date"]
        self.value_source_value = self.cdm_config["columns"]["value_source_value"]
        self.measurement_source_value = self.cdm_config["columns"]["measurement_source_value"]
        
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
            person_data = self.read_csv(self.person_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            # provider_data = self.read_csv(self.provider_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_data = self.read_csv(self.visit_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_detail = self.read_csv(self.visit_detail, path_type = self.cdm_flag, dtype = self.source_dtype)
            concept_etc = self.read_csv(self.concept_etc, path_type = self.source_flag, dtype = self.source_dtype, encoding=self.cdm_encoding)
            logging.debug(f'원천 데이터 row수: {len(source)}')

            source["visit_source_key"] = source[self.person_source_value] + ';' + source[self.orddd] + ';' + ';'

            # 원천에서 조건걸기
            source["처방일"] = source[self.measurement_date]
            source[self.measurement_date] = pd.to_datetime(source[self.measurement_date], errors="coerce")
            source = source[(source[self.measurement_date] <= self.data_range)]
            logging.debug(f'조건 적용후 원천 데이터 row수: {len(source)}')

            # CDM 데이터 컬럼 줄이기
            person_data = person_data[["person_id", "person_source_value", "환자명"]]
            visit_data = visit_data[["visit_occurrence_id", "visit_start_date", "care_site_id", "visit_source_value", "person_id"]]

            # person table과 병합
            source = pd.merge(source, person_data, left_on=self.person_source_value, right_on="person_source_value", how="inner")
            logging.debug(f'person 테이블과 결합 후 원천 데이터 row수: {len(source)}')

            # provider table과 병합
            # source = pd.merge(source, provider_data, left_on=self.provider, right_on="provider_source_value", how="left", suffixes=('', '_y'))
            # logging.debug(f'provider 테이블과 결합 후 원천 데이터 row수: {len(source)}')

            # visit_start_datetime 형태 변경
            visit_data["visit_start_date"] = pd.to_datetime(visit_data["visit_start_date"])

            # visit_occurrence table과 병합
            visit_data["visit_start_date"] = pd.to_datetime(visit_data["visit_start_date"])
            source = pd.merge(source, visit_data, left_on=["person_id", self.measurement_date], right_on=["person_id", "visit_start_date"], how="left", suffixes=('', '_y'))
            logging.debug(f'visit_occurrence 테이블과 결합 후 원천 데이터 row수: {len(source)}')

            visit_detail = visit_detail[["visit_detail_id", "visit_detail_start_datetime", "visit_detail_end_datetime", "visit_occurrence_id"]]
            visit_detail["visit_detail_start_datetime"] = pd.to_datetime(visit_detail["visit_detail_start_datetime"])
            visit_detail["visit_detail_end_datetime"] = pd.to_datetime(visit_detail["visit_detail_end_datetime"])
            source = pd.merge(source, visit_detail, left_on=["visit_occurrence_id"], right_on=["visit_occurrence_id"], how="left", suffixes=('', '_y'))
            logging.debug(f"visit_detail 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            source["visit_detail_id"] = source.apply(lambda row: row['visit_detail_id'] if pd.notna(row['visit_detail_start_datetime']) and row['visit_detail_start_datetime'] <= row[self.measurement_date] <= row['visit_detail_end_datetime'] else pd.NA, axis=1)
            source = source.drop(columns = ["visit_detail_start_datetime", "visit_detail_end_datetime"])
            source = source.drop_duplicates()
            logging.debug(f"visit_detail 테이블과 결합 후 조건 적용 후 원천 데이터 row수: {len(source)}")

            ### concept_etc테이블과 병합 ###
            concept_etc["concept_id"] = concept_etc["concept_id"].astype(int)   
            # type_concept_id 만들고 type_concept_id_name 기반 만들기
            source["measurement_type_concept_id"] = 44818702
            source = pd.merge(source, concept_etc, left_on = "measurement_type_concept_id", right_on="concept_id", how="left", suffixes=('', '_measurement_type'))
            logging.debug(f'concept_etc: type_concept_id 테이블과 결합 후 데이터 row수: {len(source)}')

            # 값이 없는 경우 0으로 값 입력
            source.loc[source["care_site_id"].isna(), "care_site_id"] = 0

            logging.debug(f'CDM 테이블과 결합 후 원천 데이터 row수: {len(source)}')

            return source

        except Exception as e :
            logging.error(f"{self.table} 테이블 소스 데이터 처리 중 오류: {e}", exc_info = True)

    def transform_cdm(self, source):
        """
        주어진 소스 데이터를 CDM 형식에 맞게 변환하는 메소드.
        변환된 데이터는 새로운 DataFrame으로 구성됩니다.
        """
        try :
            # concept_id 정의, {key: [measurement_concept_id, measurement_concept_name, unit_concept_id, unit_concept_name]}
            # measurement_concept = {
            #     "height": [4177340, "body height", 8582, "cm"],
            #     "weight": [4099154, "body weight", 9529, "kg"],
            #     "bmi": [40490382, "BMI", 9531, "kilogram per square meter"],
            #     "sbp": [4152194, "systolic blood pressure (SBP)", 4118323, "mmHg"],
            #     "dbp": [4154790, "diastolic blood pressure (DBP)", 4118323, "mmHg"],
            #     "pulse": [4224504, "pulse rate (PR)", 4118124, "beats/min"],
            #     "breth": [4313591, "respiratory rate(RR)", 8541, "respiratory rate(RR)"],
            #     "bdtp": [4302666, "body temperature", 586323, "degree Celsius"],
            #     "spo2": [4020553, "SPO2", 8554, "%"]
            # }

            # 숫자가 아닌 value 값 수정
            # source["value_as_number"] = source[self.value_source_value].str.extract(r'(-?\d+\.\d+|\d+)').copy()
            source["value_as_number"] = source[self.value_source_value].apply(convert_to_numeric).copy()
            source["value_as_number"].astype(float)


            # cdm생성
            cdm = pd.DataFrame({
                "measurement_id": source.index + 1,
                "person_id": source["person_id"],
                "환자명": source["환자명"],
                "measurement_concept_id": self.no_matching_concept[0],
                "measurement_date": source[self.measurement_date].dt.date,
                "measurement_datetime": source[self.measurement_date],
                "measurement_time": source[self.measurement_date].dt.time,
                # "measurement_date_type": "기록일시", 
                "measurement_type_concept_id": 44818702,
                "measurement_type_concept_id_name": source["concept_name"],
                "operator_concept_id": self.no_matching_concept[0],
                "value_as_number": source["value_as_number"],
                "value_as_concept_id": self.no_matching_concept[0],
                "value_as_concept_id_name": self.no_matching_concept[1],
                "unit_concept_id": self.no_matching_concept[0],
                "unit_concept_id_name": self.no_matching_concept[1],
                "range_low": None,
                "range_high": None,
                "provider_id": None,
                "visit_occurrence_id": source["visit_occurrence_id"],
                "visit_detail_id": source["visit_detail_id"],
                "measurement_source_value": source[self.measurement_source_value],
                "measurement_source_value_name": source[self.measurement_source_value],
                "measurement_source_concept_id": self.no_matching_concept[0],
                "EDI코드": None,
                "unit_source_value": None,
                "value_source_value": source[self.value_source_value].str[:50],
                "vocabulary_id": None,
                "visit_source_key": source["visit_source_key"],
                "처방코드": None,
                "처방명": None,
                "환자구분": source["visit_source_value"],
                "진료과": None,
                "진료과명": None,
                "처방일": source["처방일"],
                "진료일시": source[self.orddd],
                "접수일시": None,
                "실시일시": None,
                "판독일시": None,
                "보고일시": None,
                "처방순번": None,
                "정상치(상)": None,
                "정상치(하)": None,
                # "나이": None,
                "결과내역": source[self.value_source_value]
                })

            logging.debug(f'CDM 데이터 row수: {len(cdm)}')
            logging.debug(f"요약:\n{cdm.describe(include = 'all').T.to_string()}")
            logging.debug(f"컬럼별 null 개수:\n{cdm.isnull().sum().to_string()}")

            return cdm   

        except Exception as e :
            logging.error(f"{self.table} 테이블 CDM 데이터 변환 중 오류:\n {e}", exc_info = True)


class MeasurementNITransformer(DataTransformer):
    def __init__(self, config_path):
        super().__init__(config_path)
        self.table = "measurement_ni"
        self.cdm_config = self.config[self.table]

        self.source_data = self.cdm_config["data"]["source_data"]
        self.output_filename = self.cdm_config["data"]["output_filename"]
        self.provider = self.cdm_config["columns"]["provider"]
        self.admtime = self.cdm_config["columns"]["admtime"]
        self.height = self.cdm_config["columns"]["height"]
        self.weight = self.cdm_config["columns"]["weight"]
        self.dbp = self.cdm_config["columns"]["dbp"]
        self.sbp = self.cdm_config["columns"]["sbp"]
        self.pulse = self.cdm_config["columns"]["pulse"]
        self.breth = self.cdm_config["columns"]["breth"]
        self.bdtp = self.cdm_config["columns"]["bdtp"]
        self.spo2 = self.cdm_config["columns"]["spo2"]

        
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
            person_data = self.read_csv(self.person_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            care_site_data = self.read_csv(self.care_site_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            provider_data = self.read_csv(self.provider_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_data = self.read_csv(self.visit_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_detail = self.read_csv(self.visit_detail, path_type = self.cdm_flag, dtype = self.source_dtype)
            concept_etc = self.read_csv(self.concept_etc, path_type = self.source_flag, dtype = self.source_dtype, encoding=self.cdm_encoding)
            logging.debug(f'원천 데이터 row수: {len(source)}')

            # 원천에서 조건걸기
            source = source[[self.person_source_value, self.admtime, self.provider, self.height, self.weight, self.sbp, self.dbp, self.pulse, self.breth, self.bdtp, self.spo2, self.hospital]]
            # visit_source_key 생성
            source["visit_source_key"] = source[self.person_source_value] + ';' + source[self.admtime] + ';' +  ';' + source[self.hospital]

            source[self.admtime] = pd.to_datetime(source[self.admtime], format="%Y%m%d")
            source = source[(source[self.admtime] <= self.data_range)]
            logging.debug(f'조건 적용후 원천 데이터 row수: {len(source)}')

            # CDM 데이터 컬럼 줄이기
            person_data = person_data[["person_id", "person_source_value", "환자명"]]
            care_site_data = care_site_data[["care_site_id", "care_site_source_value", "care_site_name", "place_of_service_source_value"]]
            provider_data = provider_data[["provider_id", "provider_source_value", "provider_name"]]
            visit_data = visit_data[["visit_occurrence_id", "visit_start_date", "care_site_id", "visit_source_value", "person_id", "visit_source_key"]]

            # person table과 병합
            source = pd.merge(source, person_data, left_on=self.person_source_value, right_on="person_source_value", how="inner")
            logging.debug(f'person 테이블과 결합 후 원천 데이터 row수: {len(source)}')
            
            # care_site table과 병합
            # source = pd.merge(source, care_site_data, left_on=self.meddept, right_on="care_site_source_value", how="left")
            # logging.debug(f'care_site 테이블과 결합 후 원천 데이터 row수: {len(source)}')

            # provider table과 병합
            source = pd.merge(source, provider_data, left_on=self.provider, right_on="provider_source_value", how="left", suffixes=('', '_y'))
            logging.debug(f'provider 테이블과 결합 후 원천 데이터 row수: {len(source)}')

            # visit_start_datetime 형태 변경
            visit_data["visit_start_date"] = pd.to_datetime(visit_data["visit_start_date"])

            # visit_occurrence table과 병합
            visit_data = visit_data[visit_data["visit_source_value"] == 'I']
            visit_data["instcd"] = visit_data["visit_source_key"].str.split(';', expand = True)[3]
            visit_data["visit_start_date"] = pd.to_datetime(visit_data["visit_start_date"])
            source = pd.merge(source, visit_data, left_on=["person_id", self.admtime, self.hospital], right_on=["person_id", "visit_start_date", "instcd"], how="left", suffixes=('', '_y'))
            logging.debug(f'visit_occurrence 테이블과 결합 후 원천 데이터 row수: {len(source)}')

            visit_detail = visit_detail[["visit_detail_id", "visit_detail_start_datetime", "visit_detail_end_datetime", "visit_occurrence_id"]]
            visit_detail["visit_detail_start_datetime"] = pd.to_datetime(visit_detail["visit_detail_start_datetime"])
            visit_detail["visit_detail_end_datetime"] = pd.to_datetime(visit_detail["visit_detail_end_datetime"])
            source = pd.merge(source, visit_detail, left_on=["visit_occurrence_id"], right_on=["visit_occurrence_id"], how="left", suffixes=('', '_y'))
            logging.debug(f"visit_detail 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            source["visit_detail_id"] = source.apply(lambda row: row['visit_detail_id'] if pd.notna(row['visit_detail_start_datetime']) and row['visit_detail_start_datetime'] <= row[self.admtime] <= row['visit_detail_end_datetime'] else pd.NA, axis=1)
            source = source.drop(columns = ["visit_detail_start_datetime", "visit_detail_end_datetime"])
            source = source.drop_duplicates()
            logging.debug(f"visit_detail 테이블과 결합 후 조건 적용 후 원천 데이터 row수: {len(source)}")

            ### concept_etc테이블과 병합 ###
            concept_etc["concept_id"] = concept_etc["concept_id"].astype(int)            

            # type_concept_id 만들고 type_concept_id_name 기반 만들기
            source["measurement_type_concept_id"] = 44818702
            source = pd.merge(source, concept_etc, left_on = "measurement_type_concept_id", right_on="concept_id", how="left", suffixes=('', '_measurement_type'))
            logging.debug(f'concept_etc: type_concept_id 테이블과 결합 후 데이터 row수: {len(source)}')

            logging.debug(f'CDM 테이블과 결합 후 원천 데이터 row수: {len(source)}')

            return source

        except Exception as e :
            logging.error(f"{self.table} 테이블 소스 데이터 처리 중 오류: {e}", exc_info = True)

    def transform_cdm(self, source):
        """
        주어진 소스 데이터를 CDM 형식에 맞게 변환하는 메소드.
        변환된 데이터는 새로운 DataFrame으로 구성됩니다.
        """
        try :
            # concept_id 정의, {key: [measurement_concept_id, measurement_concept_name, unit_concept_id, unit_concept_name]}
            measurement_concept = {
                "height": [4177340, "body height", 8582, "cm"],
                "weight": [4099154, "body weight", 9529, "kg"],
                "bmi": [40490382, "BMI", 9531, "kilogram per square meter"],
                "sbp": [4152194, "systolic blood pressure (SBP)", 4118323, "mmHg"],
                "dbp": [4154790, "diastolic blood pressure (DBP)", 4118323, "mmHg"],
                "pulse": [4224504, "pulse rate (PR)", 4118124, "beats/min"],
                "breth": [4313591, "respiratory rate(RR)", 8541, "respiratory rate(RR)"],
                "bdtp": [4302666, "body temperature", 586323, "degree Celsius"],
                "spo2": [4020553, "SPO2", 8554, "%"]
            }

            source_weight = source[source[self.weight].notna()]
            source_height = source[source[self.height].notna()]

            source_bmi = source[source[self.height].notna() | source[self.weight].notna()]

            source_sbp = source[source[self.sbp].notna()]
            source_dbp = source[source[self.dbp].notna()]
            source_pulse = source[source[self.pulse].notna()]
            source_breth = source[source[self.breth].notna()]
            source_bdtp = source[source[self.bdtp].notna()]
            source_spo2 = source[source[self.spo2].notna()]

            # 숫자가 아닌 value 값 수정
            # source_weight["value_as_number"] = source_weight[self.weight].str.extract(r'(-?\d+\.\d+|\d+)').copy()
            source_weight["value_as_number"] = source_weight[self.weight].apply(convert_to_numeric).copy()
            source_weight["value_as_number"].astype(float)

            # source_height["value_as_number"] = source_height[self.height].str.extract(r'(-?\d+\.\d+|\d+)').copy()
            source_height["value_as_number"] = source_height[self.height].apply(convert_to_numeric).copy()
            source_height["value_as_number"].astype(float)

            # source_bmi[self.weight] = source_bmi[self.height].str.extract(r'(-?\d+\.\d+|\d+)').copy()
            source_bmi[self.weight] = source_bmi[self.height].apply(convert_to_numeric).copy()
            source_bmi[self.weight] = source_bmi[self.weight].astype(float)
            # source_bmi[self.height] = source_bmi[self.height].str.extract(r'(-?\d+\.\d+|\d+)').copy()
            source_bmi[self.height] = source_bmi[self.height].apply(convert_to_numeric).copy()
            source_bmi[self.height] = source_bmi[self.height].astype(float)
            source_bmi['bmi'] = round(source_bmi[self.weight] / (source_bmi[self.height]*0.01)**2, 1).copy()

            # source_sbp["value_as_number"] = source_sbp[self.sbp].str.extract(r'(-?\d+\.\d+|\d+)').copy()
            source_sbp["value_as_number"] = source_sbp[self.sbp].apply(convert_to_numeric).copy()
            source_sbp["value_as_number"].astype(float)

            # source_dbp["value_as_number"] = source_dbp[self.dbp].str.extract(r'(-?\d+\.\d+|\d+)').copy()
            source_dbp["value_as_number"] = source_dbp[self.dbp].apply(convert_to_numeric).copy()
            source_dbp["value_as_number"].astype(float)

            # source_pulse["value_as_number"] = source_pulse[self.pulse].str.extract(r'(-?\d+\.\d+|\d+)').copy()
            source_pulse["value_as_number"] = source_pulse[self.pulse].apply(convert_to_numeric).copy()
            source_pulse["value_as_number"].astype(float)

            # source_breth["value_as_number"] = source_breth[self.breth].str.extract(r'(-?\d+\.\d+|\d+)').copy()
            source_breth["value_as_number"] = source_breth[self.breth].apply(convert_to_numeric).copy()
            source_breth["value_as_number"].astype(float)

            # source_bdtp["value_as_number"] = source_bdtp[self.bdtp].str.extract(r'(-?\d+\.\d+|\d+)').copy()
            source_bdtp["value_as_number"] = source_bdtp[self.bdtp].apply(convert_to_numeric).copy()
            source_bdtp["value_as_number"].astype(float)

            # source_spo2["value_as_number"] = source_spo2[self.spo2].str.extract(r'(-?\d+\.\d+|\d+)').copy()
            source_spo2["value_as_number"] = source_spo2[self.spo2].apply(convert_to_numeric).copy()
            source_spo2["value_as_number"].astype(float)

            logging.debug(f"""weight값이 있는 원천 데이터 row수:\n 
                        source_weight: {len(source_weight)}\n
                        source_height: {len(source_height)}\n
                        source_bmi: {len(source_bmi)}\n
                        source_sbp: {len(source_sbp)}\n
                        source_dbp: {len(source_dbp)}\n
                        source_pulse: {len(source_pulse)}\n
                        source_breth: {len(source_breth)}\n
                        source_dbtp: {len(source_bdtp)}\n
                        source_spo2: {len(source_spo2)}\n
                        총합: {len(source_weight) + len(source_height) + len(source_bmi) + len(source_sbp)
                             + len(source_dbp) + len(source_pulse) + len(source_breth) + len(source_bdtp) + len(source_spo2)
                             }
                        """)

            # weight값이 저장된 cdm생성
            cdm_weight = pd.DataFrame({
                "measurement_id": source_weight.index + 1,
                "person_id": source_weight["person_id"],
                "환자명": source_weight["환자명"],
                "measurement_concept_id": measurement_concept["weight"][0],
                "measurement_date": source_weight[self.admtime].dt.date,
                "measurement_datetime": source_weight[self.admtime],
                "measurement_time": source_weight[self.admtime].dt.time,
                # "measurement_date_type": "입원일자", 
                "measurement_type_concept_id": 44818702,
                "measurement_type_concept_id_name": source_weight["concept_name"],
                "operator_concept_id": self.no_matching_concept[0],
                "operator_concept_id_name": self.no_matching_concept[1],
                "value_as_number": source_weight["value_as_number"],
                "value_as_concept_id": self.no_matching_concept[0],
                "value_as_concept_id_name": self.no_matching_concept[1],
                "unit_concept_id": measurement_concept["weight"][2],
                "unit_concept_id_name": measurement_concept["weight"][3],
                "range_low": None,
                "range_high": None,
                "provider_id": source_weight["provider_id"],
                "provider_name": source_weight["provider_name"],
                "visit_occurrence_id": source_weight["visit_occurrence_id"],
                "visit_detail_id": source_weight["visit_detail_id"],
                "measurement_source_value": measurement_concept["weight"][1],
                "measurement_source_value_name": measurement_concept["weight"][1],
                "measurement_source_concept_id": measurement_concept["weight"][0],
                "EDI코드": None,
                "unit_source_value": measurement_concept["weight"][3],
                "value_source_value": source_weight[self.weight],
                "vocabulary_id": "SNOMED",
                "visit_source_key": source_weight["visit_source_key"],
                "처방코드": None,
                "처방명": None,
                "환자구분": source_weight["visit_source_value"],
                "진료과": None,
                "진료과명": None,
                "처방일": None,
                "진료일시": None,
                "접수일시": None,
                "실시일시": None,
                "판독일시": None,
                "보고일시": None,
                "처방순번": None,
                "정상치(상)": None,
                "정상치(하)": None,
                # "나이": None,
                "결과내역": source_weight[self.weight]
                })

            # height값이 저장된 cdm생성
            cdm_height = pd.DataFrame({
                "measurement_id": source_height.index + 1,
                "person_id": source_height["person_id"],
                "환자명": source_height["환자명"],
                "measurement_concept_id": measurement_concept["height"][0],
                "measurement_date": source_height[self.admtime].dt.date,
                "measurement_datetime": source_height[self.admtime],
                "measurement_time": source_height[self.admtime].dt.time, 
                # "measurement_date_type": "입원일자", 
                "measurement_type_concept_id": 44818702,
                "measurement_type_concept_id_name": source_height["concept_name"],
                "operator_concept_id": self.no_matching_concept[0],
                "operator_concept_id_name": self.no_matching_concept[1],
                "value_as_number": source_height["value_as_number"],
                "value_as_concept_id": self.no_matching_concept[0],
                "value_as_concept_id_name": self.no_matching_concept[1],
                "unit_concept_id": measurement_concept["height"][2],
                "unit_concept_id_name": measurement_concept["height"][3],
                "range_low": None,
                "range_high": None,
                "provider_id": source_height["provider_id"],
                "provider_name": source_height["provider_name"],
                "visit_occurrence_id": source_height["visit_occurrence_id"],
                "visit_detail_id": source_height["visit_detail_id"],
                "measurement_source_value": measurement_concept["height"][1],
                "measurement_source_value_name": measurement_concept["height"][1],
                "measurement_source_concept_id": measurement_concept["height"][0],
                "EDI코드": None,
                "unit_source_value": measurement_concept["height"][3],
                "value_source_value": source_height[self.height],
                "vocabulary_id": "SNOMED",
                "visit_source_key": source_height["visit_source_key"],
                "처방코드": None,
                "처방명": None,
                "환자구분": source_height["visit_source_value"],
                "진료과": None,
                "진료과명": None,
                "처방일": None,
                "진료일시": None,
                "접수일시": None,
                "실시일시": None,
                "판독일시": None,
                "보고일시": None,
                "처방순번": None,
                "정상치(상)": None,
                "정상치(하)": None,
                # "나이": None,
                "결과내역": source_height[self.height] 
                })

            # bmi값이 저장된 cdm생성
            cdm_bmi = pd.DataFrame({
                "measurement_id": source_bmi.index + 1,
                "person_id": source_bmi["person_id"],
                "환자명": source_bmi["환자명"],
                "measurement_concept_id": measurement_concept["bmi"][0],
                "measurement_date": source_bmi[self.admtime].dt.date,
                "measurement_datetime": source_bmi[self.admtime],
                "measurement_time": source_bmi[self.admtime].dt.time, 
                # "measurement_date_type": "입원일자", 
                "measurement_type_concept_id": 44818702,
                "measurement_type_concept_id_name": source_bmi["concept_name"],
                "operator_concept_id": self.no_matching_concept[0],
                "operator_concept_id_name": self.no_matching_concept[1],
                "value_as_number": source_bmi["bmi"],
                "value_as_concept_id": self.no_matching_concept[0],
                "value_as_concept_id_name": self.no_matching_concept[1],
                "unit_concept_id": measurement_concept["bmi"][2],
                "unit_concept_id_name": measurement_concept["bmi"][3],
                "range_low": None,
                "range_high": None,
                "provider_id": source_bmi["provider_id"],
                "provider_name": source_bmi["provider_name"],
                "visit_occurrence_id": source_bmi["visit_occurrence_id"],
                "visit_detail_id": source_bmi["visit_detail_id"],
                "measurement_source_value": measurement_concept["bmi"][1],
                "measurement_source_value_name": measurement_concept["bmi"][1],
                "measurement_source_concept_id": measurement_concept["bmi"][0],
                "EDI코드": None,
                "unit_source_value": measurement_concept["bmi"][3],
                "value_source_value": source_bmi["bmi"],
                "vocabulary_id": "SNOMED",
                "visit_source_key": source_bmi["visit_source_key"],
                "처방코드": None,
                "처방명": None,
                "환자구분": source_bmi["visit_source_value"],
                "진료과": None,
                "진료과명": None,
                "처방일": None,
                "진료일시": None,
                "접수일시": None,
                "실시일시": None,
                "판독일시": None,
                "보고일시": None,
                "처방순번": None,
                "정상치(상)": None,
                "정상치(하)": None,
                # "나이": None,
                "결과내역": source_bmi["bmi"]
                })

            cdm_sbp = pd.DataFrame({
                "measurement_id": source_sbp.index + 1,
                "person_id": source_sbp["person_id"],
                "환자명": source_sbp["환자명"],
                "measurement_concept_id": measurement_concept["sbp"][0],
                "measurement_date": source_sbp[self.admtime].dt.date,
                "measurement_datetime": source_sbp[self.admtime],
                "measurement_time": source_sbp[self.admtime].dt.time, 
                # "measurement_date_type": "입원일자", 
                "measurement_type_concept_id": 44818702,
                "measurement_type_concept_id_name": source_sbp["concept_name"],
                "operator_concept_id": self.no_matching_concept[0],
                "operator_concept_id_name": self.no_matching_concept[1],
                "value_as_number": source_sbp["value_as_number"],
                "value_as_concept_id": self.no_matching_concept[0],
                "value_as_concept_id_name": self.no_matching_concept[1],
                "unit_concept_id": measurement_concept["sbp"][2],
                "unit_concept_id_name": measurement_concept["sbp"][3],
                "range_low": None,
                "range_high": None,
                "provider_id": source_sbp["provider_id"],
                "provider_name": source_sbp["provider_name"],
                "visit_occurrence_id": source_sbp["visit_occurrence_id"],
                "visit_detail_id": source_sbp["visit_detail_id"],
                "measurement_source_value": measurement_concept["sbp"][1],
                "measurement_source_value_name": measurement_concept["sbp"][1],
                "measurement_source_concept_id": measurement_concept["sbp"][0],
                "EDI코드": None,
                "unit_source_value": measurement_concept["sbp"][3],
                "value_source_value": source_sbp[self.sbp],
                "vocabulary_id": "SNOMED",
                "visit_source_key": source_sbp["visit_source_key"],
                "처방코드": None,
                "처방명": None,
                "환자구분": source_sbp["visit_source_value"],
                "진료과": None,
                "진료과명": None,
                "처방일": None,
                "진료일시": None,
                "접수일시": None,
                "실시일시": None,
                "판독일시": None,
                "보고일시": None,
                "처방순번": None,
                "정상치(상)": None,
                "정상치(하)": None,
                # "나이": None,
                "결과내역": source_sbp[self.sbp]  
                })

            cdm_dbp = pd.DataFrame({
                "measurement_id": source_dbp.index + 1,
                "person_id": source_dbp["person_id"],
                "환자명": source_dbp["환자명"],
                "measurement_concept_id": measurement_concept["dbp"][0],
                "measurement_date": source_dbp[self.admtime].dt.date,
                "measurement_datetime": source_dbp[self.admtime],
                "measurement_time": source_dbp[self.admtime].dt.time, 
                # "measurement_date_type": "입원일자", 
                "measurement_type_concept_id": 44818702,
                "measurement_type_concept_id_name": source_dbp["concept_name"],
                "operator_concept_id": self.no_matching_concept[0],
                "operator_concept_id_name": self.no_matching_concept[1],
                "value_as_number": source_dbp["value_as_number"],
                "value_as_concept_id": self.no_matching_concept[0],
                "value_as_concept_id_name": self.no_matching_concept[1],
                "unit_concept_id": measurement_concept["dbp"][2],
                "unit_concept_id_name": measurement_concept["dbp"][3],
                "range_low": None,
                "range_high": None,
                "provider_id": source_dbp["provider_id"],
                "provider_name": source_dbp["provider_name"],
                "visit_occurrence_id": source_dbp["visit_occurrence_id"],
                "visit_detail_id": source_dbp["visit_detail_id"],
                "measurement_source_value": measurement_concept["dbp"][1],
                "measurement_source_value_name": measurement_concept["dbp"][1],
                "measurement_source_concept_id": measurement_concept["dbp"][0],
                "EDI코드": None,
                "unit_source_value": measurement_concept["dbp"][3],
                "value_source_value": source_dbp[self.dbp],
                "vocabulary_id": "SNOMED",
                "처방명": None,
                "환자구분": source_dbp["visit_source_value"],
                "진료과": None,
                "진료과명": None,
                "처방일": None,
                "진료일시": None,
                "접수일시": None,
                "실시일시": None,
                "판독일시": None,
                "보고일시": None,
                "처방순번": None,
                "정상치(상)": None,
                "정상치(하)": None,
                # "나이": None,
                "결과내역": source_dbp[self.dbp] 
                })

            cdm_pulse = pd.DataFrame({
                "measurement_id": source_pulse.index + 1,
                "person_id": source_pulse["person_id"],
                "환자명": source_pulse["환자명"],
                "measurement_concept_id": measurement_concept["pulse"][0],
                "measurement_date": source_pulse[self.admtime].dt.date,
                "measurement_datetime": source_pulse[self.admtime],
                "measurement_time": source_pulse[self.admtime].dt.time, 
                # "measurement_date_type": "입원일자", 
                "measurement_type_concept_id": 44818702,
                "measurement_type_concept_id_name": source_pulse["concept_name"],
                "operator_concept_id": self.no_matching_concept[0],
                "operator_concept_id_name": self.no_matching_concept[1],
                "value_as_number": source_pulse["value_as_number"],
                "value_as_concept_id": self.no_matching_concept[0],
                "value_as_concept_id_name": self.no_matching_concept[1],
                "unit_concept_id": measurement_concept["pulse"][2],
                "unit_concept_id_name": measurement_concept["pulse"][3],
                "range_low": None,
                "range_high": None,
                "provider_id": source_pulse["provider_id"],
                "provider_name": source_pulse["provider_name"],
                "visit_occurrence_id": source_pulse["visit_occurrence_id"],
                "visit_detail_id": source_pulse["visit_detail_id"],
                "measurement_source_value": measurement_concept["pulse"][1],
                "measurement_source_value_name": measurement_concept["pulse"][1],
                "measurement_source_concept_id": measurement_concept["pulse"][0],
                "EDI코드": None,
                "unit_source_value": measurement_concept["pulse"][3],
                "value_source_value": source_pulse[self.pulse],
                "vocabulary_id": "SNOMED",
                "visit_source_key": source_pulse["visit_source_key"],
                "처방코드": None,
                "처방명": None,
                "환자구분": source_pulse["visit_source_value"],
                "진료과": None,
                "진료과명": None,
                "처방일": None,
                "진료일시": None,
                "접수일시": None,
                "실시일시": None,
                "판독일시": None,
                "보고일시": None,
                "처방순번": None,
                "정상치(상)": None,
                "정상치(하)": None,
                # "나이": None,
                "결과내역": source_pulse[self.pulse]  
                })

            cdm_breth = pd.DataFrame({
                "measurement_id": source_breth.index + 1,
                "person_id": source_breth["person_id"],
                "환자명": source_breth["환자명"],
                "measurement_concept_id": measurement_concept["breth"][0],
                "measurement_date": source_breth[self.admtime].dt.date,
                "measurement_datetime": source_breth[self.admtime],
                "measurement_time": source_breth[self.admtime].dt.time, 
                # "measurement_date_type": "입원일자", 
                "measurement_type_concept_id": 44818702,
                "measurement_type_concept_id_name": source_breth["concept_name"],
                "operator_concept_id": self.no_matching_concept[0],
                "operator_concept_id_name": self.no_matching_concept[1],
                "value_as_number": source_breth["value_as_number"],
                "value_as_concept_id": self.no_matching_concept[0],
                "value_as_concept_id_name": self.no_matching_concept[1],
                "unit_concept_id": measurement_concept["breth"][2],
                "unit_concept_id_name": measurement_concept["breth"][3],
                "range_low": None,
                "range_high": None,
                "provider_id": source_breth["provider_id"],
                "provider_name": source_breth["provider_name"],
                "visit_occurrence_id": source_breth["visit_occurrence_id"],
                "visit_detail_id": source_breth["visit_detail_id"],
                "measurement_source_value": measurement_concept["breth"][1],
                "measurement_source_value_name": measurement_concept["breth"][1],
                "measurement_source_concept_id": measurement_concept["breth"][0],
                "EDI코드": None,
                "unit_source_value": measurement_concept["breth"][3],
                "value_source_value": source_breth[self.breth],
                "vocabulary_id": "SNOMED",
                "visit_source_key": source_breth["visit_source_key"],
                "처방코드": None,
                "처방명": None,
                "환자구분": source_breth["visit_source_value"],
                "진료과": None,
                "진료과명": None,
                "처방일": None,
                "진료일시": None,
                "접수일시": None,
                "실시일시": None,
                "판독일시": None,
                "보고일시": None,
                "처방순번": None,
                "정상치(상)": None,
                "정상치(하)": None,
                # "나이": None,
                "결과내역": source_breth[self.breth]  
                })

            cdm_bdtp = pd.DataFrame({
                "measurement_id": source_bdtp.index + 1,
                "person_id": source_bdtp["person_id"],
                "환자명": source_bdtp["환자명"],
                "measurement_concept_id": measurement_concept["bdtp"][0],
                "measurement_date": source_bdtp[self.admtime].dt.date,
                "measurement_datetime": source_bdtp[self.admtime],
                "measurement_time": source_bdtp[self.admtime].dt.time, 
                # "measurement_date_type": "입원일자", 
                "measurement_type_concept_id": 44818702,
                "measurement_type_concept_id_name": source_bdtp["concept_name"],
                "operator_concept_id": self.no_matching_concept[0],
                "operator_concept_id_name": self.no_matching_concept[1],
                "value_as_number": source_bdtp["value_as_number"],
                "value_as_concept_id": self.no_matching_concept[0],
                "value_as_concept_id_name": self.no_matching_concept[1],
                "unit_concept_id": measurement_concept["bdtp"][2],
                "unit_concept_id_name": measurement_concept["bdtp"][3],
                "range_low": None,
                "range_high": None,
                "provider_id": source_bdtp["provider_id"],
                "provider_name": source_bdtp["provider_name"],
                "visit_occurrence_id": source_bdtp["visit_occurrence_id"],
                "visit_detail_id": source_bdtp["visit_detail_id"],
                "measurement_source_value": measurement_concept["bdtp"][1],
                "measurement_source_value_name": measurement_concept["bdtp"][1],
                "measurement_source_concept_id": measurement_concept["bdtp"][0],
                "EDI코드": None,
                "unit_source_value": measurement_concept["bdtp"][3],
                "value_source_value": source_bdtp[self.bdtp],
                "vocabulary_id": "SNOMED",
                "visit_source_key": source_bdtp["visit_source_key"],
                "처방코드": None,
                "처방명": None,
                "환자구분": source_bdtp["visit_source_value"],
                "진료과": None,
                "진료과명": None,
                "처방일": None,
                "진료일시": None,
                "접수일시": None,
                "실시일시": None,
                "판독일시": None,
                "보고일시": None,
                "처방순번": None,
                "정상치(상)": None,
                "정상치(하)": None,
                # "나이": None,
                "결과내역": source_bdtp[self.bdtp] 
                })

            cdm_spo2 = pd.DataFrame({
                "measurement_id": source_spo2.index + 1,
                "person_id": source_spo2["person_id"],
                "환자명": source_spo2["환자명"],
                "measurement_concept_id": measurement_concept["spo2"][0],
                "measurement_date": source_spo2[self.admtime].dt.date,
                "measurement_datetime": source_spo2[self.admtime],
                "measurement_time": source_spo2[self.admtime].dt.time, 
                # "measurement_date_type": "입원일자", 
                "measurement_type_concept_id": 44818702,
                "measurement_type_concept_id_name": source_spo2["concept_name"],
                "operator_concept_id": self.no_matching_concept[0],
                "operator_concept_id_name": self.no_matching_concept[1],
                "value_as_number": source_spo2["value_as_number"],
                "value_as_concept_id": self.no_matching_concept[0],
                "value_as_concept_id_name": self.no_matching_concept[1],
                "unit_concept_id": measurement_concept["spo2"][2],
                "unit_concept_id_name": measurement_concept["spo2"][3],
                "range_low": None,
                "range_high": None,
                "provider_id": source_spo2["provider_id"],
                "provider_name": source_spo2["provider_name"],
                "visit_occurrence_id": source_spo2["visit_occurrence_id"],
                "visit_detail_id": source_spo2["visit_detail_id"],
                "measurement_source_value": measurement_concept["spo2"][1],
                "measurement_source_value_name": measurement_concept["spo2"][1],
                "measurement_source_concept_id": measurement_concept["spo2"][0],
                "EDI코드": None,
                "unit_source_value": measurement_concept["spo2"][3],
                "value_source_value": source_spo2[self.spo2],
                "vocabulary_id": "SNOMED",
                "visit_source_key": source_spo2["visit_source_key"],
                "처방코드": None,
                "처방명": None,
                "환자구분": source_spo2["visit_source_value"],
                "진료과": None,
                "진료과명": None,
                "처방일": None,
                "진료일시": None,
                "접수일시": None,
                "실시일시": None,
                "판독일시": None,
                "보고일시": None,
                "처방순번": None,
                "정상치(상)": None,
                "정상치(하)": None,
                # "나이": None,
                "결과내역": source_spo2[self.spo2]
                })
            logging.debug(f"""weight값이 있는 원천 데이터 row수:\n 
                        cdm_weight: {len(cdm_weight)}\n
                        cdm_height: {len(cdm_height)}\n
                        cdm_bmi: {len(cdm_bmi)}\n
                        cdm_sbp: {len(cdm_sbp)}\n
                        cdm_dbp: {len(cdm_dbp)}\n
                        cdm_pulse: {len(cdm_pulse)}\n
                        cdm_breth: {len(cdm_breth)}\n
                        cdm_dbtp: {len(cdm_bdtp)}\n
                        cdm_spo2: {len(cdm_spo2)}\n
                        총합: {len(cdm_weight) + len(cdm_height) + len(cdm_bmi) + len(cdm_sbp)
                             + len(cdm_dbp) + len(cdm_pulse) + len(cdm_breth) + len(cdm_bdtp) + len(cdm_spo2)
                             }
                        """)
            
            cdm = pd.concat([cdm_weight, cdm_height, cdm_bmi, cdm_sbp, cdm_dbp, cdm_pulse, cdm_breth, cdm_bdtp, cdm_spo2], axis = 0, ignore_index=True)

            logging.debug(f'CDM 데이터 row수: {len(cdm)}')
            logging.debug(f"요약:\n{cdm.describe(include = 'all').T.to_string()}")
            logging.debug(f"컬럼별 null 개수:\n{cdm.isnull().sum().to_string()}")

            return cdm   

        except Exception as e :
            logging.error(f"{self.table} 테이블 CDM 데이터 변환 중 오류:\n {e}", exc_info = True)


class MergeMeasurementTransformer(DataTransformer):
    def __init__(self, config_path):
        super().__init__(config_path)
        self.table = "merge_measurement"
        self.cdm_config = self.config[self.table]

        self.source_data1 = self.cdm_config["data"]["source_data1"]
        self.source_data2 = self.cdm_config["data"]["source_data2"]
        self.source_data3 = self.cdm_config["data"]["source_data3"]
        self.source_data4 = self.cdm_config["data"]["source_data4"]
        self.output_filename = self.cdm_config["data"]["output_filename"]

    def transform(self):
        """
        소스 데이터를 읽어들여 CDM 형식으로 변환하고 결과를 CSV 파일로 저장하는 메소드입니다.
        """
        try : 
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
            source1 = self.read_csv(self.source_data1, path_type = self.cdm_flag, dtype = self.source_dtype)
            source2 = self.read_csv(self.source_data2, path_type = self.cdm_flag, dtype = self.source_dtype)
            source3 = self.read_csv(self.source_data3, path_type = self.cdm_flag, dtype = self.source_dtype)
            source4 = self.read_csv(self.source_data4, path_type = self.cdm_flag, dtype = self.source_dtype)
            
            logging.debug(f"""원천1 데이터 row수 : {len(source1)}, \n
                          원천2 데이터 row수 :{len(source2)}, \n
                          원천3 데이터 row수 :{len(source3)},\n
                          원천1, 원천2 row수 합: {len(source1) + len(source2) + len(source3)}""")

            # axis = 0을 통해 행으로 데이터 합치기, ignore_index = True를 통해 dataframe index재설정
            cdm = pd.concat([source1, source2, source3, source4], axis = 0, ignore_index=True)

            cdm["measurement_id"] = cdm.index + 1

            logging.debug(f"CDM 데이터 row수 : {len(cdm)}")

            return cdm

        except Exception as e :
            logging.error(f"{self.table} 테이블 소스 데이터 처리 중 오류: {e}", exc_info = True)


class ProcedureEDITransformer(DataTransformer):
    def __init__(self, config_path):
        super().__init__(config_path)
        self.table = "procedure_edi"
        self.cdm_config = self.config[self.table]

        # 컬럼 변수 재정의      
        self.order_data = self.cdm_config["data"]["order_data"]
        self.edi_data = self.cdm_config["data"]["edi_data"]
        self.concept_data = self.cdm_config["data"]["concept_data"]
        self.output_filename = self.cdm_config["data"]["output_filename"]

        self.ordercode = self.cdm_config["columns"]["ordercode"]
        self.sugacode = self.cdm_config["columns"]["sugacode"]
        self.edicode = self.cdm_config["columns"]["edicode"]
        self.fromdd = self.cdm_config["columns"]["fromdd"]
        self.todd = self.cdm_config["columns"]["todd"]
        self.ordnm = self.cdm_config["columns"]["ordnm"]
            
        
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
            edi_data = self.read_csv(self.edi_data, path_type = self.source_flag, dtype = self.source_dtype)
            concept_data = self.read_csv(self.concept_data, path_type = self.source_flag, dtype = self.source_dtype, encoding=self.cdm_encoding)
            logging.debug(f'원천 데이터 row수: order: {len(order_data)}, edi: {len(edi_data)}')
            
            edi_data = edi_data[[self.sugacode, self.hospital, self.edicode, self.fromdd, self.todd, "ORDNM"]]
            order_data = order_data[[self.ordercode, self.hospital, self.fromdd, self.todd, "PRCPNM", "PRCPHNGNM"]]
            # 처방코드 마스터와 수가코드 매핑
            source = pd.merge(order_data, edi_data, left_on=[self.ordercode, self.hospital], right_on=[self.sugacode, self.hospital], how="left")
            logging.debug(f'처방코드, 수가코드와 결합 후 데이터 row수: {len(source)}')
            source = source[(source["FROMDD_x"] >= source["FROMDD_y"]) & (source["FROMDD_x"] <= source["TODD_y"])]
            logging.debug(f'조건 적용 후 데이터 row수: {len(source)}')

            # fromdate, todate 설정
            source[self.fromdate] = source["FROMDD_x"].where(source["FROMDD_x"].notna(), source["FROMDD_y"])
            source[self.todate] = source["TODD_x"].where(source["TODD_x"].notna(), source["TODD_y"])

            concept_data = concept_data.sort_values(by = ["vocabulary_id"], ascending=[False])
            concept_data['Sequence'] = concept_data.groupby(["concept_code"]).cumcount() + 1
            concept_data = concept_data[concept_data["Sequence"] == 1]

            # concept_id 매핑
            source = pd.merge(source, concept_data, left_on=self.edicode, right_on="concept_code", how="left")
            logging.debug(f'concept merge후 데이터 row수: {len(source)}')

            # local_edi = source[[self.ordercode, self.fromdate, self.todate, self.edicode, self.hospital,
            #                      "concept_id", "concept_name", "domain_id", "vocabulary_id", "concept_class_id", 
            #                      "standard_concept", "concept_code", "valid_start_date", "valid_end_date", "invalid_reason"
            #                      ]]
        
            # 중복제거
            source = source.drop_duplicates()
            logging.debug(f'중복제거 후 데이터 row수: {len(source)}')


            logging.debug(f'local_edi row수: {len(source)}')
            logging.debug(f"요약:\n{source.describe(include = 'all').T.to_string()}")
            logging.debug(f"컬럼별 null 개수:\n{source.isnull().sum().to_string()}")

            return source

        except Exception as e :
            logging.error(f"{self.table} 테이블 소스 데이터 처리 중 오류:\n {e}", exc_info = True)


class ProcedurePACSTransformer(DataTransformer):
    def __init__(self, config_path):
        super().__init__(config_path)
        self.table = "procedure_pacs"
        self.cdm_config = self.config[self.table]

        self.source_data1 = self.cdm_config["data"]["source_data1"]
        self.source_data2 = self.cdm_config["data"]["source_data2"]
        self.source_data3 = self.cdm_config["data"]["source_data3"]
        self.output_filename = self.cdm_config["data"]["output_filename"]
        self.meddept = self.cdm_config["columns"]["meddept"]
        self.provider = self.cdm_config["columns"]["provider"]
        self.orddate = self.cdm_config["columns"]["orddate"]
        self.procedure_source_value = self.cdm_config["columns"]["procedure_source_value"]
        self.orddd = self.cdm_config["columns"]["orddd"]
        self.readtext = self.cdm_config["columns"]["readtext"]
        self.conclusion = self.cdm_config["columns"]["conclusion"]

        
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
            source1 = self.read_csv(self.source_data1, path_type = self.source_flag, dtype = self.source_dtype)
            source2 = self.read_csv(self.source_data2, path_type = self.source_flag, dtype = self.source_dtype)
            source3 = self.read_csv(self.source_data3, path_type = self.source_flag, dtype = self.source_dtype)
            procedure_edi = self.read_csv(self.procedure_edi_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            person_data = self.read_csv(self.person_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            provider_data = self.read_csv(self.provider_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            care_site_data = self.read_csv(self.care_site_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_data = self.read_csv(self.visit_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_detail = self.read_csv(self.visit_detail, path_type = self.cdm_flag, dtype = self.source_dtype)
            concept_etc = self.read_csv(self.concept_etc, path_type = self.source_flag, dtype = self.source_dtype, encoding=self.cdm_encoding)
            logging.debug(f"원천 데이터 row수: 검사처방: {len(source1)}, 처방상세: {len(source2)}, 영상검사결과: {len(source3)}")

            # 원천에서 조건걸기
            source1 = source1[[self.hospital, self.orddate, self.person_source_value, "PRCPHISTCD", "ORDDD", 
                               "CRETNO", "PRCPCLSCD", "PRCPNO", "PRCPHISTNO", "LASTUPDTDT", 
                               "ORDDRID", "PRCPNM", "PRCPCD", self.meddept, self.frstrgstdt]]
            source1[self.orddate] = pd.to_datetime(source1[self.orddate])
            source1[self.frstrgstdt] = pd.to_datetime(source1[self.frstrgstdt])
            # source1["ORDDD"] = pd.to_datetime(source1["ORDDD"])
            source1 = source1[(source1[self.orddate] <= self.data_range)]

            source2 = source2[[self.hospital, self.orddate, "PRCPNO", "PRCPHISTNO", "EXECPRCPUNIQNO", "EXECDD", "EXECTM"]]
            source2["HISORDERID"] = source2["PRCPDD"] + source2["EXECPRCPUNIQNO"]
            source2[self.orddate] = pd.to_datetime(source2[self.orddate])
            source2 = source2[(source2[self.orddate] <= self.data_range)]

            source3 = source3[["PATID", "HISORDERID", "QUEUEID", "CONFDATE", "CONFTIME", self.conclusion, self.readtext]]
            logging.debug(f"조건적용 후 원천 데이터 row수: 검사처방: {len(source1)}, 처방상세: {len(source2)}, 영상검사결과: {len(source3)}")

            source = pd.merge(source1, source2, left_on=[self.hospital, self.orddate, "PRCPNO", "PRCPHISTNO"], right_on=[self.hospital, self.orddate, "PRCPNO", "PRCPHISTNO"], how="inner", suffixes=("", "_2"))
            logging.debug(f"검사처방, 처방상세 결합 후 데이터 수: {len(source)}")
            del source1
            del source2
            
            # source3 = source3.groupby('HISORDERID').agg({'PATID': 'first', 'QUEUEID': 'max', "CONFDATE": "first", "CONFTIME": "first"}).reset_index()
            # 각 컬럼별로 집계
            grouped_patid = source3.groupby('HISORDERID')['PATID'].agg('first').reset_index()
            grouped_queueid = source3.groupby('HISORDERID')['QUEUEID'].agg('max').reset_index()
            grouped_confdate = source3.groupby('HISORDERID')['CONFDATE'].agg('first').reset_index()
            grouped_conftime = source3.groupby('HISORDERID')['CONFTIME'].agg('first').reset_index()
            grouped_conclusion = source3.groupby('HISORDERID')['CONCLUSION'].agg('first').reset_index()
            grouped_readtext = source3.groupby('HISORDERID')['READTEXT'].agg('first').reset_index()

            # 결과 병합
            source3 = (grouped_patid.merge(grouped_queueid, on='HISORDERID', how='outer')
                                    .merge(grouped_confdate, on='HISORDERID', how='outer')
                                    .merge(grouped_conftime, on='HISORDERID', how='outer')
                                    .merge(grouped_conclusion, on='HISORDERID', how='outer')
                                    .merge(grouped_readtext, on='HISORDERID', how='outer'))
            source = pd.merge(source, source3, left_on=["PID", "HISORDERID"], right_on=["PATID", "HISORDERID"], how="inner", suffixes=("", "_3"))
            del source3
            logging.debug(f"검사처방, 처방상세, 영상검사결과 결합 후 데이터 수: {len(source)}")
            
            source["진료일시"] = source[self.orddd]

            source[self.orddd] = pd.to_datetime(source[self.orddd])
            source["visit_source_key"] = source[self.person_source_value] + ';' + source[self.orddd].dt.strftime("%Y%m%d") + ';' + source[self.visit_no] + ';' + source[self.hospital]
            source["procedure_datetime"] = source["CONFDATE"] + source["CONFTIME"]
            source["procedure_datetime"] = pd.to_datetime(source["procedure_datetime"])
            source[self.readtext] = source[self.readtext].astype(str)
            source[self.conclusion] = source[self.conclusion].astype(str)

            # person table과 병합
            source = pd.merge(source, person_data, left_on=self.person_source_value, right_on="person_source_value", how="inner")
            source = source.drop(columns = ["care_site_id", "provider_id"])
            logging.debug(f'person 테이블과 결합 후 데이터 row수, {len(source)}')
            
            procedure_edi = procedure_edi[[self.procedure_source_value, self.fromdate, self.todate, self.edicode, "concept_id", self.hospital, "ORDNM"]]
            procedure_edi[self.fromdate] = pd.to_datetime(procedure_edi[self.fromdate], errors="coerce")
            # procedure_edi[self.fromdate].fillna(pd.Timestamp('1900-01-01'), inplace = True)
            procedure_edi[self.todate] = pd.to_datetime(procedure_edi[self.todate], errors="coerce")
            # procedure_edi[self.todate].fillna(pd.Timestamp('2099-12-31'), inplace = True)

            # LOCAL코드와 EDI코드 매핑 테이블과 병합
            source = pd.merge(source, procedure_edi, left_on=[self.procedure_source_value, self.hospital], right_on=[self.procedure_source_value, self.hospital], how="left")
            logging.debug(f'local_edi 테이블과 결합 후 데이터 row수: {len(source)}')
            source[self.fromdate] =source[self.fromdate].fillna(pd.Timestamp('1900-01-01'))
            source[self.todate] = source[self.todate].fillna(pd.Timestamp('2099-12-31'))
            source = source[(source[self.orddate] >= source[self.fromdate]) & (source[self.orddate] <= source[self.todate])]
            logging.debug(f"local_edi 사용기간별 필터 적용 후 데이터 row수: {len(source)}")

            # care_site table과 병합
            source = pd.merge(source, care_site_data, left_on=self.meddept, right_on="care_site_source_value", how="left")
            logging.debug(f'care_site 테이블과 결합 후 데이터 row수, {len(source)}')

            source[self.care_site_fromdate] = pd.to_datetime(source[self.care_site_fromdate], errors = "coerce")
            source[self.care_site_todate] = pd.to_datetime(source[self.care_site_todate], errors = "coerce")
            source[self.care_site_fromdate] = source[self.care_site_fromdate].fillna("19000101")
            source[self.care_site_todate] = source[self.care_site_todate].fillna("20991231")
            source = source[(source[self.frstrgstdt] >= source[self.care_site_fromdate]) & (source[self.frstrgstdt] <= source[self.care_site_todate])]
            logging.debug(f"care_site 사용 기간 조건 설정 후 원천 데이터 row수: {len(source)}")

            # provider table과 병합
            source = pd.merge(source, provider_data, left_on=self.provider, right_on="provider_source_value", how="left", suffixes=('', '_y'))
            logging.debug(f'provider 테이블과 결합 후 데이터 row수, {len(source)}')

            # visit_start_datetime 형태 변경
            visit_data["visit_start_date"] = pd.to_datetime(visit_data["visit_start_date"])

            # visit_occurrence table과 병합
            source = pd.merge(source, visit_data, left_on=["visit_source_key" ], right_on=["visit_source_key" ], how="left", suffixes=('', '_y'))
            logging.debug(f'visit_occurrence 테이블과 결합 후 데이터 row수, {len(source)}')

            ### concept_etc테이블과 병합 ###
            concept_etc["concept_id"] = concept_etc["concept_id"].astype(int)            

            # type_concept_id 만들고 type_concept_id_name 기반 만들기
            source["procedure_type_concept_id"] = 38000275
            source = pd.merge(source, concept_etc, left_on = "procedure_type_concept_id", right_on="concept_id", how="left", suffixes=('', '_procedure_type'))
            logging.debug(f'concept_etc: type_concept_id 테이블과 결합 후 데이터 row수: {len(source)}')

            visit_detail = visit_detail[["visit_detail_id", "visit_detail_start_datetime", "visit_detail_end_datetime", "visit_occurrence_id"]]
            visit_detail["visit_detail_start_datetime"] = pd.to_datetime(visit_detail["visit_detail_start_datetime"])
            visit_detail["visit_detail_end_datetime"] = pd.to_datetime(visit_detail["visit_detail_end_datetime"])
            source = pd.merge(source, visit_detail, left_on=["visit_occurrence_id"], right_on=["visit_occurrence_id"], how="left", suffixes=('', '_y'))
            logging.debug(f"visit_detail 테이블과 결합 후 원천 데이터 row수: {len(source)}")

            source["visit_detail_id"] = source.apply(lambda row: row['visit_detail_id'] if pd.notna(row['visit_detail_start_datetime']) and row['visit_detail_start_datetime'] <= row[self.orddate] <= row['visit_detail_end_datetime'] else pd.NA, axis=1)
            source = source.drop(columns = ["visit_detail_start_datetime", "visit_detail_end_datetime"])
            source = source.drop_duplicates()
            logging.debug(f"visit_detail 테이블과 결합 후 조건 적용 후 원천 데이터 row수: {len(source)}")

            # 값이 없는 경우 0으로 값 입력
            # source.loc[source["care_site_id"].isna(), "care_site_id"] = 0
            source.loc[source["concept_id"].isna(), "concept_id"] = 0

            logging.debug(f'CDM 테이블과 결합 후 데이터 row수, {len(source)}, {source}')

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
                "procedure_occurrence_id": 1,
                "person_id": source["person_id"],
                "환자명": source["환자명"],
                "procedure_concept_id": np.select([source["concept_id"].notna()], [source["concept_id"]], default=self.no_matching_concept[0]),
                "procedure_date": np.select([source["procedure_datetime"].notna()], [source["procedure_datetime"].dt.date], default = source[self.orddate].dt.date),
                "procedure_datetime": np.select([source["procedure_datetime"].notna()], [source["procedure_datetime"]], default = source[self.orddate]),
                "procedure_date_type": np.select([source["procedure_datetime"].notna()], ["판독일시"], default = "처방일"),
                "procedure_type_concept_id": np.select([source["procedure_type_concept_id"].notna()], [source["procedure_type_concept_id"]], default=self.no_matching_concept[0]),
                "procedure_type_concept_id_name": np.select([source["concept_name"].notna()], [source["concept_name"]], default=self.no_matching_concept[1]),
                "modifier_concept_id": self.no_matching_concept[0],
                "quantity": None,
                "provider_id": source["provider_id"],
                "처방의명": source["provider_name"],
                "visit_occurrence_id": source["visit_occurrence_id"],
                "visit_detail_id": source["visit_detail_id"],
                "procedure_source_value": source[self.procedure_source_value],
                "procedure_source_value_name": source["PRCPNM"],
                "procedure_source_concept_id": np.select([source["concept_id"].notna()], [source["concept_id"]], default=self.no_matching_concept[0]),
                "EDI코드": source[self.edicode],
                "modifier_source_value": None ,
                "vocabulary_id": "EDI",
                "visit_source_key": source["visit_source_key"],
                "처방코드": source[self.procedure_source_value],
                "처방명": source["PRCPNM"],
                "환자구분": source["visit_source_value"],
                "진료과": source[self.meddept],
                "진료과명": source["care_site_name"],
                "나이": None,
                "처방일": source[self.orddate],
                "수술일": None,
                "진료일시": source["진료일시"],
                "접수일시": None,
                "실시일시": source["EXECDD"] + source["EXECTM"],
                "판독일시": source["CONFDATE"] + source["CONFTIME"],
                "보고일시": None,
                "결과내역": source[self.readtext],
                "결론 및 진단": source[self.conclusion],
                "결과단위": None
                })
            
            cdm = cdm.drop_duplicates()
            cdm.reset_index(drop=True, inplace=True)

            cdm["procedure_occurrence_id"] = cdm.index + 1

            logging.debug(f'CDM 데이터 row수, {len(cdm)}')
            logging.debug(f"요약:\n{source.describe(include = 'all').T.to_string()}")
            logging.debug(f"컬럼별 null 개수:\n{cdm.isnull().sum().to_string()}")

            return cdm   

        except Exception as e :
            logging.error(f"{self.table} 테이블 CDM 데이터 변환 중 오류:\n {e}", exc_info = True)


class ProcedureBaseOrderTransformer(DataTransformer):
    def __init__(self, config_path):
        super().__init__(config_path)
        self.table = "procedure_baseorder"
        self.cdm_config = self.config[self.table]

        self.source_data = self.cdm_config["data"]["source_data"]
        self.output_filename = self.cdm_config["data"]["output_filename"]
        self.meddept = self.cdm_config["columns"]["meddept"]
        self.provider = self.cdm_config["columns"]["provider"]
        self.procedure_date = self.cdm_config["columns"]["procedure_date"]
        self.procedure_source_value = self.cdm_config["columns"]["procedure_source_value"]
        self.orddd = self.cdm_config["columns"]["orddd"]
        self.ordname = self.cdm_config["columns"]["ordname"]

        
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
            procedure_edi = self.read_csv(self.procedure_edi_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            person_data = self.read_csv(self.person_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            provider_data = self.read_csv(self.provider_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            care_site_data = self.read_csv(self.care_site_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_data = self.read_csv(self.visit_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            concept_etc = self.read_csv(self.concept_etc, path_type = self.source_flag, dtype = self.source_dtype, encoding=self.cdm_encoding)
            logging.debug(f"원천 데이터 row수: {len(source)}")

            # 원천에서 조건걸기
            
            source = source[[self.hospital, self.procedure_date, self.person_source_value, self.meddept,
                             self.orddd,self.ordname, self.procedure_source_value, self.provider, self.visit_no]]
            
            source["처방일"] = source[self.procedure_date]
            source["진료일시"] = source[self.orddd]

            source[self.procedure_date] = pd.to_datetime(source[self.procedure_date])
            source[self.orddd] = pd.to_datetime(source[self.orddd])
            source["visit_source_key"] = source[self.person_source_value] + source[self.meddept] + source[self.orddd].dt.strftime("%Y%m%d") + source[self.visit_no] + source[self.hospital]
            source = source[(source[self.procedure_date] <= self.data_range)]
            logging.debug(f"조건적용 후 원천 데이터 row수:{len(source)}")


            procedure_edi = procedure_edi[[self.procedure_source_value, self.fromdate, self.todate, self.edicode, "concept_id", self.hospital, "ORDNM"]]
            procedure_edi[self.fromdate] = pd.to_datetime(procedure_edi[self.fromdate], errors="coerce")
            procedure_edi[self.fromdate] = procedure_edi[self.fromdate].fillna(pd.Timestamp('1900-01-01'))
            procedure_edi[self.todate] = pd.to_datetime(procedure_edi[self.todate], errors="coerce")
            procedure_edi[self.todate] = procedure_edi[self.todate].fillna(pd.Timestamp('2099-12-31'))

            # LOCAL코드와 EDI코드 매핑 테이블과 병합
            source = pd.merge(source, procedure_edi, left_on=[self.procedure_source_value, self.hospital], right_on=[self.procedure_source_value, self.hospital], how="left")
            logging.debug(f'local_edi 테이블과 결합 후 데이터 row수: {len(source)}')
            source = source[(source[self.procedure_date] >= source[self.fromdate]) & (source[self.procedure_date] <= source[self.todate])]
            logging.debug(f"local_edi 사용기간별 필터 적용 후 데이터 row수: {len(source)}")

            # person table과 병합
            source = pd.merge(source, person_data, left_on=self.person_source_value, right_on="person_source_value", how="inner")
            source = source.drop(columns = ["care_site_id", "provider_id"])
            logging.debug(f'person 테이블과 결합 후 데이터 row수, {len(source)}')

            # care_site table과 병합
            source = pd.merge(source, care_site_data, left_on=self.meddept, right_on="care_site_source_value", how="left")
            logging.debug(f'care_site 테이블과 결합 후 데이터 row수, {len(source)}')

            # provider table과 병합
            source = pd.merge(source, provider_data, left_on=self.provider, right_on="provider_source_value", how="left", suffixes=('', '_y'))
            logging.debug(f'provider 테이블과 결합 후 데이터 row수, {len(source)}')

            # visit_start_datetime 형태 변경
            visit_data["visit_start_date"] = pd.to_datetime(visit_data["visit_start_date"])

            # visit_occurrence table과 병합
            source = pd.merge(source, visit_data, left_on=["visit_source_key" ], right_on=["visit_source_key" ], how="left", suffixes=('', '_y'))
            logging.debug(f'visit_occurrence 테이블과 결합 후 데이터 row수, {len(source)}')

            ### concept_etc테이블과 병합 ###
            concept_etc["concept_id"] = concept_etc["concept_id"].astype(int)            

            # type_concept_id 만들고 type_concept_id_name 기반 만들기
            source["procedure_type_concept_id"] = 38000275
            source = pd.merge(source, concept_etc, left_on = "procedure_type_concept_id", right_on="concept_id", how="left", suffixes=('', '_procedure_type'))
            logging.debug(f'concept_etc: type_concept_id 테이블과 결합 후 데이터 row수: {len(source)}')

            ### concept_etc테이블과 병합 ###
            concept_etc["concept_id"] = concept_etc["concept_id"].astype(int)            

            # type_concept_id 만들고 type_concept_id_name 기반 만들기
            source["procedure_type_concept_id"] = 38000275
            source = pd.merge(source, concept_etc, left_on = "procedure_type_concept_id", right_on="concept_id", how="left", suffixes=('', '_procedure_type'))
            logging.debug(f'concept_etc: type_concept_id 테이블과 결합 후 데이터 row수: {len(source)}')

            # 값이 없는 경우 0으로 값 입력
            source.loc[source["care_site_id"].isna(), "care_site_id"] = 0
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
                "환자명": source["환자명"],
                "procedure_concept_id": np.select([source["concept_id"].notna()], [source["concept_id"]], default=self.no_matching_concept[0]),
                "procedure_date": source[self.procedure_date].dt.date,
                "procedure_datetime": source[self.procedure_date],
                "procedure_date_type": "처방일",
                "procedure_type_concept_id": 38000275,
                "procedure_type_concept_id_name": source["concept_name"],
                "modifier_concept_id": self.no_matching_concept[0],
                "quantity": None,
                "provider_id": source["provider_id"],
                "처방의명": source["provider_name"],
                "visit_occurrence_id": source["visit_occurrence_id"],
                "visit_detail_id": None,
                "procedure_source_value": source[self.procedure_source_value],
                "procedure_source_value_name": source[self.ordname],
                "procedure_source_concept_id": source[self.edicode],
                "modifier_source_value": None ,
                "vocabulary_id": "EDI",
                "visit_source_key": source["visit_source_key"],
                "처방코드": source[self.procedure_source_value],
                "처방명": source[self.ordname],
                "환자구분": source["visit_source_value"],
                "진료과": source[self.meddept],
                "진료과명": source["care_site_name"],
                "나이": None,
                "처방일": source["처방일"],
                "수술일": None,
                "진료일시": source["진료일시"],
                "접수일시": None,
                "실시일시": None,
                "판독일시": None,
                "보고일시": None,
                "결과내역": None,
                "결론 및 진단": None,
                "결과단위": None
                })

            logging.debug(f'CDM 데이터 row수, {len(cdm)}')
            logging.debug(f"요약:\n{source.describe(include = 'all').T.to_string()}")
            logging.debug(f"컬럼별 null 개수:\n{cdm.isnull().sum().to_string()}")

            return cdm   

        except Exception as e :
            logging.error(f"{self.table} 테이블 CDM 데이터 변환 중 오류:\n {e}", exc_info = True)


class ProcedureBldOrderTransformer(DataTransformer):
    def __init__(self, config_path):
        super().__init__(config_path)
        self.table = "procedure_bldorder"
        self.cdm_config = self.config[self.table]

        self.source_data = self.cdm_config["data"]["source_data"]
        self.output_filename = self.cdm_config["data"]["output_filename"]
        self.meddept = self.cdm_config["columns"]["meddept"]
        self.provider = self.cdm_config["columns"]["provider"]
        self.procedure_date = self.cdm_config["columns"]["procedure_date"]
        self.procedure_source_value = self.cdm_config["columns"]["procedure_source_value"]
        self.orddd = self.cdm_config["columns"]["orddd"]
        self.ordname = self.cdm_config["columns"]["ordname"]

        
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
            procedure_edi = self.read_csv(self.procedure_edi_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            person_data = self.read_csv(self.person_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            provider_data = self.read_csv(self.provider_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            care_site_data = self.read_csv(self.care_site_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_data = self.read_csv(self.visit_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            concept_etc = self.read_csv(self.concept_etc, path_type = self.source_flag, dtype = self.source_dtype, encoding=self.cdm_encoding)
            logging.debug(f"원천 데이터 row수: {len(source)}")

            # 원천에서 조건걸기
            source = source[[self.hospital, self.procedure_date, self.person_source_value, self.meddept,
                             self.orddd, self.ordname, self.procedure_source_value, self.provider, self.visit_no]]
            source["처방일"] = source[self.procedure_date]
            source["진료일시"] = source[self.orddd]

            source[self.procedure_date] = pd.to_datetime(source[self.procedure_date])
            source[self.orddd] = pd.to_datetime(source[self.orddd])
            source["visit_source_key"] = source[self.person_source_value] + source[self.meddept] + source[self.orddd].dt.strftime("%Y%m%d") + source[self.visit_no] + source[self.hospital]
            source = source[(source[self.procedure_date] <= self.data_range)]
            logging.debug(f"조건적용 후 원천 데이터 row수:{len(source)}")


            procedure_edi = procedure_edi[[self.procedure_source_value, self.fromdate, self.todate, self.edicode, "concept_id", self.hospital, "ORDNM"]]
            procedure_edi[self.fromdate] = pd.to_datetime(procedure_edi[self.fromdate], errors="coerce")
            procedure_edi[self.fromdate] = procedure_edi[self.fromdate].fillna(pd.Timestamp('1900-01-01'))
            procedure_edi[self.todate] = pd.to_datetime(procedure_edi[self.todate], errors="coerce")
            procedure_edi[self.todate] = procedure_edi[self.todate].fillna(pd.Timestamp('2099-12-31'))

            # LOCAL코드와 EDI코드 매핑 테이블과 병합
            source = pd.merge(source, procedure_edi, left_on=[self.procedure_source_value, self.hospital], right_on=[self.procedure_source_value, self.hospital], how="left")
            logging.debug(f'local_edi 테이블과 결합 후 데이터 row수: {len(source)}')
            source = source[(source[self.procedure_date] >= source[self.fromdate]) & (source[self.procedure_date] <= source[self.todate])]
            logging.debug(f"local_edi 사용기간별 필터 적용 후 데이터 row수: {len(source)}")

            # person table과 병합
            source = pd.merge(source, person_data, left_on=self.person_source_value, right_on="person_source_value", how="inner")
            source = source.drop(columns = ["care_site_id", "provider_id"])
            logging.debug(f'person 테이블과 결합 후 데이터 row수, {len(source)}')

            # care_site table과 병합
            source = pd.merge(source, care_site_data, left_on=self.meddept, right_on="care_site_source_value", how="left")
            logging.debug(f'care_site 테이블과 결합 후 데이터 row수, {len(source)}')

            # provider table과 병합
            source = pd.merge(source, provider_data, left_on=self.provider, right_on="provider_source_value", how="left", suffixes=('', '_y'))
            logging.debug(f'provider 테이블과 결합 후 데이터 row수, {len(source)}')

            # visit_start_datetime 형태 변경
            visit_data["visit_start_date"] = pd.to_datetime(visit_data["visit_start_date"])

            # visit_occurrence table과 병합
            source = pd.merge(source, visit_data, left_on=["visit_source_key" ], right_on=["visit_source_key" ], how="left", suffixes=('', '_y'))
            logging.debug(f'visit_occurrence 테이블과 결합 후 데이터 row수, {len(source)}')

            ### concept_etc테이블과 병합 ###
            concept_etc["concept_id"] = concept_etc["concept_id"].astype(int)            

            # type_concept_id 만들고 type_concept_id_name 기반 만들기
            source["procedure_type_concept_id"] = 38000275
            source = pd.merge(source, concept_etc, left_on = "procedure_type_concept_id", right_on="concept_id", how="left", suffixes=('', '_procedure_type'))
            logging.debug(f'concept_etc: type_concept_id 테이블과 결합 후 데이터 row수: {len(source)}')

            ### concept_etc테이블과 병합 ###
            concept_etc["concept_id"] = concept_etc["concept_id"].astype(int)            

            # type_concept_id 만들고 type_concept_id_name 기반 만들기
            source["procedure_type_concept_id"] = 38000275
            source = pd.merge(source, concept_etc, left_on = "procedure_type_concept_id", right_on="concept_id", how="left", suffixes=('', '_procedure_type'))
            logging.debug(f'concept_etc: type_concept_id 테이블과 결합 후 데이터 row수: {len(source)}')

            # 값이 없는 경우 0으로 값 입력
            source.loc[source["care_site_id"].isna(), "care_site_id"] = 0
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
                "환자명": source["환자명"],
                "procedure_concept_id": np.select([source["concept_id"].notna()], [source["concept_id"]], default=self.no_matching_concept[0]),
                "procedure_date": source[self.procedure_date].dt.date,
                "procedure_datetime": source[self.procedure_date],
                "procedure_date_type": "처방일",
                "procedure_type_concept_id": 38000275,
                "procedure_type_concept_id_name": source["concept_name"],
                "modifier_concept_id": self.no_matching_concept[0],
                "quantity": None,
                "provider_id": source["provider_id"],
                "처방의명": source["provider_name"],
                "visit_occurrence_id": source["visit_occurrence_id"],
                "visit_detail_id": None,
                "procedure_source_value": source[self.procedure_source_value],
                "procedure_source_value_name": source[self.ordname],
                "procedure_source_concept_id": source[self.edicode],
                "modifier_source_value": None ,
                "vocabulary_id": "EDI",
                "visit_source_key": source["visit_source_key"],
                "처방코드": source[self.procedure_source_value],
                "처방명": source[self.ordname],
                "환자구분": source["visit_source_value"],
                "진료과": source[self.meddept],
                "진료과명": source["care_site_name"],
                "나이": None,
                "처방일": source["처방일"],
                "수술일": None,
                "진료일시": source["진료일시"],
                "접수일시": None,
                "실시일시": None,
                "판독일시": None,
                "보고일시": None,
                "결과내역": None,
                "결론 및 진단": None,
                "결과단위": None
                })

            logging.debug(f'CDM 데이터 row수, {len(cdm)}')
            logging.debug(f"요약:\n{source.describe(include = 'all').T.to_string()}")
            logging.debug(f"컬럼별 null 개수:\n{cdm.isnull().sum().to_string()}")

            return cdm   

        except Exception as e :
            logging.error(f"{self.table} 테이블 CDM 데이터 변환 중 오류:\n {e}", exc_info = True)


class MergeProcedureTransformer(DataTransformer):
    def __init__(self, config_path):
        super().__init__(config_path)
        self.table = "merge_procedure"
        self.cdm_config = self.config[self.table]

        self.source_data1 = self.cdm_config["data"]["source_data1"]
        self.source_data2 = self.cdm_config["data"]["source_data2"]
        self.source_data3 = self.cdm_config["data"]["source_data3"]
        self.output_filename = self.cdm_config["data"]["output_filename"]

    def transform(self):
        """
        소스 데이터를 읽어들여 CDM 형식으로 변환하고 결과를 CSV 파일로 저장하는 메소드입니다.
        """
        try : 
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
            source1 = self.read_csv(self.source_data1, path_type = self.cdm_flag, dtype = self.source_dtype)
            if self.source_data2 :
                source2 = self.read_csv(self.source_data2, path_type = self.cdm_flag, dtype = self.source_dtype)
            else:
                source2 = pd.DataFrame()
            if self.source_data3 :
                source3 = self.read_csv(self.source_data3, path_type = self.cdm_flag, dtype = self.source_dtype)
            else:
                source3 = pd.DataFrame()
            
            logging.debug(f"""원천1 데이터 row수 : {len(source1)}, \n
                          원천2 데이터 row수 :{len(source2)}, \n
                          원천3 데이터 row수 :{len(source3)},\n
                          원천1, 원천2 row수 합: {len(source1) + len(source2) + len(source3)}""")

            # axis = 0을 통해 행으로 데이터 합치기, ignore_index = True를 통해 dataframe index재설정
            cdm = pd.concat([source1, source2, source3], axis = 0, ignore_index=True)

            cdm["procedure_occurrence_id"] = cdm.index + 1

            logging.debug(f"CDM 데이터 row수 : {len(cdm)}")

            return cdm

        except Exception as e :
            logging.error(f"{self.table} 테이블 소스 데이터 처리 중 오류: {e}", exc_info = True)

        
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
            logging.debug(f"요약:\n{cdm.describe(include = 'O').T.to_string()}")
            logging.debug(f"요약:\n{cdm.describe().T.to_string()}")
            logging.debug(f"컬럼별 null 개수:\n{cdm.isnull().sum().to_string()}")

            return cdm

        except Exception as e :
            logging.error(f"{self.table} 테이블 소스 데이터 처리 중 오류: {e}", exc_info = True)
