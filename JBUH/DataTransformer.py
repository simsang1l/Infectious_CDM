import pandas as pd
import numpy as np
import yaml
import os, psutil
from datetime import datetime
import logging
import warnings
import inspect

# 현재 프로세스의 PID를 얻습니다.
pid = os.getpid()
# 현재 프로세스 객체를 얻습니다.
ps = psutil.Process(pid)

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
        self.memory_usage = str(ps.memory_info().rss / (1024**3)) + "GB"

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
            logging.debug(f"원천 데이터 row수: {len(source_data)}, {self.memory_usage}")

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

            logging.debug(f"CDM 데이터 row수: {len(cdm)}, {self.memory_usage}")
            logging.debug(f"요약(문자형_data):\n{cdm.describe(include = 'O').T.to_string()}, {self.memory_usage}")
            logging.debug(f"요약:\n{cdm.describe().T.to_string()}, {self.memory_usage}")
            logging.debug(f"컬럼별 null 개수:\n{cdm.isnull().sum().to_string()}, {self.memory_usage}")
            
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
            care_site = self.read_csv(self.care_site_data, path_type = self.cdm_flag, dtype = self.source_dtype, encoding = self.encoding)
            logging.debug(f"원천 데이터 row수: {len(source_data)}, {self.memory_usage}")

            source = pd.merge(source_data,
                            care_site,
                            left_on = self.care_site_source_value,
                            right_on = "care_site_source_value",
                            how = "left")
            
            logging.debug(f"CDM 결합 후 원천 데이터 row수: {len(source_data)}, {self.memory_usage}")
        
            return source

        except Exception as e :
            logging.error(f"{self.table} 테이블 원천 데이터 처리 중 오류:\n {e}", exc_info=True)

    def transform_cdm(self, source_data):
        """
        주어진 소스 데이터를 CDM 형식에 맞게 변환하는 메소드.
        변환된 데이터는 새로운 DataFrame으로 구성됩니다.
        """
        try : 
            specialty_conditions = [
            source_data[self.specialty_source_value].isin(['500', '916', '912']),
            source_data[self.specialty_source_value].isin(['010', '020', '100', '110', '120'
                                    , '121', '122', '130', '133', '140'
                                    , '150', '160', '170', '180', '200'])
            ]
            specialty_concept_id = [32581, 32577]

            #     source[gender_source_value].isin(['M']),
            #     source[gender_source_value].isin(['F'])
            # ]
            # gender_concept_id = [8507, 8532]  

            cdm = pd.DataFrame({
                "provider_id" : source_data.index + 1,
                "provider_name": source_data[self.provider_name],
                "npi": None,
                "dea": None,
                "specialty_concept_id": np.select(specialty_conditions, specialty_concept_id, default = 0),
                "care_site_id": source_data["care_site_id"],
                "year_of_birth": None,
                "gender_concept_id": 0, #np.select(gender_conditions, gender_concept_id, default = 0),
                "provider_source_value": source_data[self.provider_source_value],
                "specialty_source_value": source_data[self.specialty_source_value],
                "specialty_source_concept_id": 0,
                "gender_source_value": self.gender_source_value, #np.select([source[gender_source_value].isna()], None, default = None),
                "gender_source_concept_id": 0, #np.select(gender_conditions, gender_concept_id, default = 0),
                })

            logging.debug(f"CDM 데이터 row수: {len(cdm)}, {self.memory_usage}")
            logging.debug(f"요약(문자형_data):\n{cdm.describe(include = 'O').T.to_string()}, {self.memory_usage}")
            logging.debug(f"요약:\n{cdm.describe().T.to_string()}, {self.memory_usage}")
            logging.debug(f"컬럼별 null 개수:\n{cdm.isnull().sum().to_string()}, {self.memory_usage}")

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
        self.birth_resno1 = self.cdm_config["columns"]["birth_resno1"]
        self.birth_resno2 = self.cdm_config["columns"]["birth_resno2"]

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
            source_data = self.read_csv(self.source_data, path_type = self.source_flag, dtype = self.source_dtype, encoding = self.encoding)
            location_data = self.read_csv(self.location_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            logging.debug(f"원천 데이터 row수: {len(source_data)}")

            source_data[self.location_source_value] = source_data[self.location_source_value].str[:3]
            source_data.loc[source_data[self.birth_resno2].str[:1].isin(['9', '0']), self.birth_resno1] = "18" + source_data[self.birth_resno1].astype(str)
            source_data.loc[source_data[self.birth_resno2].str[:1].isin(['1', '2', '5', '6']), self.birth_resno1] = "19" + source_data[self.birth_resno1].astype(str)
            source_data.loc[source_data[self.birth_resno2].str[:1].isin(['3', '4', '7', '8']), self.birth_resno1] = "20" + source_data[self.birth_resno1].astype(str)
            
            source_data = pd.merge(source_data, location_data, left_on = self.location_source_value, right_on="LOCATION_SOURCE_VALUE", how = "left")
            source_data.loc[source_data["LOCATION_ID"].isna(), "LOCATION_ID"] = 0
            logging.debug(f"location 테이블과 결합 후 원천 데이터1 row수: {len(source_data)}, {self.memory_usage}")

            logging.debug(f"CDM테이블과 결합 후 원천 데이터 row수: source: {len(source_data)}, {self.memory_usage}")

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
            source[self.birth_resno2].str[:1].isin(['0', '1', '2', '3', '4', '9']),
            source[self.birth_resno2].str[:1].isin(['5', '6', '7', '8'])
            ]
            race_concept_id = [38003585, 8552]

            gender_conditions = [
                source[self.gender_source_value].isin(['M']),
                source[self.gender_source_value].isin(['F'])
            ]
            gender_concept_id = [8507, 8532]

            cdm = pd.DataFrame({
                "person_id" : source.index + 1,
                "gender_concept_id": np.select(gender_conditions, gender_concept_id, default = 0),
                "year_of_birth": source[self.birth_resno1].str[:4],
                "month_of_birth": source[self.birth_resno1].str[4:6],
                "day_of_birth": source[self.birth_resno1].str[6:8],
                "birth_datetime": pd.to_datetime(source[self.birth_resno1], format = "%Y%m%d", errors='coerce'),
                "death_datetime": pd.to_datetime(source[self.death_datetime], format = "%Y%m%d", errors='coerce'),
                "race_concept_id": np.select(race_conditions, race_concept_id, default = 0),
                "ethnicity_concept_id": 0,
                "location_id": source["LOCATION_ID"],
                "provider_id": 0,
                "care_site_id": 0, 
                "person_source_value": source[self.person_source_value],
                "gender_source_value": source[self.gender_source_value],
                "gender_source_concept_id": np.select(gender_conditions, gender_concept_id, default = 0),
                "race_source_value": source[self.birth_resno1].str[:1],
                "race_source_concept_id": 0,
                "ethnicity_source_value": None,
                "ethnicity_source_concept_id": 0
                })
            
            cdm["birth_datetime"] = cdm["birth_datetime"].dt.strftime('%Y-%m-%d %H:%M:%S')
            cdm["death_datetime"] = cdm["death_datetime"].dt.strftime('%Y-%m-%d %H:%M:%S')

            logging.debug(f"CDM 데이터 row수: {len(cdm)}, {self.memory_usage}")
            logging.debug(f"요약(문자형_data):\n{cdm.describe(include = 'O').T.to_string()}, {self.memory_usage}")
            logging.debug(f"요약:\n{cdm.describe().T.to_string()}, {self.memory_usage}")
            logging.debug(f"컬럼별 null 개수:\n{cdm.isnull().sum().to_string()}, {self.memory_usage}")

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
                    , "preceding_visit_occurrence_id"]

        # 컬럼 변수 재정의
        self.source_data = self.cdm_config["data"]["source_data"]
        self.source_data2 = self.cdm_config["data"]["source_data2"]
        self.output_filename = self.cdm_config["data"]["output_filename"]

        self.meddept = self.cdm_config["columns"]["meddept"]
        self.meddr = self.cdm_config["columns"]["meddr"]
        self.medtime = self.cdm_config["columns"]["medtime"]
        self.chadr = self.cdm_config["columns"]["chadr"]
        self.admtime = self.cdm_config["columns"]["admtime"]
        self.dschtime = self.cdm_config["columns"]["dschtime"]
        self.admitted_from_source_value = self.cdm_config["columns"]["admitted_from_source_value"]
        self.discharge_to_source_value = self.cdm_config["columns"]["discharge_to_source_value"]
        self.visit_source_value = self.cdm_config["columns"]["visit_source_value"]
        

    def transform(self):
        """
        소스 데이터를 읽어들여 CDM 형식으로 변환하고 결과를 CSV 파일로 저장하는 메소드입니다.
        """
        try : 
            source_data, source_data2 = self.process_source()
            transformed_data = self.transform_cdm(source_data, source_data2)

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
            source = self.read_csv(self.source_data, path_type = self.source_flag , dtype = self.source_dtype, encoding = self.encoding)
            source2 = self.read_csv(self.source_data2, path_type = self.source_flag , dtype = self.source_dtype, encoding = self.encoding)
            person_data = self.read_csv(self.person_data, path_type = self.cdm_flag , dtype = self.source_dtype)
            provider_data = self.read_csv(self.provider_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            care_site_data = self.read_csv(self.care_site_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            logging.debug(f"원천 데이터 row수: source: {len(source)}, source2: {len(source2)}, {self.memory_usage}")

            # 원천 데이터 범위 설정
            # 201903081045같은 데이터가 2019-03-08 10:04:05로 바뀌는 문제 발견 
            source[self.medtime] = source[self.medtime].apply(lambda x : x[:4] + "-" + x[4:6] + "-" + x[6:8] + " " + x[8:10] + ":" + x[10:])
            source[self.medtime] = pd.to_datetime(source[self.medtime], errors = "coerce")
            source = source[source[self.medtime] <= self.data_range]
            logging.debug(f"데이터 범위 조건 적용 후 원천 데이터 row수: {len(source)}, {self.memory_usage}")

            # 불러온 원천 전처리
            source = pd.merge(source, person_data, left_on = self.person_source_value, right_on="person_source_value", how="inner")
            source = source.drop(columns = ["care_site_id", "provider_id"])
            logging.debug(f"person 테이블과 결합 후 원천 데이터1 row수: {len(source)}, {self.memory_usage}")

            source = pd.merge(source, care_site_data, left_on = self.meddept, right_on="care_site_source_value", how="left")
            logging.debug(f"care_site 테이블과 결합 후 원천 데이터1 row수: {len(source)}, {self.memory_usage}")

            source = pd.merge(source, provider_data, left_on = self.meddr, right_on="provider_source_value", how="left", suffixes=('', '_y'))
            source.loc[source["care_site_id"].isna(), "care_site_id"] = 0
            logging.debug(f"provider 테이블과 결합 후 원천 데이터1 row수: {len(source)}, {self.memory_usage}")

            # 원천 데이터2 범위 설정
            source2[self.admtime] = source2[self.admtime].apply(lambda x : x[:4] + "-" + x[4:6] + "-" + x[6:8] + " " + x[8:10] + ":" + x[10:])
            source2[self.admtime] = pd.to_datetime(source2[self.admtime], errors = "coerce")
            source2 = source2[source2[self.admtime] <= self.data_range]
            logging.debug(f"데이터 범위 조건 적용 후 원천 데이터2 row수: {len(source2)}, {self.memory_usage}")

            # 불러온 원천2 전처리    
            source2 = pd.merge(source2, person_data, left_on=self.person_source_value, right_on="person_source_value", how="inner")
            source2 = source2.drop(columns = ["care_site_id", "provider_id"])
            logging.debug(f"person 테이블과 결합 후 원천 데이터2 row수: {len(source2)}, {self.memory_usage}")

            source2 = pd.merge(source2, care_site_data, left_on = self.meddept, right_on="care_site_source_value", how="left")
            logging.debug(f"care_site 테이블과 결합 후 원천 데이터2 row수: {len(source2)}, {self.memory_usage}")

            source2 = pd.merge(source2, provider_data, left_on = self.chadr, right_on="provider_source_value", how="left", suffixes=('', '_y'))
            logging.debug(f"provider 테이블과 결합 후 원천 데이터2 row수: {len(source2)}, {self.memory_usage}")

            logging.debug(f"CDM 테이블과 결합 후 원천 데이터 row수: {len(source2)}, {self.memory_usage}")

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
                "visit_concept_id": 9202,
                "visit_start_date": source[self.medtime].dt.date,
                "visit_start_datetime": source[self.medtime],
                "visit_end_date": source[self.medtime].dt.date,
                "visit_end_datetime": source[self.medtime],
                "visit_type_concept_id": np.select([source[self.meddept] == "CTC"], [44818519], default = 44818518),
                "provider_id": source["provider_id"],
                "care_site_id": source["care_site_id"],
                "visit_source_value": "O",
                "visit_source_concept_id": 9202,
                "admitted_from_concept_id": 0,
                "admitted_from_source_value": None,
                "discharge_to_concept_id": 0,
                "discharge_to_source_value": None
                })

            # cdm_ie 생성
            visit_condition = [
                source2[self.visit_source_value] == "I",
                source2[self.visit_source_value] == "E",
            ]
            visit_concept_id = [9201, 9203]

            admit_condition = [
                source2[self.admitted_from_source_value].isin(["1", "6"]),
                source2[self.admitted_from_source_value].isin(["3"]),
                source2[self.admitted_from_source_value].isin(["7"]),
                source2[self.admitted_from_source_value].isin(["9"])
                ]
            admit_concept_id = [8765, 8892, 8870, 8844]

            discharge_condition = [
                source2[self.discharge_to_source_value].isin(["1"]),
                source2[self.discharge_to_source_value].isin(["2"]),
                source2[self.discharge_to_source_value].isin(["3"]),
                source2[self.discharge_to_source_value].isin(["8"]),
                source2[self.discharge_to_source_value].isin(["9"])
                ]
            discharge_concept_id = [44790567, 4061268, 8536, 44814693, 8844]

            cdm_ie = pd.DataFrame({
                "person_id": source2["person_id"],
                "visit_concept_id": np.select(visit_condition, visit_concept_id, default = 0),
                "visit_start_date": source2[self.admtime].dt.date ,
                "visit_start_datetime": source2[self.admtime],
                "visit_end_date": source2[self.dschtime].apply(lambda x: datetime.strptime(x, '%Y%m%d%H%M%S').strftime('%Y-%m-%d')),
                "visit_end_datetime": source2[self.dschtime].apply(lambda x: datetime.strptime(x, '%Y%m%d%H%M%S')), #pd.to_datetime(source2[dschtime], format="%Y%m%d%H%M%S"),
                "visit_type_concept_id": np.select([source2[self.meddept] == "CTC"], [44818519], default = 44818518),
                "provider_id": source2["provider_id"],
                "care_site_id": source2["care_site_id"],
                "visit_source_value": source2[self.visit_source_value],
                "visit_source_concept_id": np.select(visit_condition, visit_concept_id, default = 0),
                "admitted_from_concept_id": np.select(admit_condition, admit_concept_id, default = 0),
                "admitted_from_source_value": source2[self.admitted_from_source_value],
                "discharge_to_concept_id": np.select(discharge_condition, discharge_concept_id, default = 0),
                "discharge_to_source_value": source2[self.discharge_to_source_value]
                })
            cdm = pd.concat([cdm_o, cdm_ie], axis = 0)
            
            cdm.reset_index(drop=True, inplace = True)
            cdm["visit_occurrence_id"] = cdm.index + 1
            cdm.sort_values(by=["person_id", "visit_start_datetime"], inplace = True)
            cdm["preceding_visit_occurrence_id"] = cdm.groupby("person_id")["visit_occurrence_id"].shift(1)
            cdm["preceding_visit_occurrence_id"] = cdm["preceding_visit_occurrence_id"].apply(lambda x : x if pd.isna(x) else str(int(x)))

            cdm = cdm[self.columns]
            
            logging.debug(f"CDM 데이터 row수: {len(cdm)}, {self.memory_usage}")
            logging.debug(f"요약(문자형_data):\n{cdm.describe(include = 'O').T.to_string()}, {self.memory_usage}")
            logging.debug(f"요약:\n{cdm.describe().T.to_string()}, {self.memory_usage}")
            logging.debug(f"컬럼별 null 개수:\n{cdm.isnull().sum().to_string()}, {self.memory_usage}")

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
                        , "visit_detail_parent_id", "visit_occurrence_id"]

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
            source = self.read_csv(self.source_data, path_type = self.source_flag, dtype = self.source_dtype, encoding = self.encoding)
            person_data = self.read_csv(self.person_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            provider_data = self.read_csv(self.provider_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            care_site_data = self.read_csv(self.care_site_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_data = self.read_csv(self.visit_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            logging.debug(f"원천 데이터 row수: {len(source)}, {self.memory_usage}")

            # 원천에서 조건걸기
            source = source[source["DELYN"]== "N"]
            logging.debug(f"조건 적용 후 원천 데이터 row수: {len(source)}, {self.memory_usage}")

            # person table과 병합
            source = pd.merge(source, person_data, left_on=self.person_source_value, right_on="person_source_value", how="inner")
            source = source.drop(columns = ["care_site_id", "provider_id"])
            logging.debug(f"person 테이블과 결합 후 원천 데이터 row수: {len(source)}, {self.memory_usage}")

            # care_site table과 병합
            source = pd.merge(source, care_site_data, left_on=self.meddept, right_on="care_site_source_value", how="left")
            logging.debug(f"care_site 테이블과 결합 후 원천 데이터 row수: {len(source)}, {self.memory_usage}")

            # provider table과 병합
            source = pd.merge(source, provider_data, left_on=self.provider, right_on="provider_source_value", how="left", suffixes=('', '_y'))
            logging.debug(f"provider 테이블과 결합 후 원천 데이터 row수: {len(source)}, {self.memory_usage}")

            # visit_occurrence테이블에서 I, E에 해당하는 데이터만 추출
            visit_data = visit_data[visit_data["visit_source_value"].isin(["I", "E"])]
            # visit_occurrence table과 병합
            source = pd.merge(source, visit_data, left_on=["person_id", "care_site_id"], right_on=["person_id", "care_site_id"], how="left", suffixes=('', '_y'))
            logging.debug(f"visit_occurrence 테이블과 결합 후 원천 데이터 row수: {len(source)}, {self.memory_usage}")

            # 컬럼을 datetime형태로 변경
            source[self.visit_detail_start_datetime] = pd.to_datetime(source[self.visit_detail_start_datetime], format = "%Y%m%d%H%M%S")
            source["visit_start_datetime"] = pd.to_datetime(source["visit_start_datetime"])
            # pandas가 약 2262년 까지만 지원함.., errors="coerce" 하면 에러발생하는 부분은 NaT()처리
            source["visit_end_datetime"] = pd.to_datetime(source["visit_end_datetime"], errors="coerce")
            # 에러 발생하는 부분을 최대값으로 처리
            # 최대 Timestamp 값
            max_timestamp = pd.Timestamp.max

            # NaT 값을 최대 Timestamp 값으로 대체
            # 원본에 256건의 NaT값이 있지만 감안하고 하자.. 12건 늘어남
            source["visit_end_datetime"] = source["visit_end_datetime"].fillna(max_timestamp)

            source = source[(source[self.visit_detail_start_datetime] >= source["visit_start_datetime"]) & (source[self.visit_detail_start_datetime] <= source["visit_end_datetime"])]
            source.loc[source["care_site_id"].isna(), "care_site_id"] = 0
            source[self.visit_detail_end_datetime] = pd.to_datetime(source[self.visit_detail_end_datetime], errors="coerce")
            logging.debug(f"CDM 테이블과 결합 후 원천 데이터 row수: {len(source)}, {self.memory_usage}")

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
                "visit_detail_end_datetime": pd.to_datetime(source[self.visit_detail_end_datetime], errors="coerce"),
                "visit_detail_type_concept_id": 44818518,
                "provider_id": source["provider_id"],
                "care_site_id": source["care_site_id"],
                "visit_detail_source_value": self.visit_detail_source_value,
                "visit_detail_source_concept_id": 0,
                "admitted_from_concept_id": 0,
                "admitted_from_source_value": source[self.admitted_from_source_value],
                "discharge_to_source_value": source[self.discharge_to_source_value],
                "discharge_to_concept_id": 0,
                "visit_detail_parent_id": 0,
                "visit_occurrence_id": source["visit_occurrence_id"]
                })

            cdm = cdm[cdm["visit_detail_start_datetime"] <= self.data_range]
            # 컬럼 생성
            cdm.reset_index(drop = True, inplace = True)
            cdm["visit_detail_id"] = cdm.index + 1
            cdm.sort_values(by=["person_id", "visit_detail_start_datetime"], inplace = True)
            cdm["preceding_visit_detail_id"] = cdm.groupby("person_id")["visit_detail_id"].shift(1)
            cdm["preceding_visit_detail_id"] = cdm["preceding_visit_detail_id"].apply(lambda x : str(int(x)) if not pd.isna(x) else x)

            cdm = cdm[self.columns]

            logging.debug(f"CDM 데이터 row수: {len(cdm)}, {self.memory_usage}")
            logging.debug(f"요약(문자형_data):\n{cdm.describe(include = 'O').T.to_string()}, {self.memory_usage}")
            logging.debug(f"요약:\n{cdm.describe().T.to_string()}, {self.memory_usage}")
            logging.debug(f"컬럼별 null 개수:\n{cdm.isnull().sum().to_string()}, {self.memory_usage}")

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
        self.meddept = self.cdm_config["columns"]["meddept"]
        self.provider = self.cdm_config["columns"]["provider"]
        self.condition_start_datetime = self.cdm_config["columns"]["condition_start_datetime"]
        self.condition_type = self.cdm_config["columns"]["condition_type"]
        self.condition_source_value = self.cdm_config["columns"]["condition_source_value"]
        self.condition_status_source_value = self.cdm_config["columns"]["condition_status_source_value"]
        self.patfg = self.cdm_config["columns"]["patfg"]
        
    def transform(self):
        """
        소스 데이터를 읽어들여 CDM 형식으로 변환하고 결과를 CSV 파일로 저장하는 메소드입니다.
        """
        try:
            source_data = self.process_source()
            logging.info(f"{self.table} 테이블: {len(source_data)}건, {self.memory_usage}")
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
            person_data = self.read_csv(self.person_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            provider_data = self.read_csv(self.provider_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            care_site_data = self.read_csv(self.care_site_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_data = self.read_csv(self.visit_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_detail = self.read_csv(self.visit_detail, path_type = self.cdm_flag, dtype = self.source_dtype)
            logging.debug(f"원천 데이터 row수: {len(source)}, {self.memory_usage}")

            # 원천에서 조건걸기
            source = source[pd.to_datetime(source[self.condition_start_datetime].str[:8], format="%Y%m%d") <= self.data_range]
            source[self.condition_start_datetime] = source[self.condition_start_datetime].apply(lambda x : x[:4] + "-" + x[4:6] + "-" + x[6:8] + " " + x[8:10] + ":" + x[10:])
            source[self.condition_start_datetime] = pd.to_datetime(source[self.condition_start_datetime], errors = "coerce")
            source = source[source[self.condition_start_datetime].notna()]
            logging.debug(f"조건 적용후 원천 데이터 row수: {len(source)}, {self.memory_usage}")

            # person table과 병합
            source = pd.merge(source, person_data, left_on=self.person_source_value, right_on="person_source_value", how="inner")
            source = source.drop(columns = ["care_site_id", "provider_id"])
            logging.debug(f"person 테이블과 결합 후 원천 데이터 row수: {len(source)}, {self.memory_usage}")

            # care_site table과 병합
            source = pd.merge(source, care_site_data, left_on=self.meddept, right_on="care_site_source_value", how="left")
            logging.debug(f"care_site 테이블과 결합 후 원천 데이터 row수: {len(source)}, {self.memory_usage}")

            # provider table과 병합
            source = pd.merge(source, provider_data, left_on=self.provider, right_on="provider_source_value", how="left", suffixes=('', '_y'))
            logging.debug(f"provider 테이블과 결합 후 원천 데이터 row수: {len(source)}, {self.memory_usage}")

            # visit_start_datetime 형태 변경
            visit_data["visit_start_datetime"] = pd.to_datetime(visit_data["visit_start_datetime"], errors = "coerce")
            # visit_occurrence table과 병합
            source = pd.merge(source, visit_data, left_on=["person_id", "care_site_id", self.patfg, self.condition_start_datetime], right_on=["person_id", "care_site_id", "visit_source_value", "visit_start_datetime"], how="left", suffixes=('', '_y'))
            logging.debug(f"visit_occurrence 테이블과 결합 후 원천 데이터 row수: {len(source)}, {self.memory_usage}")

            # visit_detail table과 병합
            source = pd.merge(source, visit_detail, left_on=["visit_occurrence_id"], right_on=["visit_occurrence_id"], how="left", suffixes=('', '_y'))
            logging.debug(f"visit_detail 테이블과 결합 후 원천 데이터 row수: {len(source)}, {self.memory_usage}")

            # care_site_id가 없는 경우 0으로 값 입력
            source.loc[source["care_site_id"].isna(), "care_site_id"] = 0

            logging.debug(f"CDM테이블과 결합 후 원천 데이터 row수: {len(source)}, {self.memory_usage}")

            return source

        except Exception as e :
            logging.error(f"{self.table} 테이블 소스 데이터 처리 중 오류: {e}", exc_info = True)
    

    def transform_cdm(self, source):
        """
        주어진 소스 데이터를 CDM 형식에 맞게 변환하는 메소드.
        변환된 데이터는 새로운 DataFrame으로 구성됩니다.
        """
        try : 
            end_date_condition = [
            source[self.patfg] == 'O',
            source[self.patfg].isin(["E", "I"])
            ]
            end_date_value = [
                source[self.condition_start_datetime].dt.date,
            source["visit_end_date"]
            ]

            end_datetime_value = [
                source[self.condition_start_datetime],
                source["visit_end_datetime"]
            ]

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
                "condition_end_date": np.select(end_date_condition, end_date_value, default = pd.NaT),
                "condition_end_datetime": np.select(end_date_condition, end_datetime_value, default = pd.NaT),
                "condition_type_concept_id": np.select(type_condition, type_id, default = 0),
                "condition_status_concept_id": np.select(status_condition, status_id, default = 0),
                "stop_reason": None,
                "provider_id": source["provider_id"],
                "visit_occurrence_id": source["visit_occurrence_id"],
                "visit_detail_id": source["visit_detail_id"],
                "condition_source_value": source[self.condition_source_value],
                "condition_source_concept_id": 0,
                "condition_status_source_value": source[self.condition_status_source_value]
                })

            # datetime format 형식 맞춰주기, ns로 표기하는 값이 들어갈 수 있어서 처리함
            cdm["condition_end_date"] = pd.to_datetime(cdm["condition_end_date"],errors = "coerce").dt.date
            cdm["condition_end_datetime"] = pd.to_datetime(cdm["condition_end_datetime"], errors = "coerce")

            logging.debug(f"CDM 데이터 row수: {len(cdm)}, {self.memory_usage}")
            logging.debug(f"요약:\n{cdm.describe(include = 'O').T.to_string()}, {self.memory_usage}")
            logging.debug(f"요약:\n{cdm.describe().T.to_string()}, {self.memory_usage}")
            logging.debug(f"컬럼별 null 개수:\n{cdm.isnull().sum().to_string()}, {self.memory_usage}")

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
        self.edi_data = self.cdm_config["data"]["edi_data"]
        self.concept_data = self.cdm_config["data"]["concept_data"]
        self.output_filename = self.cdm_config["data"]["output_filename"]

        self.ordercode = self.cdm_config["columns"]["ordercode"]
        self.sugacode = self.cdm_config["columns"]["sugacode"]
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
            order_data = self.read_csv(self.order_data, path_type = self.source_flag, dtype = self.source_dtype, encoding = self.encoding)
            edi_data = self.read_csv(self.edi_data, path_type = self.source_flag, dtype = self.source_dtype)
            concept_data = self.read_csv(self.concept_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            logging.debug(f'원천 데이터 row수: order: {len(order_data)}, edi: {len(edi_data)}, {self.memory_usage}')

            # 처방코드 마스터와 수가코드 매핑
            source = pd.merge(order_data, edi_data, left_on=self.ordercode, right_on=self.sugacode, how="left")
            logging.debug(f'처방코드, 수가코드와 결합 후 데이터 row수: {len(source)}, {self.memory_usage}')

            # null값에 fromdate, todate 설정
            source["FROMDATE_x"].fillna("19000101")
            source["TODATE_x"].fillna("20991231")
            source[self.fromdate] = source["FROMDATE_y"].where(source["FROMDATE_y"].notna(), source["FROMDATE_x"])
            source[self.todate] = source["TODATE_y"].where(source["TODATE_y"].notna(), source["TODATE_x"])

            concept_data = concept_data.sort_values(by = ["vocabulary_id"], ascending=[False])
            concept_data['Sequence'] = concept_data.groupby(["concept_code"]).cumcount() + 1
            concept_data = concept_data[concept_data["Sequence"] == 1]

            # concept_id 매핑
            source = pd.merge(source, concept_data, left_on=self.edicode, right_on="concept_code", how="left")
            logging.debug(f'concept merge후 데이터 row수: {len(source)}, {self.memory_usage}')

            # drug의 경우 KCD, EDI 순으로 매핑
            source = source.sort_values(by = [self.ordercode, self.fromdate, "vocabulary_id"], ascending=[True, True, False])
            source['Sequence'] = source.groupby([self.ordercode, self.fromdate]).cumcount() + 1
            source = source[source["Sequence"] == 1]
            logging.debug(f'중복되는 concept_id 제거 후 데이터 row수: {len(source)}, {self.memory_usage}')

            local_edi = source[[self.ordercode, self.fromdate, self.todate, self.edicode,
                                 "ORDNAME_x", "ORDNAME_y",   "concept_id", "concept_name", 
                                 "domain_id", "vocabulary_id", "concept_class_id", "standard_concept", 
                                 "concept_code", "valid_start_date", "valid_end_date", "invalid_reason"]]
            local_edi = local_edi.copy()
            local_edi.loc[:, "ORDNAME"] = local_edi["ORDNAME_x"]
            local_edi.loc[:, "SUGANAME"] = local_edi["ORDNAME_y"].values
        
            logging.debug(f'local_edi row수: {len(local_edi)}, {self.memory_usage}')
            logging.debug(f"요약:\n{local_edi.describe(include = 'O').T.to_string()}, {self.memory_usage}")
            logging.debug(f"요약:\n{local_edi.describe().T.to_string()}, {self.memory_usage}")
            logging.debug(f"컬럼별 null 개수:\n{local_edi.isnull().sum().to_string()}, {self.memory_usage}")

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
        self.dose_unit_source_value = self.cdm_config["columns"]["dose_unit_source_value"]
        self.medtime = self.cdm_config["columns"]["medtime"]
        self.dcyn = self.cdm_config["columns"]["dcyn"]
        self.patfg = self.cdm_config["columns"]["patfg"]
        
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
            provider_data = self.read_csv(self.provider_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            care_site_data = self.read_csv(self.care_site_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_data = self.read_csv(self.visit_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_detail = self.read_csv(self.visit_detail, path_type = self.cdm_flag, dtype = self.source_dtype)

            person_data = person_data[["person_id", "person_source_value"]]
            care_site_data = care_site_data[["care_site_id", "care_site_source_value"]]
            provider_data = provider_data[["provider_id", "provider_source_value"]]
            visit_data = visit_data[["visit_occurrence_id", "visit_start_datetime", "care_site_id", "visit_source_value", "person_id"]]
            visit_detail = visit_detail[["visit_detail_id", "visit_occurrence_id"]]
            logging.info(f"원천 데이터 row수:, {len(source)}, {self.memory_usage}")

            # 원천에서 조건걸기
            source[self.drug_exposure_start_datetime] = pd.to_datetime(source[self.drug_exposure_start_datetime], format="%Y%m%d")
            source = source[(source[self.drug_exposure_start_datetime] <= self.data_range) & ((source[self.dcyn] == "N" )| (source[self.dcyn] == None))]
            source = source[[self.person_source_value, self.drug_source_value, self.drug_exposure_start_datetime,
                             self.meddept, self.provider, self.patfg, self.medtime, self.days_supply,
                             self.qty, self.cnt, self.dose_unit_source_value]]
            source[self.medtime] = source[self.medtime].apply(lambda x : x[:4] + "-" + x[4:6] + "-" + x[6:8] + " " + x[8:10] + ":" + x[10:])
            source[self.medtime] = pd.to_datetime(source[self.medtime], errors = "coerce")
            source = source[source[self.medtime].notna()]
            logging.info(f"조건 적용후 원천 데이터 row수:, {len(source)}, {self.memory_usage}")

            local_edi = local_edi[["ORDCODE", "FROMDATE", "TODATE", "INSEDICODE", "concept_id"]]
            local_edi["FROMDATE"] = pd.to_datetime(local_edi["FROMDATE"] , format="%Y%m%d", errors="coerce")
            local_edi["FROMDATE"].fillna(pd.Timestamp('1900-01-01'), inplace = True)
            local_edi["TODATE"] = pd.to_datetime(local_edi["TODATE"] , format="%Y%m%d", errors="coerce")
            local_edi["TODATE"].fillna(pd.Timestamp('2099-12-31'), inplace = True)

            # LOCAL코드와 EDI코드 매핑 테이블과 병합
            source = pd.merge(source, local_edi, left_on=self.drug_source_value, right_on="ORDCODE", how="inner")
            logging.info(f"local_edi와 병합 후 데이터 row수:, {len(source)}, {self.memory_usage}")
            source = source[(source[self.drug_exposure_start_datetime] >= source["FROMDATE"]) & (source[self.drug_exposure_start_datetime] <= source["TODATE"])]
            logging.info(f"local_edi날짜 조건 적용 후 데이터 row수: {len(source)}, {self.memory_usage}")

            # person table과 병합
            source = pd.merge(source, person_data, left_on=self.person_source_value, right_on="person_source_value", how="inner")
            logging.info(f"person 테이블과 결합 후 데이터 row수: {len(source)}, {self.memory_usage}")

            # care_site table과 병합
            source = pd.merge(source, care_site_data, left_on=self.meddept, right_on="care_site_source_value", how="left")
            logging.info(f"care_site 테이블과 결합 후 데이터 row수: {len(source)}, {self.memory_usage}")

            # provider table과 병합
            source = pd.merge(source, provider_data, left_on=self.provider, right_on="provider_source_value", how="left", suffixes=('', '_y'))
            logging.info(f"provider 테이블과 결합 후 데이터 row수: {len(source)}, {self.memory_usage}")

            # visit_start_datetime 형태 변경
            visit_data["visit_start_datetime"] = pd.to_datetime(visit_data["visit_start_datetime"])
            # visit_occurrence table과 병합
            source = pd.merge(source, visit_data, left_on=["person_id", "care_site_id", self.patfg, self.medtime], right_on=["person_id", "care_site_id", "visit_source_value", "visit_start_datetime"], how="left", suffixes=('', '_y'))
            logging.info(f"visit_occurrence 테이블과 결합 후 데이터 row수: {len(source)}, {self.memory_usage}")

            # visit_detail table과 병합
            source = pd.merge(source, visit_detail, left_on=["visit_occurrence_id"], right_on=["visit_occurrence_id"], how="left", suffixes=('', '_y'))
            logging.info(f"visit_detail 테이블과 결합 후 데이터 row수: {len(source)}, {self.memory_usage}")

            # care_site_id가 없는 경우 0으로 값 입력
            source.loc[source["care_site_id"].isna(), "care_site_id"] = 0
            source.loc[source["concept_id"].isna(), "concept_id"] = 0

            logging.info(f"CDM테이블과 병합 후 데이터 row수: {len(source)}, {self.memory_usage}")

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
            "drug_exposure_end_date": source[self.drug_exposure_start_datetime].dt.date + pd.to_timedelta(source[self.days_supply].astype(int) + 1, unit = "D"),
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
            "provider_id": source["provider_id"],
            "visit_occurrence_id": source["visit_occurrence_id"],
            "visit_detail_id": source["visit_detail_id"],
            "drug_source_value": source[self.drug_source_value],
            "drug_source_concept_id": source["INSEDICODE"],
            "route_source_value": None,
            "dose_unit_source_value": source[self.dose_unit_source_value],
            "vocabulary_id": "EDI"
            })

            logging.info(f"CDM테이블 row수: {len(cdm)}, {self.memory_usage}")
            logging.debug(f"요약:\n{cdm.describe(include = 'O').T.to_string()}, {self.memory_usage}")
            logging.debug(f"요약:\n{cdm.describe().T.to_string()}, {self.memory_usage}")
            logging.debug(f"컬럼별 null 개수:\n{cdm.isnull().sum().to_string()}, {self.memory_usage}")

            return cdm   

        except Exception as e :
            logging.error(f"{self.table} 테이블 CDM 데이터 변환 중 오류: {e}", exc_info = True)

class MeasurementStresultTransformer(DataTransformer):
    def __init__(self, config_path):
        super().__init__(config_path)
        self.table = "measurement_stresult"
        self.cdm_config = self.config[self.table]

        # 컬럼 변수 재정의   
        self.source_data1 = self.cdm_config["data"]["source_data1"]
        self.source_data2 = self.cdm_config["data"]["source_data2"]
        self.output_filename = self.cdm_config["data"]["output_filename"]
        self.meddept = self.cdm_config["columns"]["meddept"]
        self.provider = self.cdm_config["columns"]["provider"]
        self.orddate = self.cdm_config["columns"]["orddate"]
        self.exectime = self.cdm_config["columns"]["exectime"]
        self.measurement_source_value = self.cdm_config["columns"]["measurement_source_value"]
        self.value_source_value = self.cdm_config["columns"]["value_source_value"]
        self.unit_source_value = self.cdm_config["columns"]["unit_source_value"]
        self.range_low = self.cdm_config["columns"]["range_low"]
        self.range_high = self.cdm_config["columns"]["range_high"]
        self.patfg = self.cdm_config["columns"]["patfg"]
        self.medtime = self.cdm_config["columns"]["medtime"]
        self.dcyn = self.cdm_config["columns"]["dcyn"]
        self.ordseqno = self.cdm_config["columns"]["ordseqno"]
        
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
            source1 = self.read_csv(self.source_data1, path_type = self.source_flag, dtype = self.source_dtype, encoding = self.encoding)
            source2 = self.read_csv(self.source_data2, path_type = self.source_flag, dtype = self.source_dtype, encoding = self.encoding)
            local_edi = self.read_csv(self.local_edi_data, path_type = self.cdm_flag, dtype = self.source_dtype, encoding = self.encoding)
            person_data = self.read_csv(self.person_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            provider_data = self.read_csv(self.provider_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            care_site_data = self.read_csv(self.care_site_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_data = self.read_csv(self.visit_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_detail = self.read_csv(self.visit_detail, path_type = self.cdm_flag, dtype = self.source_dtype)
            unit_data = self.read_csv(self.concept_unit, path_type = self.cdm_flag , dtype = self.source_dtype)
            logging.debug(f'원천 데이터 row수: {len(source1)}, {len(source2)}, {self.memory_usage}')

            # 원천에서 조건걸기
            source1 = source1[(source1[self.orddate] <= self.data_range) & ((source1[self.dcyn] == "N" )| (source1[self.dcyn] == None))]
            source1 = source1[[self.person_source_value, self.orddate, self.exectime, self.ordseqno, self.meddept, self.provider, self.patfg, self.medtime]]
            source1[self.orddate] = pd.to_datetime(source1[self.orddate], format="%Y%m%d")
            source1[self.exectime] = pd.to_datetime(source1[self.exectime], format="%Y%m%d%H%M%S", errors = "coerce")
            
            source2 = source2[[self.person_source_value.lower(), self.orddate.lower(), self.ordseqno.lower(), self.value_source_value, self.measurement_source_value, self.unit_source_value, self.range_low, self.range_high]]
            source2[self.orddate.lower()] = pd.to_datetime(source2[self.orddate.lower()], format="%Y%m%d")
            source2 = source2[(source2[self.orddate.lower()] <= self.data_range) & (source2[self.measurement_source_value].str[:1].isin(["L", "P"]))]

            # value_as_number float형태로 저장되게 값 변경
            source2["value_as_number"] = source2[self.value_source_value].str.extract('(-?\d+\.\d+|\d+)')
            source2["value_as_number"] = source2["value_as_number"].astype(float)
            source2[self.range_low] = source2[self.range_low].str.extract('(-?\d+\.\d+|\d+)')
            source2[self.range_low] = source2[self.range_low].astype(float)
            source2[self.range_high] = source2[self.range_high].str.extract('(-?\d+\.\d+|\d+)')
            source2[self.range_high] = source2[self.range_high].astype(float)

            logging.debug(f'조건적용 후 원천 데이터 row수: {len(source1)}, {len(source2)}, {self.memory_usage}')

            source = pd.merge(source2, source1, left_on=[self.person_source_value.lower(), self.orddate.lower(), self.ordseqno.lower()], right_on=[self.person_source_value, self.orddate, self.ordseqno], how="inner")
            # 201903081045같은 데이터가 2019-03-08 10:04:05로 바뀌는 문제 발견 
            source[self.medtime] = source[self.medtime].apply(lambda x : x[:4] + "-" + x[4:6] + "-" + x[6:8] + " " + x[8:10] + ":" + x[10:])
            source[self.medtime] = pd.to_datetime(source[self.medtime], errors = "coerce")
            logging.debug(f'원천 병합 후 데이터 row수: {len(source2)}, {self.memory_usage}')
            del source1
            del source2

            # local_edi 전처리
            local_edi = local_edi[["ORDCODE", "FROMDATE", "TODATE", "INSEDICODE", "concept_id"]]
            local_edi["FROMDATE"] = pd.to_datetime(local_edi["FROMDATE"] , format="%Y%m%d", errors="coerce")
            local_edi["FROMDATE"].fillna(pd.Timestamp('1900-01-01'), inplace = True)
            local_edi["TODATE"] = pd.to_datetime(local_edi["TODATE"] , format="%Y%m%d", errors="coerce")
            local_edi["TODATE"].fillna(pd.Timestamp('2099-12-31'), inplace = True)

            # 데이터 컬럼 줄이기
            person_data = person_data[["person_id", "person_source_value"]]
            care_site_data = care_site_data[["care_site_id", "care_site_source_value"]]
            provider_data = provider_data[["provider_id", "provider_source_value"]]
            visit_data = visit_data[["visit_occurrence_id", "visit_start_datetime", "care_site_id", "visit_source_value", "person_id"]]
            visit_detail = visit_detail[["visit_detail_id", "visit_occurrence_id"]]
            unit_data = unit_data[["concept_id", "concept_code"]]
            
            # LOCAL코드와 EDI코드 매핑 테이블과 병합
            source = pd.merge(source, local_edi, left_on=self.measurement_source_value, right_on="ORDCODE", how="inner")
            del local_edi
            logging.debug(f'EDI코드 테이블과 병합 후 데이터 row수: {len(source)}, {self.memory_usage}')
            source = source[(source[self.orddate] >= source["FROMDATE"]) & (source[self.orddate] <= source["TODATE"])]
            logging.debug(f"EDI코드 사용기간별 필터 적용 후 데이터 row수: {len(source)}, {self.memory_usage}")

            # person table과 병합
            source = pd.merge(source, person_data, left_on=self.person_source_value.lower(), right_on="person_source_value", how="inner")
            del person_data
            logging.debug(f'person 테이블과 결합 후 데이터 row수: {len(source)}, {self.memory_usage}')

            # care_site table과 병합
            source = pd.merge(source, care_site_data, left_on=self.meddept, right_on="care_site_source_value", how="left")
            del care_site_data
            logging.debug(f'care_site 테이블과 결합 후 데이터 row수: {len(source)}, {self.memory_usage}')

            # provider table과 병합
            source = pd.merge(source, provider_data, left_on=self.provider, right_on="provider_source_value", how="left", suffixes=('', '_y'))
            logging.debug(f'provider 테이블과 결합 후 데이터 row수: {len(source)}, {self.memory_usage}')

            # visit_start_datetime 형태 변경
            visit_data["visit_start_datetime"] = pd.to_datetime(visit_data["visit_start_datetime"])

            # visit_occurrence table과 병합
            source = pd.merge(source, visit_data, left_on=["person_id", "care_site_id", self.patfg, self.medtime], right_on=["person_id", "care_site_id", "visit_source_value", "visit_start_datetime"], how="left", suffixes=('', '_y'))
            del visit_data
            logging.debug(f'visit_occurrence 테이블과 결합 후 데이터 row수: {len(source)}, {self.memory_usage}')

            # visit_detail table과 병합
            source = pd.merge(source, visit_detail, left_on=["visit_occurrence_id"], right_on=["visit_occurrence_id"], how="left", suffixes=('', '_y'))
            logging.debug(f'visit_detail 테이블과 결합 후 데이터 row수: {len(source)}, {self.memory_usage}')

            # concept_unit과 병합
            source = pd.merge(source, unit_data, left_on=self.unit_source_value, right_on="concept_code", how="left", suffixes=["", "_unit"])
            del unit_data
            # 값이 없는 경우 0으로 값 입력
            source.loc[source["care_site_id"].isna(), "care_site_id"] = 0
            source.loc[source["concept_id"].isna(), "concept_id"] = 0
            
            logging.debug(f'CDM 테이블과 결합 후 데이터 row수: {len(source)}, {self.memory_usage}')

            return source
        
        except Exception as e :
            logging.error(f"{self.table} 테이블 소스 데이터 처리 중 오류: {e}", exc_info = True)

    def transform_cdm(self, source):
        """
        주어진 소스 데이터를 CDM 형식에 맞게 변환하는 메소드.
        변환된 데이터는 새로운 DataFrame으로 구성됩니다.
        """
        try : 
            measurement_date_condition = [source[self.exectime].notna()]
            measurement_date_value = [source[self.exectime].dt.date]
            measurement_datetime_value = [source[self.exectime]]
            measurement_time_value = [source[self.exectime].dt.time]

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
                "measurement_date": np.select(measurement_date_condition, measurement_date_value, default=source["orddate"].dt.date),
                "measurement_datetime": np.select(measurement_date_condition, measurement_datetime_value, default=source["orddate"]),
                "measurement_time": np.select(measurement_date_condition, measurement_time_value, default=source["orddate"].dt.time),
                "measurement_type_concept_id": 44818702,
                "operator_concept_id": np.select(operator_condition, operator_value, default = 0),
                "value_as_number": source["value_as_number"],
                "value_as_concept_id": np.select(value_concept_condition, value_concept_value, default = 0),
                "unit_concept_id": source["concept_id_unit"],
                "range_low": source[self.range_low],
                "range_high": source[self.range_high],
                "provider_id": source["provider_id"],
                "visit_occurrence_id": source["visit_occurrence_id"],
                "visit_detail_id": source["visit_detail_id"],
                "measurement_source_value": source[self.measurement_source_value],
                "measurement_source_concept_id": source["INSEDICODE"],
                "unit_source_value": source[self.unit_source_value],
                "value_source_value": source[self.value_source_value].str[:50],
                "vocabulary_id": "EDI"
                })

            logging.debug(f'CDM 데이터 row수: {len(cdm)}, {self.memory_usage}')
            logging.debug(f"요약:\n{cdm.describe(include = 'O').T.to_string()}, {self.memory_usage}")
            logging.debug(f"요약:\n{cdm.describe().T.to_string()}, {self.memory_usage}")
            logging.debug(f"컬럼별 null 개수:\n{cdm.isnull().sum().to_string()}, {self.memory_usage}")

            return cdm   

        except Exception as e :
            logging.error(f"{self.table} 테이블 CDM 데이터 변환 중 오류:\n {e}", exc_info = True)


# class MeasurementBMITransformer(DataTransformer):
#     def __init__(self, config_path):
#         super().__init__(config_path)
#         self.table = "measurement_bmi"
#         self.cdm_config = self.config[self.table]

#         self.source_data = self.cdm_config["data"]["source_data"]
#         self.output_filename = self.cdm_config["data"]["output_filename"]
#         self.meddept = self.cdm_config["columns"]["meddept"]
#         self.provider = self.cdm_config["columns"]["provider"]
#         self.admtime = self.cdm_config["columns"]["admtime"]
#         self.height = self.cdm_config["columns"]["height"]
#         self.weight = self.cdm_config["columns"]["weight"]

        
#     def transform(self):
#         """
#         소스 데이터를 읽어들여 CDM 형식으로 변환하고 결과를 CSV 파일로 저장하는 메소드입니다.
#         """
#         try :
#             source_data = self.process_source()
#             transformed_data = self.transform_cdm(source_data)


#             save_path = os.path.join(self.cdm_path, self.output_filename)
#             self.write_csv(transformed_data, save_path)

#             logging.info(f"{self.table} 테이블 변환 완료")
#             logging.info(f"============================")

#         except Exception as e :
#             logging.error(f"{self.table} 테이블 변환 중 오류:\n {e}", exc_info=True)
#             raise

#     def process_source(self):
#         """
#         소스 데이터를 로드하고 전처리 작업을 수행하는 메소드입니다.
#         """
#         try:
#             source = self.read_csv(self.source_data, path_type = self.source_flag, dtype = self.source_dtype, encoding = self.encoding)
#             person_data = self.read_csv(self.person_data, path_type = self.cdm_flag, dtype = self.source_dtype)
#             provider_data = self.read_csv(self.provider_data, path_type = self.cdm_flag, dtype = self.source_dtype)
#             care_site_data = self.read_csv(self.care_site_data, path_type = self.cdm_flag, dtype = self.source_dtype)
#             visit_data = self.read_csv(self.visit_data, path_type = self.cdm_flag, dtype = self.source_dtype)
#             visit_detail = self.read_csv(self.visit_detail, path_type = self.cdm_flag, dtype = self.source_dtype)
#             logging.debug(f'원천 데이터 row수: {len(source)}, {self.memory_usage}')

#             # 원천에서 조건걸기
#             source = source[[self.person_source_value, self.admtime, self.meddept, self.provider, self.height, self.weight]]
#             source[self.admtime] = source[self.admtime].apply(lambda x : x[:4] + "-" + x[4:6] + "-" + x[6:8] + " " + x[8:10] + ":" + x[10:])
#             source[self.admtime] = pd.to_datetime(source[self.admtime], errors = "coerce")
#             source = source[(source[self.admtime] <= self.data_range)]
#             logging.debug(f'조건 적용후 원천 데이터 row수: {len(source)}, {self.memory_usage}')

#             source = source[(source[self.height].notna()) | source[self.weight].notna()]
#             source[self.weight] = source[self.weight].str.extract(r'(-?\d+\.\d+|\d+)').copy()
#             source[self.height] = source[self.height].str.extract(r'(-?\d+\.\d+|\d+)').copy()
#             source['bmi'] = round(source[self.weight].astype(float) / (source[self.height].astype(float)*0.01)**2, 1)

#             # CDM 데이터 컬럼 줄이기
#             person_data = person_data[["person_id", "person_source_value"]]
#             care_site_data = care_site_data[["care_site_id", "care_site_source_value"]]
#             provider_data = provider_data[["provider_id", "provider_source_value"]]
#             visit_data = visit_data[["visit_occurrence_id", "visit_start_datetime", "care_site_id", "visit_source_value", "person_id"]]
#             visit_detail = visit_detail[["visit_detail_id", "visit_occurrence_id"]]

#             # person table과 병합
#             source = pd.merge(source, person_data, left_on=self.person_source_value, right_on="person_source_value", how="inner")
#             logging.debug(f'person 테이블과 결합 후 원천 데이터 row수: {len(source)}, {self.memory_usage}')

#             # care_site table과 병합
#             source = pd.merge(source, care_site_data, left_on=self.meddept, right_on="care_site_source_value", how="left")
#             logging.debug(f'care_site 테이블과 결합 후 원천 데이터 row수: {len(source)}, {self.memory_usage}')

#             # provider table과 병합
#             source = pd.merge(source, provider_data, left_on=self.provider, right_on="provider_source_value", how="left", suffixes=('', '_y'))
#             logging.debug(f'provider 테이블과 결합 후 원천 데이터 row수: {len(source)}, {self.memory_usage}')

#             # visit_start_datetime 형태 변경
#             visit_data["visit_start_datetime"] = pd.to_datetime(visit_data["visit_start_datetime"])

#             # visit_occurrence table과 병합
#             visit_data = visit_data[visit_data["visit_source_value"] == 'I']
#             visit_data["visit_start_datetime"] = pd.to_datetime(visit_data["visit_start_datetime"])
#             source = pd.merge(source, visit_data, left_on=["person_id", "care_site_id", self.admtime], right_on=["person_id", "care_site_id", "visit_start_datetime"], how="left", suffixes=('', '_y'))
#             logging.debug(f'visit_occurrence 테이블과 결합 후 원천 데이터 row수: {len(source)}, {self.memory_usage}')

#             # visit_detail table과 병합
#             source = pd.merge(source, visit_detail, left_on=["visit_occurrence_id"], right_on=["visit_occurrence_id"], how="left", suffixes=('', '_y'))
#             logging.debug(f'visit_detail 테이블과 결합 후 원천 데이터 row수: {len(source)}, {self.memory_usage}')

#             # 값이 없는 경우 0으로 값 입력
#             source.loc[source["care_site_id"].isna(), "care_site_id"] = 0

#             logging.debug(f'CDM 테이블과 결합 후 원천 데이터 row수: {len(source)}, {self.memory_usage}')

#             return source

#         except Exception as e :
#             logging.error(f"{self.table} 테이블 소스 데이터 처리 중 오류: {e}", exc_info = True)

#     def transform_cdm(self, source):
#         """
#         주어진 소스 데이터를 CDM 형식에 맞게 변환하는 메소드.
#         변환된 데이터는 새로운 DataFrame으로 구성됩니다.
#         """
#         try : 
#             source_weight = source[source[self.weight].notna()]
#             source_weight[self.weight] = source_weight[self.weight].str.extract(r'(-?\d+\.\d+|\d+)').copy()
#             source_weight[self.weight] = source_weight[self.weight].astype(float)
#             logging.debug(f'weight값이 있는 원천 데이터 row수: {len(source_weight)}, {self.memory_usage}')

#             source_height = source[source[self.height].notna()]
#             source_height[self.height] = source_height[self.height].str.extract(r'(-?\d+\.\d+|\d+)').copy()
#             source_height[self.weight] = source_height[self.height].astype(float)
#             logging.debug(f'height값이 있는 원천 데이터 row수: {len(source_height)}, {self.memory_usage}')

#             source_bmi = source[source["bmi"].notna()]
#             source_bmi["bmi"] = source_bmi["bmi"].astype(float)
#             logging.debug(f'bmi값이 있는 원천 데이터 row수: {len(source_bmi)}')

#             logging.debug(f'weight, height, bmi값이 있는 원천 데이터 row수 합: {len(source)} + {len(source_height)} + {len(source_bmi)}, {self.memory_usage}')

#             # weight값이 저장된 cdm_bmi생성
#             cdm_weight = pd.DataFrame({
#                 "measurement_id": source_weight.index + 1,
#                 "person_id": source_weight["person_id"],
#                 "measurement_concept_id": 4099154,
#                 "measurement_date": source_weight[self.admtime].dt.date,
#                 "measurement_datetime": source_weight[self.admtime],
#                 "measurement_time": source_weight[self.admtime].dt.time, 
#                 "measurement_type_concept_id": 44818702,
#                 "operator_concept_id": 0,
#                 "value_as_number": source_weight[self.weight],
#                 "value_as_concept_id": 0,
#                 "unit_concept_id": 9529,
#                 "range_low": None,
#                 "range_high": None,
#                 "provider_id": source_weight["provider_id"],
#                 "visit_occurrence_id": source_weight["visit_occurrence_id"],
#                 "visit_detail_id": source_weight["visit_detail_id"],
#                 "measurement_source_value": "weight",
#                 "measurement_source_concept_id": 4099154,
#                 "unit_source_value": "kg",
#                 "value_source_value": source_weight[self.weight],
#                 "vocabulary_id": "EDI"
#                 })

#             # height값이 저장된 cdm_bmi생성
#             cdm_height = pd.DataFrame({
#                 "measurement_id": source_height.index + 1,
#                 "person_id": source_height["person_id"],
#                 "measurement_concept_id": 4177340,
#                 "measurement_date": source_height[self.admtime].dt.date,
#                 "measurement_datetime": source_height[self.admtime],
#                 "measurement_time": source_height[self.admtime].dt.time, 
#                 "measurement_type_concept_id": 44818702,
#                 "operator_concept_id": 0,
#                 "value_as_number": source_height[self.height],
#                 "value_as_concept_id": 0,
#                 "unit_concept_id": 8582,
#                 "range_low": None,
#                 "range_high": None,
#                 "provider_id": source_height["provider_id"],
#                 "visit_occurrence_id": source_height["visit_occurrence_id"],
#                 "visit_detail_id": source_height["visit_detail_id"],
#                 "measurement_source_value": "height",
#                 "measurement_source_concept_id": 4177340,
#                 "unit_source_value": "cm",
#                 "value_source_value": source_height[self.height],
#                 "vocabulary_id": "EDI"
#                 })

#             # bmi값이 저장된 cdm_bmi생성
#             cdm_bmi = pd.DataFrame({
#                 "measurement_id": source_bmi.index + 1,
#                 "person_id": source_bmi["person_id"],
#                 "measurement_concept_id": 40490382,
#                 "measurement_date": source_bmi[self.admtime].dt.date,
#                 "measurement_datetime": source_bmi[self.admtime],
#                 "measurement_time": source_bmi[self.admtime].dt.time, 
#                 "measurement_type_concept_id": 44818702,
#                 "operator_concept_id": 0,
#                 "value_as_number": source_bmi["bmi"],
#                 "value_as_concept_id": 0,
#                 "unit_concept_id": 9531,
#                 "range_low": None,
#                 "range_high": None,
#                 "provider_id": source_bmi["provider_id"],
#                 "visit_occurrence_id": source_bmi["visit_occurrence_id"],
#                 "visit_detail_id": source_bmi["visit_detail_id"],
#                 "measurement_source_value": "BMI",
#                 "measurement_source_concept_id": 40490382,
#                 "unit_source_value": "kilogram per square meter",
#                 "value_source_value": source_bmi["bmi"],
#                 "vocabulary_id": "EDI"
#                 })

#             cdm = pd.concat([cdm_weight, cdm_height, cdm_bmi], axis = 0, ignore_index=True)

#             logging.debug(f'CDM 데이터 row수: {len(cdm)}')
#             logging.debug(f"요약:\n{cdm.describe(include = 'O').T.to_string()}")
#             logging.debug(f"요약:\n{cdm.describe().T.to_string()}")
#             logging.debug(f"컬럼별 null 개수:\n{cdm.isnull().sum().to_string()}")

#             return cdm   

#         except Exception as e :
#             logging.error(f"{self.table} 테이블 CDM 데이터 변환 중 오류:\n {e}", exc_info = True)

class MeasurementVSTransformer(DataTransformer):
    def __init__(self, config_path):
        super().__init__(config_path)
        self.table = "measurement_vs"
        self.cdm_config = self.config[self.table]

        self.source_data = self.cdm_config["data"]["source_data"]
        self.output_filename = self.cdm_config["data"]["output_filename"]
        self.meddept = self.cdm_config["columns"]["meddept"]
        self.measurement_datetime = self.cdm_config["columns"]["measurement_datetime"]
        self.admtime = self.cdm_config["columns"]["admtime"]
        self.patfg = self.cdm_config["columns"]["patfg"]
        self.height = self.cdm_config["columns"]["height"]
        self.weight = self.cdm_config["columns"]["weight"]
        self.bmi = self.cdm_config["columns"]["bmi"]
        self.sbp = self.cdm_config["columns"]["sbp"]
        self.dbp = self.cdm_config["columns"]["dbp"]
        self.pr = self.cdm_config["columns"]["pr"]
        self.rr = self.cdm_config["columns"]["rr"]
        self.bt = self.cdm_config["columns"]["bt"]
        self.spo2 = self.cdm_config["columns"]["spo2"]

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
            person_data = self.read_csv(self.person_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            provider_data = self.read_csv(self.provider_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            care_site_data = self.read_csv(self.care_site_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_data = self.read_csv(self.visit_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_detail = self.read_csv(self.visit_detail, path_type = self.cdm_flag, dtype = self.source_dtype)
            logging.debug(f'원천 데이터 row수: {len(source)}, {self.memory_usage}')

            # 원천에서 조건걸기
            source[self.admtime] = source[self.admtime].apply(lambda x : x[:4] + "-" + x[4:6] + "-" + x[6:8] + " " + x[8:10] + ":" + x[10:])
            source[self.admtime] = pd.to_datetime(source[self.admtime], errors = "coerce")
            source[self.measurement_datetime] = source[self.measurement_datetime].apply(lambda x : x[:4] + "-" + x[4:6] + "-" + x[6:8] + " " + x[8:10] + ":" + x[10:])
            source[self.measurement_datetime] = pd.to_datetime(source[self.measurement_datetime], errors = "coerce")
            source = source[(source[self.admtime] <= self.data_range)]
            logging.debug(f'조건 적용후 원천 데이터 row수: {len(source)}, {self.memory_usage}')

            source[self.weight] = source[self.weight].astype(float)
            source[self.height] = source[self.height].astype(float)
            source[self.bmi] = source[self.bmi].astype(float)
            source[self.sbp] = source[self.sbp].astype(float)
            source[self.dbp] = source[self.dbp].astype(float)
            source[self.pr] = source[self.pr].astype(float)
            source[self.rr] = source[self.rr].astype(float)
            source[self.bt] = source[self.bt].astype(float)
            source[self.spo2] = source[self.spo2].astype(float)
            source['calc_bmi'] = round(source[self.weight].astype(float) / (source[self.height].astype(float)*0.01)**2, 1)

            # CDM 데이터 컬럼 줄이기
            person_data = person_data[["person_id", "person_source_value"]]
            care_site_data = care_site_data[["care_site_id", "care_site_source_value"]]
            provider_data = provider_data[["provider_id", "provider_source_value"]]
            visit_data = visit_data[["visit_occurrence_id", "visit_start_datetime", "care_site_id", "visit_source_value", "person_id"]]
            visit_detail = visit_detail[["visit_detail_id", "visit_occurrence_id"]]

            # person table과 병합
            source = pd.merge(source, person_data, left_on=self.person_source_value.lower(), right_on="person_source_value", how="inner")
            logging.debug(f'person 테이블과 결합 후 원천 데이터 row수: {len(source)}, {self.memory_usage}')

            # care_site table과 병합
            source = pd.merge(source, care_site_data, left_on=self.meddept, right_on="care_site_source_value", how="left")
            logging.debug(f'care_site 테이블과 결합 후 원천 데이터 row수: {len(source)}, {self.memory_usage}')

            # visit_start_datetime 형태 변경
            visit_data["visit_start_datetime"] = pd.to_datetime(visit_data["visit_start_datetime"])

            # visit_occurrence table과 병합
            visit_data["visit_start_datetime"] = pd.to_datetime(visit_data["visit_start_datetime"])
            source = pd.merge(source, visit_data, left_on=["person_id", "care_site_id", self.admtime, self.patfg], right_on=["person_id", "care_site_id", "visit_start_datetime", "visit_source_value"], how="left", suffixes=('', '_y'))
            logging.debug(f'visit_occurrence 테이블과 결합 후 원천 데이터 row수: {len(source)}, {self.memory_usage}')

            # visit_detail table과 병합
            source = pd.merge(source, visit_detail, left_on=["visit_occurrence_id"], right_on=["visit_occurrence_id"], how="left", suffixes=('', '_y'))
            logging.debug(f'visit_detail 테이블과 결합 후 원천 데이터 row수: {len(source)}, {self.memory_usage}')

            # 값이 없는 경우 0으로 값 입력
            source.loc[source["care_site_id"].isna(), "care_site_id"] = 0

            logging.debug(f'CDM 테이블과 결합 후 원천 데이터 row수: {len(source)}, {self.memory_usage}')

            return source

        except Exception as e :
            logging.error(f"{self.table} 테이블 소스 데이터 처리 중 오류: {e}", exc_info = True)

    def transform_cdm(self, source):
        """
        주어진 소스 데이터를 CDM 형식에 맞게 변환하는 메소드.
        변환된 데이터는 새로운 DataFrame으로 구성됩니다.
        """
        try : 
            measurement_concept = {
                self.height: [4177340, "body height", 8582, "cm"],
                self.weight: [4099154, "body weight", 9529, "kg"],
                self.bmi: [40490382, "BMI", 9531, "kilogram per square meter"],
                self.sbp: [4152194, "systolic blood pressure (SBP)", 4118323, "mmHg"],
                self.dbp: [4154790, "diastolic blood pressure (DBP)", 4118323, "mmHg"],
                self.pr: [4224504, "pulse rate (PR)", 4118124, "beats/min"],
                self.rr: [4313591, "respiratory rate(RR)", 8541, "respiratory rate(RR)"],
                self.bt: [4302666, "body temperature", 586323, "degree Celsius"],
                self.spo2: [4020553, "SPO2", 8554, "%"]
            }
            source_weight = source[source[self.weight]>0]
            source_height = source[source[self.height]>0]
            source_bmi = source[source[self.bmi] > 0]

            source_dbp = source[source[self.dbp] > 0]
            source_sbp = source[source[self.sbp] > 0]
            source_pr = source[source[self.pr] > 0]
            source_bt = source[source[self.bt] > 0]
            source_rr = source[source[self.rr] > 0]
            source_spo2 = source[source[self.spo2] > 0]
            logging.debug(f"""값이 0보다 큰 원천 데이터 row수: 
                          weight: {len(source_weight)},
                          height: {len(source_height)},
                          bmi: {len(source_bmi)},
                          sbp: {len(source_sbp)},
                          dbp: {len(source_dbp)},
                          pr: {len(source_pr)},
                          bt: {len(source_bt)},
                          rr: {len(source_rr)},
                          spo2: {len(source_spo2)}
                        """)

            # weight값이 저장된 cdm_bmi생성
            cdm_weight = pd.DataFrame({
                "measurement_id": source_weight.index + 1,
                "person_id": source_weight["person_id"],
                "measurement_concept_id": measurement_concept[self.weight][0],
                "measurement_date": source_weight[self.measurement_datetime].dt.date,
                "measurement_datetime": source_weight[self.measurement_datetime],
                "measurement_time": source_weight[self.measurement_datetime].dt.time, 
                "measurement_type_concept_id": 44818702,
                "operator_concept_id": 0,
                "value_as_number": source_weight[self.weight],
                "value_as_concept_id": 0,
                "unit_concept_id": measurement_concept[self.weight][2],
                "range_low": None,
                "range_high": None,
                "provider_id": None,
                "visit_occurrence_id": source_weight["visit_occurrence_id"],
                "visit_detail_id": source_weight["visit_detail_id"],
                "measurement_source_value": measurement_concept[self.weight][1],
                "measurement_source_concept_id": measurement_concept[self.weight][0],
                "unit_source_value": measurement_concept[self.weight][3] ,
                "value_source_value": source_weight[self.weight],
                "vocabulary_id": "SNOMED"
                })

            # height값이 저장된 cdm_bmi생성
            cdm_height = pd.DataFrame({
                "measurement_id": source_height.index + 1,
                "person_id": source_height["person_id"],
                "measurement_concept_id": measurement_concept[self.height][0],
                "measurement_date": source_height[self.measurement_datetime].dt.date,
                "measurement_datetime": source_height[self.measurement_datetime],
                "measurement_time": source_height[self.measurement_datetime].dt.time, 
                "measurement_type_concept_id": 44818702,
                "operator_concept_id": 0,
                "value_as_number": source_height[self.height],
                "value_as_concept_id": 0,
                "unit_concept_id": measurement_concept[self.height][2],
                "range_low": None,
                "range_high": None,
                "provider_id": None,
                "visit_occurrence_id": source_height["visit_occurrence_id"],
                "visit_detail_id": source_height["visit_detail_id"],
                "measurement_source_value": measurement_concept[self.height][1],
                "measurement_source_concept_id": measurement_concept[self.height][0],
                "unit_source_value": measurement_concept[self.height][3],
                "value_source_value": source_height[self.height],
                "vocabulary_id": "SNOMED"
                })

            # bmi값이 저장된 cdm_bmi생성
            cdm_bmi = pd.DataFrame({
                "measurement_id": source_bmi.index + 1,
                "person_id": source_bmi["person_id"],
                "measurement_concept_id": measurement_concept[self.bmi][0],
                "measurement_date": source_bmi[self.measurement_datetime].dt.date,
                "measurement_datetime": source_bmi[self.measurement_datetime],
                "measurement_time": source_bmi[self.measurement_datetime].dt.time, 
                "measurement_type_concept_id": 44818702,
                "operator_concept_id": 0,
                "value_as_number": source_bmi[self.bmi],
                "value_as_concept_id": 0,
                "unit_concept_id": measurement_concept[self.bmi][2],
                "range_low": None,
                "range_high": None,
                "provider_id": None,
                "visit_occurrence_id": source_bmi["visit_occurrence_id"],
                "visit_detail_id": source_bmi["visit_detail_id"],
                "measurement_source_value": measurement_concept[self.bmi][1],
                "measurement_source_concept_id": measurement_concept[self.bmi][0],
                "unit_source_value": measurement_concept[self.bmi][3],
                "value_source_value": source_bmi[self.bmi],
                "vocabulary_id": "SNOMED"
                })

            cdm_sbp = pd.DataFrame({
                "measurement_id": source_sbp.index + 1,
                "person_id": source_sbp["person_id"],
                "measurement_concept_id": measurement_concept[self.sbp][0],
                "measurement_date": source_sbp[self.measurement_datetime].dt.date,
                "measurement_datetime": source_sbp[self.measurement_datetime],
                "measurement_time": source_sbp[self.measurement_datetime].dt.time, 
                "measurement_type_concept_id": 44818702,
                "operator_concept_id": 0,
                "value_as_number": source_sbp[self.sbp],
                "value_as_concept_id": 0,
                "unit_concept_id": measurement_concept[self.sbp][2],
                "range_low": None,
                "range_high": None,
                "provider_id": None,
                "visit_occurrence_id": source_sbp["visit_occurrence_id"],
                "visit_detail_id": source_sbp["visit_detail_id"],
                "measurement_source_value": measurement_concept[self.sbp][1],
                "measurement_source_concept_id": measurement_concept[self.sbp][0],
                "unit_source_value": measurement_concept[self.sbp][3],
                "value_source_value": source_sbp[self.sbp],
                "vocabulary_id": "SNOMED"
                })

            cdm_dbp = pd.DataFrame({
                "measurement_id": source_dbp.index + 1,
                "person_id": source_dbp["person_id"],
                "measurement_concept_id": measurement_concept[self.dbp][0],
                "measurement_date": source_dbp[self.measurement_datetime].dt.date,
                "measurement_datetime": source_dbp[self.measurement_datetime],
                "measurement_time": source_dbp[self.measurement_datetime].dt.time, 
                "measurement_type_concept_id": 44818702,
                "operator_concept_id": 0,
                "value_as_number": source_dbp[self.dbp],
                "value_as_concept_id": 0,
                "unit_concept_id": measurement_concept[self.dbp][2],
                "range_low": None,
                "range_high": None,
                "provider_id": None,
                "visit_occurrence_id": source_dbp["visit_occurrence_id"],
                "visit_detail_id": source_dbp["visit_detail_id"],
                "measurement_source_value": measurement_concept[self.dbp][1],
                "measurement_source_concept_id": measurement_concept[self.dbp][0],
                "unit_source_value": measurement_concept[self.dbp][3],
                "value_source_value": source_dbp[self.dbp],
                "vocabulary_id": "SNOMED"
                })
            
            cdm_pr = pd.DataFrame({
                "measurement_id": source_pr.index + 1,
                "person_id": source_pr["person_id"],
                "measurement_concept_id": measurement_concept[self.pr][0],
                "measurement_date": source_pr[self.measurement_datetime].dt.date,
                "measurement_datetime": source_pr[self.measurement_datetime],
                "measurement_time": source_pr[self.measurement_datetime].dt.time, 
                "measurement_type_concept_id": 44818702,
                "operator_concept_id": 0,
                "value_as_number": source_pr[self.pr],
                "value_as_concept_id": 0,
                "unit_concept_id": measurement_concept[self.pr][2],
                "range_low": None,
                "range_high": None,
                "provider_id": None,
                "visit_occurrence_id": source_pr["visit_occurrence_id"],
                "visit_detail_id": source_pr["visit_detail_id"],
                "measurement_source_value": measurement_concept[self.pr][1],
                "measurement_source_concept_id": measurement_concept[self.pr][0],
                "unit_source_value": measurement_concept[self.pr][3],
                "value_source_value": source_pr[self.pr],
                "vocabulary_id": "SNOMED"
                })
            
            cdm_bt = pd.DataFrame({
                "measurement_id": source_bt.index + 1,
                "person_id": source_bt["person_id"],
                "measurement_concept_id": measurement_concept[self.bt][0],
                "measurement_date": source_bt[self.measurement_datetime].dt.date,
                "measurement_datetime": source_bt[self.measurement_datetime],
                "measurement_time": source_bt[self.measurement_datetime].dt.time, 
                "measurement_type_concept_id": 44818702,
                "operator_concept_id": 0,
                "value_as_number": source_bt[self.bt],
                "value_as_concept_id": 0,
                "unit_concept_id": measurement_concept[self.bt][2],
                "range_low": None,
                "range_high": None,
                "provider_id": None,
                "visit_occurrence_id": source_bt["visit_occurrence_id"],
                "visit_detail_id": source_bt["visit_detail_id"],
                "measurement_source_value": measurement_concept[self.bt][1],
                "measurement_source_concept_id": measurement_concept[self.bt][0],
                "unit_source_value": measurement_concept[self.bt][3],
                "value_source_value": source_bt[self.bt],
                "vocabulary_id": "SNOMED"
                })
            
            cdm_rr = pd.DataFrame({
                "measurement_id": source_rr.index + 1,
                "person_id": source_rr["person_id"],
                "measurement_concept_id": measurement_concept[self.rr][0],
                "measurement_date": source_rr[self.measurement_datetime].dt.date,
                "measurement_datetime": source_rr[self.measurement_datetime],
                "measurement_time": source_rr[self.measurement_datetime].dt.time, 
                "measurement_type_concept_id": 44818702,
                "operator_concept_id": 0,
                "value_as_number": source_rr[self.rr],
                "value_as_concept_id": 0,
                "unit_concept_id": measurement_concept[self.rr][2],
                "range_low": None,
                "range_high": None,
                "provider_id": None,
                "visit_occurrence_id": source_rr["visit_occurrence_id"],
                "visit_detail_id": source_rr["visit_detail_id"],
                "measurement_source_value": measurement_concept[self.rr][1],
                "measurement_source_concept_id": measurement_concept[self.rr][0],
                "unit_source_value": measurement_concept[self.rr][3],
                "value_source_value": source_rr[self.rr],
                "vocabulary_id": "SNOMED"
                })
            
            cdm_spo2 = pd.DataFrame({
                "measurement_id": source_spo2.index + 1,
                "person_id": source_spo2["person_id"],
                "measurement_concept_id": measurement_concept[self.spo2][0],
                "measurement_date": source_spo2[self.measurement_datetime].dt.date,
                "measurement_datetime": source_spo2[self.measurement_datetime],
                "measurement_time": source_spo2[self.measurement_datetime].dt.time, 
                "measurement_type_concept_id": 44818702,
                "operator_concept_id": 0,
                "value_as_number": source_spo2[self.spo2],
                "value_as_concept_id": 0,
                "unit_concept_id": measurement_concept[self.spo2][2],
                "range_low": None,
                "range_high": None,
                "provider_id": None,
                "visit_occurrence_id": source_spo2["visit_occurrence_id"],
                "visit_detail_id": source_spo2["visit_detail_id"],
                "measurement_source_value": measurement_concept[self.spo2][1],
                "measurement_source_concept_id": measurement_concept[self.spo2][0],
                "unit_source_value": measurement_concept[self.spo2][3],
                "value_source_value": source_spo2[self.spo2],
                "vocabulary_id": "SNOMED"
                })
            
            logging.debug(f"""CDM별 데이터 row수: 
                          weight: {len(cdm_weight)},
                          height: {len(cdm_height)},
                          bmi: {len(cdm_bmi)},
                          sbp: {len(cdm_sbp)},
                          dbp: {len(cdm_dbp)},
                          pr: {len(cdm_pr)},
                          rr: {len(cdm_rr)},
                          bt: {len(cdm_bt)},
                          spo2: {len(cdm_spo2)}
                          , {self.memory_usage}
                        """)

            cdm = pd.concat([cdm_weight, cdm_height, cdm_bmi, cdm_sbp, cdm_dbp, cdm_pr, cdm_rr, cdm_bt, cdm_spo2], axis = 0, ignore_index=True)

            logging.debug(f'CDM 데이터 row수: {len(cdm)}, {self.memory_usage}')
            logging.debug(f"요약:\n{cdm.describe(include = 'O').T.to_string()}, {self.memory_usage}")
            logging.debug(f"요약:\n{cdm.describe().T.to_string()}, {self.memory_usage}")
            logging.debug(f"컬럼별 null 개수:\n{cdm.isnull().sum().to_string()}, {self.memory_usage}")

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
        self.output_filename = self.cdm_config["data"]["output_filename"]

    def transform(self):
        """
        소스 데이터를 읽어들여 CDM 형식으로 변환하고 결과를 CSV 파일로 저장하는 메소드입니다.
        """
        try : 
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
            source1 = self.read_csv(self.source_data1, path_type = self.cdm_flag, dtype = self.source_dtype, encoding = self.encoding)
            source2 = self.read_csv(self.source_data2, path_type = self.cdm_flag, dtype = self.source_dtype, encoding = self.encoding)
            
            logging.debug(f"원천1 데이터 row수 : {len(source1)}, 원천2 데이터 row수 :{len(source2)}, 원천1, 원천2 row수 합: {len(source1) + len(source2)}, {self.memory_usage}" )

            # axis = 0을 통해 행으로 데이터 합치기, ignore_index = True를 통해 dataframe index재설정
            cdm = pd.concat([source1, source2], axis = 0, ignore_index=True)

            cdm["measurement_id"] = cdm.index + 1

            logging.debug(f"CDM 데이터 row수 : {len(cdm)}, {self.memory_usage}")

            return cdm

        except Exception as e :
            logging.error(f"{self.table} 테이블 소스 데이터 처리 중 오류: {e}", exc_info = True)

        
class ProcedureTRTTransformer(DataTransformer):
    def __init__(self, config_path):
        super().__init__(config_path)
        self.table = "procedure_trt"
        self.cdm_config = self.config[self.table]

        self.source_data = self.cdm_config["data"]["source_data"]
        self.output_filename = self.cdm_config["data"]["output_filename"]
        self.meddept = self.cdm_config["columns"]["meddept"]
        self.provider = self.cdm_config["columns"]["provider"]
        self.orddate = self.cdm_config["columns"]["orddate"]
        self.exectime = self.cdm_config["columns"]["exectime"]
        self.opdate = self.cdm_config["columns"]["opdate"]
        self.procedure_source_value = self.cdm_config["columns"]["procedure_source_value"]
        self.patfg = self.cdm_config["columns"]["patfg"]
        self.medtime = self.cdm_config["columns"]["medtime"]
        self.dcyn = self.cdm_config["columns"]["dcyn"]
        self.ordclstyp = self.cdm_config["columns"]["ordclstyp"]
        self.ordseqno = self.cdm_config["columns"]["ordseqno"]

        
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
            provider_data = self.read_csv(self.provider_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            care_site_data = self.read_csv(self.care_site_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_data = self.read_csv(self.visit_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_detail = self.read_csv(self.visit_detail, path_type = self.cdm_flag, dtype = self.source_dtype)
            logging.debug(f"원천 데이터 row수: {len(source)}, {self.memory_usage}")

            # 원천에서 조건걸기
            source[self.orddate] = pd.to_datetime(source[self.orddate], format="%Y%m%d")
            source[self.exectime] = source[self.exectime].astype(str).apply(lambda x : x[:4] + "-" + x[4:6] + "-" + x[6:8] + " " + x[8:10] + ":" + x[10:])
            source[self.exectime] = pd.to_datetime(source[self.exectime], format="%Y%m%d%H%M%S", errors = "coerce")
            source[self.medtime] = source[self.medtime].apply(lambda x : x[:4] + "-" + x[4:6] + "-" + x[6:8] + " " + x[8:10] + ":" + x[10:])
            source[self.medtime] = pd.to_datetime(source[self.medtime], errors = "coerce")
            source = source[(source[self.orddate] <= self.data_range) & ((source[self.dcyn] == "N" )| (source[self.dcyn] == None)) & source[self.ordclstyp] != "D2"]
            source = source[[self.person_source_value, self.orddate, self.exectime, self.ordseqno, self.opdate, self.procedure_source_value, self.meddept, self.provider, self.medtime, self.patfg]]
            logging.debug(f"조건적용 후 원천 데이터 row수: {len(source)}, {self.memory_usage}")

            local_edi = local_edi[["ORDCODE", "FROMDATE", "TODATE", "INSEDICODE", "concept_id"]]
            local_edi["FROMDATE"] = pd.to_datetime(local_edi["FROMDATE"] , format="%Y%m%d", errors="coerce")
            local_edi["FROMDATE"].fillna(pd.Timestamp('1900-01-01'), inplace = True)
            local_edi["TODATE"] = pd.to_datetime(local_edi["TODATE"] , format="%Y%m%d", errors="coerce")
            local_edi["TODATE"].fillna(pd.Timestamp('2099-12-31'), inplace = True)

            # LOCAL코드와 EDI코드 매핑 테이블과 병합
            source = pd.merge(source, local_edi, left_on=self.procedure_source_value, right_on="ORDCODE", how="left")
            logging.debug(f'local_edi 테이블과 결합 후 데이터 row수: {len(source)}, {self.memory_usage}')
            source = source[(source[self.orddate] >= source["FROMDATE"]) & (source[self.orddate] <= source["TODATE"])]
            logging.debug(f"local_edi 사용기간별 필터 적용 후 데이터 row수: {len(source)}, {self.memory_usage}")

            # person table과 병합
            source = pd.merge(source, person_data, left_on=self.person_source_value, right_on="person_source_value", how="inner")
            source = source.drop(columns = ["care_site_id", "provider_id"])
            logging.debug(f'person 테이블과 결합 후 데이터 row수, {len(source)}, {self.memory_usage}')

            # care_site table과 병합
            source = pd.merge(source, care_site_data, left_on=self.meddept, right_on="care_site_source_value", how="left")
            logging.debug(f'care_site 테이블과 결합 후 데이터 row수, {len(source)}, {self.memory_usage}')

            # provider table과 병합
            source = pd.merge(source, provider_data, left_on=self.provider, right_on="provider_source_value", how="left", suffixes=('', '_y'))
            logging.debug(f'provider 테이블과 결합 후 데이터 row수, {len(source)}, {self.memory_usage}')

            # visit_start_datetime 형태 변경
            visit_data["visit_start_datetime"] = pd.to_datetime(visit_data["visit_start_datetime"])

            # visit_occurrence table과 병합
            source = pd.merge(source, visit_data, left_on=["person_id", "care_site_id", self.patfg, self.medtime], right_on=["person_id", "care_site_id", "visit_source_value", "visit_start_datetime"], how="left", suffixes=('', '_y'))
            logging.debug(f'visit_occurrence 테이블과 결합 후 데이터 row수, {len(source)}, {self.memory_usage}')

            # visit_detail table과 병합
            source = pd.merge(source, visit_detail, left_on=["visit_occurrence_id"], right_on=["visit_occurrence_id"], how="left", suffixes=('', '_y'))
            logging.debug(f'visit_detail 테이블과 결합 후 데이터 row수, {len(source)}, {self.memory_usage}')

            # 값이 없는 경우 0으로 값 입력
            source.loc[source["care_site_id"].isna(), "care_site_id"] = 0
            source.loc[source["concept_id"].isna(), "concept_id"] = 0

            logging.debug(f'CDM 테이블과 결합 후 데이터 row수, {len(source)}, {self.memory_usage}')

            return source

        except Exception as e :
            logging.error(f"{self.table} 테이블 소스 데이터 처리 중 오류: {e}", exc_info = True)

    def transform_cdm(self, source):
        """
        주어진 소스 데이터를 CDM 형식에 맞게 변환하는 메소드.
        변환된 데이터는 새로운 DataFrame으로 구성됩니다.
        """
        try : 
            procedure_date_condition = [source[self.opdate].notna()
                                        , source[self.exectime].notna()
                                        ]
            procedure_date_value = [pd.to_datetime(source[self.opdate], format = "%Y%m%d").dt.date
                                    , source[self.exectime].dt.date
                                    ]
            procedure_datetime_value = [
                                    pd.to_datetime(source[self.opdate], format = "%Y%m%d")
                                    , pd.to_datetime(source[self.exectime], format = "%Y%m%d%H%M%S")
                                    ]
                                    
            cdm = pd.DataFrame({
                "procedure_occurrence_id": source.index + 1,
                "person_id": source["person_id"],
                "procedure_concept_id": source["concept_id"],
                "procedure_date": np.select(procedure_date_condition, procedure_date_value, default = source[self.orddate].dt.date),
                "procedure_datetime": np.select(procedure_date_condition, procedure_datetime_value, default = source[self.orddate]),
                "procedure_type_concept_id": 38000275,
                "modifier_concept_id": 0,
                "quantity": None,
                "provider_id": source["provider_id"],
                "visit_occurrence_id": source["visit_occurrence_id"],
                "visit_detail_id": source["visit_detail_id"],
                "procedure_source_value": source[self.procedure_source_value],
                "procedure_source_concept_id": source["INSEDICODE"],
                "modifier_source_value": None ,
                "vocabulary_id": "EDI"
                })

            cdm["procedure_date"] = pd.to_datetime(cdm["procedure_date"])
            cdm["procedure_datetime"] = pd.to_datetime(cdm["procedure_datetime"])

            logging.debug(f'CDM 데이터 row수, {len(cdm)}, {self.memory_usage}')
            logging.debug(f"요약:\n{cdm.describe(include = 'O').T.to_string()}, {self.memory_usage}")
            logging.debug(f"요약:\n{cdm.describe().T.to_string()}, {self.memory_usage}")
            logging.debug(f"컬럼별 null 개수:\n{cdm.isnull().sum().to_string()}, {self.memory_usage}")

            return cdm   

        except Exception as e :
            logging.error(f"{self.table} 테이블 CDM 데이터 변환 중 오류:\n {e}", exc_info = True)
          
class ProcedureStresultTransformer(DataTransformer):
    def __init__(self, config_path):
        super().__init__(config_path)
        self.table = "procedure_stresult"
        self.cdm_config = self.config[self.table]

        self.source_data1 = self.cdm_config["data"]["source_data1"]
        self.source_data2 = self.cdm_config["data"]["source_data2"]
        self.output_filename = self.cdm_config["data"]["output_filename"]
        self.meddept = self.cdm_config["columns"]["meddept"]
        self.provider = self.cdm_config["columns"]["provider"]
        self.orddate = self.cdm_config["columns"]["orddate"]
        self.exectime = self.cdm_config["columns"]["exectime"]
        self.procedure_source_value = self.cdm_config["columns"]["procedure_source_value"]
        self.value_source_value = self.cdm_config["columns"]["value_source_value"]
        self.unit_source_value = self.cdm_config["columns"]["unit_source_value"]
        self.range_low = self.cdm_config["columns"]["range_low"]
        self.range_high = self.cdm_config["columns"]["range_high"]
        self.patfg = self.cdm_config["columns"]["patfg"]
        self.medtime = self.cdm_config["columns"]["medtime"]
        self.dcyn = self.cdm_config["columns"]["dcyn"]
        self.ordseqno = self.cdm_config["columns"]["ordseqno"]

        
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
            source1 = self.read_csv(self.source_data1, path_type = self.source_flag, dtype = self.source_dtype, encoding = self.encoding)
            source2 = self.read_csv(self.source_data2, path_type = self.source_flag, dtype = self.source_dtype, encoding = self.encoding)
            local_edi = self.read_csv(self.local_edi_data, path_type = self.cdm_flag, dtype = self.source_dtype, encoding = self.encoding)
            person_data = self.read_csv(self.person_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            provider_data = self.read_csv(self.provider_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            care_site_data = self.read_csv(self.care_site_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_data = self.read_csv(self.visit_data, path_type = self.cdm_flag, dtype = self.source_dtype)
            visit_detail = self.read_csv(self.visit_detail, path_type = self.cdm_flag, dtype = self.source_dtype)
            unit_data = self.read_csv(self.concept_unit, path_type = self.cdm_flag, dtype = self.source_dtype)
            logging.debug(f'원천 데이터 row수: {len(source1)}, {len(source2)}, {self.memory_usage}')

            # 원천에서 조건걸기
            source1 = source1[[self.person_source_value, self.orddate, self.exectime, self.ordseqno, self.dcyn, self.meddept, self.provider, self.patfg, self.medtime]]
            source1[self.orddate] = pd.to_datetime(source1[self.orddate])
            source1 = source1[(source1[self.orddate] <= self.data_range) & ((source1[self.dcyn] == "N" )| (source1[self.dcyn] == None))]
            source1[self.exectime] = source1[self.exectime].astype(str).apply(lambda x : x[:4] + "-" + x[4:6] + "-" + x[6:8] + " " + x[8:10] + ":" + x[10:])
            source1[self.exectime] = pd.to_datetime(source1[self.exectime], format="%Y%m%d%H%M%S", errors = "coerce")
            
            source2 = source2[[self.person_source_value.lower(), self.orddate.lower(), self.ordseqno.lower(), self.value_source_value, self.procedure_source_value, self.unit_source_value, self.range_low, self.range_high]]
            source2[self.orddate.lower()] = pd.to_datetime(source2[self.orddate.lower()])
            source2 = source2[(source2[self.orddate.lower()] <= self.data_range) & (~source2[self.procedure_source_value].str[:1].isin(["L", "P"]))]
            # value_as_number float형태로 저장되게 값 변경
            source2[self.value_source_value] = source2[self.value_source_value].str.extract('(\d+\.\d+|\d+)')
            source2[self.value_source_value].astype(float)
            logging.debug(f'조건 적용후 원천 데이터 row수: {len(source1)}, {len(source2)}, {self.memory_usage}')

            source = pd.merge(source2, source1, left_on=[self.person_source_value.lower(), self.orddate.lower(), self.ordseqno.lower()], right_on=[self.person_source_value, self.orddate, self.ordseqno], how="inner")
            source[self.medtime] = source[self.medtime].apply(lambda x : x[:4] + "-" + x[4:6] + "-" + x[6:8] + " " + x[8:10] + ":" + x[10:])
            source[self.medtime] = pd.to_datetime(source[self.medtime], errors = "coerce")
            logging.debug(f'원천 병합 후 데이터 row수: {len(source2)}, {self.memory_usage}')

            # local_edi 전처리
            local_edi = local_edi[["ORDCODE", "FROMDATE", "TODATE", "INSEDICODE", "concept_id"]]
            local_edi["FROMDATE"] = pd.to_datetime(local_edi["FROMDATE"] , format="%Y%m%d", errors="coerce")
            local_edi["FROMDATE"].fillna(pd.Timestamp('1900-01-01'), inplace = True)
            local_edi["TODATE"] = pd.to_datetime(local_edi["TODATE"] , format="%Y%m%d", errors="coerce")
            local_edi["TODATE"].fillna(pd.Timestamp('2099-12-31'), inplace = True)

            # LOCAL코드와 EDI코드 매핑 테이블과 병합
            source = pd.merge(source, local_edi, left_on=self.procedure_source_value, right_on="ORDCODE", how="inner")
            logging.debug(f'local_edi 테이블과 결합 후 데이터 row수: {len(source)}, {self.memory_usage}')
            source = source[(source[self.orddate] >= source["FROMDATE"]) & (source[self.orddate] <= source["TODATE"])]
            logging.debug(f"EDI코드 사용기간별 필터 적용 후 데이터 row수: {len(source)}, {self.memory_usage}")

            # 데이터 컬럼 줄이기
            person_data = person_data[["person_id", "person_source_value"]]
            care_site_data = care_site_data[["care_site_id", "care_site_source_value"]]
            provider_data = provider_data[["provider_id", "provider_source_value"]]
            visit_data = visit_data[["visit_occurrence_id", "visit_start_datetime", "care_site_id", "visit_source_value", "person_id"]]
            visit_detail = visit_detail[["visit_detail_id", "visit_occurrence_id"]]
            unit_data = unit_data[["concept_id", "concept_code"]]

            # person table과 병합
            source = pd.merge(source, person_data, left_on=self.person_source_value.lower(), right_on="person_source_value", how="inner")
            logging.debug(f'person 테이블과 결합 후 데이터 row수: {len(source)}, {self.memory_usage}')

            # care_site table과 병합
            source = pd.merge(source, care_site_data, left_on=self.meddept, right_on="care_site_source_value", how="left")
            logging.debug(f'care_site 테이블과 결합 후 데이터 row수: {len(source)}, {self.memory_usage}')

            # provider table과 병합
            source = pd.merge(source, provider_data, left_on=self.provider, right_on="provider_source_value", how="left", suffixes=('', '_y'))
            logging.debug(f'provider 테이블과 결합 후 데이터 row수: {len(source)}, {self.memory_usage}')

            # visit_start_datetime 형태 변경
            visit_data["visit_start_datetime"] = pd.to_datetime(visit_data["visit_start_datetime"])

            # visit_occurrence table과 병합
            source = pd.merge(source, visit_data, left_on=["person_id", "care_site_id", self.patfg, self.medtime], right_on=["person_id", "care_site_id", "visit_source_value", "visit_start_datetime"], how="left", suffixes=('', '_y'))
            logging.debug(f'visit_occurrence 테이블과 결합 후 데이터 row수: {len(source)}, {self.memory_usage}')

            # visit_detail table과 병합
            source = pd.merge(source, visit_detail, left_on=["visit_occurrence_id"], right_on=["visit_occurrence_id"], how="left", suffixes=('', '_y'))
            logging.debug(f'visit_detail 테이블과 결합 후 데이터 row수: {len(source)}, {self.memory_usage}')

            # concept_unit과 병합
            source = pd.merge(source, unit_data, left_on=self.unit_source_value, right_on="concept_code", how="left", suffixes=["", "_unit"])
            # 값이 없는 경우 0으로 값 입력
            source.loc[source["care_site_id"].isna(), "care_site_id"] = 0
            source.loc[source["concept_id"].isna(), "concept_id"] = 0
            
            logging.debug(f'CDM 테이블과 결합 후 데이터 row수: {len(source)}, {self.memory_usage}')

            return source

        except Exception as e :
            logging.error(f"{self.table} 테이블 소스 데이터 처리 중 오류: {e}", exc_info = True)

    def transform_cdm(self, source):
        """
        주어진 소스 데이터를 CDM 형식에 맞게 변환하는 메소드.
        변환된 데이터는 새로운 DataFrame으로 구성됩니다.
        """
        try : 
            procedure_date_condition = [source[self.exectime].notna()]
            procedure_date_value = [source[self.exectime]]
            procedure_datetime_value = [source[self.exectime]]

            cdm = pd.DataFrame({
                "procedure_occurrence_id": source.index + 1,
                "person_id": source["person_id"],
                "procedure_concept_id": source["concept_id"],
                "procedure_date": np.select(procedure_date_condition, procedure_date_value, default = source[self.orddate]),
                "procedure_datetime": np.select(procedure_date_condition, procedure_datetime_value, default = source[self.orddate]),
                "procedure_type_concept_id": 38000275,
                "modifier_concept_id": 0,
                "quantity": None,
                "provider_id": source["provider_id"],
                "visit_occurrence_id": source["visit_occurrence_id"],
                "visit_detail_id": source["visit_detail_id"],
                "procedure_source_value": source[self.procedure_source_value],
                "procedure_source_concept_id": source["INSEDICODE"],
                "modifier_source_value": None,
                "vocabulary_id": "EDI"
                })

            cdm["procedure_date"] = pd.to_datetime(cdm["procedure_date"]).dt.date
            cdm["procedure_datetime"] = pd.to_datetime(cdm["procedure_datetime"])

            logging.debug(f'CDM 데이터 row수: {len(cdm)}, {self.memory_usage}')
            logging.debug(f"요약:\n{cdm.describe(include = 'O').T.to_string()}, {self.memory_usage}")
            logging.debug(f"요약:\n{cdm.describe().T.to_string()}, {self.memory_usage}")
            logging.debug(f"컬럼별 null 개수:\n{cdm.isnull().sum().to_string()}, {self.memory_usage}")

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
        self.output_filename = self.cdm_config["data"]["output_filename"]
    
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
        try:
            source1 = self.read_csv(self.source_data1, path_type = self.cdm_flag, dtype = self.source_dtype, encoding = self.encoding)
            source2 = self.read_csv(self.source_data2, path_type = self.cdm_flag, dtype = self.source_dtype, encoding = self.encoding)
            logging.debug(f'원천 데이터 row수: 원천1: {len(source1)}, 원천2: {len(source2)}, 원천1 + 원천2: {len(source1) + len(source2)}, {self.memory_usage}')

            # axis = 0을 통해 행으로 데이터 합치기, ignore_index = True를 통해 dataframe index재설정
            cdm = pd.concat([source1, source2], axis = 0, ignore_index=True)

            cdm["procedure_id"] = cdm.index + 1

            logging.debug(f'CDM 데이터 row수: {len(cdm)}, {self.memory_usage}')
            logging.debug(f"요약:\n{cdm.describe(include = 'O').T.to_string()}, {self.memory_usage}")
            logging.debug(f"요약:\n{cdm.describe().T.to_string()}, {self.memory_usage}")
            logging.debug(f"컬럼별 null 개수:\n{cdm.isnull().sum().to_string()}, {self.memory_usage}")

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

            logging.debug(f"CDM 데이터 row수 {len(cdm)}, {self.memory_usage}")
            logging.debug(f"요약:\n{cdm.describe(include = 'O').T.to_string()}, {self.memory_usage}")
            logging.debug(f"요약:\n{cdm.describe().T.to_string()}, {self.memory_usage}")
            logging.debug(f"컬럼별 null 개수:\n{cdm.isnull().sum().to_string()}, {self.memory_usage}")

            return cdm

        except Exception as e :
            logging.error(f"{self.table} 테이블 소스 데이터 처리 중 오류: {e}", exc_info = True)