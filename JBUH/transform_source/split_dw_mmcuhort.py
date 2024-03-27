import pandas as pd
import yaml
import logging, warnings, inspect
import os
from datetime import datetime


def setup_logging():
    """
    실행 시 로그에 기록하는 메소드입니다.
    """
    log_path = "transform_source/log"
    os.makedirs(log_path, exist_ok = True)
    log_filename = datetime.now().strftime('log_%Y-%m-%d_%H%M%S.log')
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s\n'

    filename = os.path.join(log_path, log_filename)
    logging.basicConfig(filename = filename, level = logging.DEBUG, format = log_format, encoding = "utf-8")

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.ERROR)
    console_handler.setFormatter(logging.Formatter(log_format))
    logging.getLogger().addHandler(console_handler)

def custom_warning_handler(message, category, filename, lineno, file=None, line=None):
    """
    실행 시 로그에 warning 항목을 기록하는 메소드입니다.
    """
    calling_frame = inspect.currentframe().f_back
    calling_code = calling_frame.f_code
    calling_function_name = calling_code.co_name
    logging.warning(f"{category.__name__} in {calling_function_name} (Line {lineno}): {message}")

def load_config(config_path):
     with open(config_path, 'r', encoding='utf-8') as file:
          return yaml.safe_load(file)
     
def split_DW(config):
    """
    DW_MMCUHORT -> DW_MMCUHORT_MED, DW_MMCUHORT_EXM, DW_MMCUHORT_ETC로 분리 
    """
    path = config["source_path"]
    filename = "dw_mmcuhort.csv"
    med_filename = "dw_mmcuhort_med.csv"
    exm_filename = "dw_mmcuhort_exm.csv"
    etc_filename = "dw_mmcuhort_etc.csv"
    chunk_size = 10000
    first_chunk = True

    for i, chunk in enumerate(pd.read_csv(os.path.join(path, filename), chunksize=chunk_size, dtype=str, lineterminator='\n', encoding="utf-8")):
        source_med = chunk[chunk["ORDTABFG"]=="MED"]
        source_exm = chunk[chunk["ORDTABFG"]=="EXM"]
        source_etc = chunk[~chunk["ORDTABFG"].isin(["MED", "EXM"])]

        if first_chunk:
            source_med.to_csv(os.path.join(path, med_filename), index = False)
            source_exm.to_csv(os.path.join(path, exm_filename), index = False)
            source_etc.to_csv(os.path.join(path, etc_filename), index = False)
            first_chunk = False
        else :
            source_med.to_csv(os.path.join(path, med_filename), index = False, mode = 'a')
            source_exm.to_csv(os.path.join(path, exm_filename), index = False, mode = 'a')
            source_etc.to_csv(os.path.join(path, etc_filename), index = False, mode = 'a')

        logging.debug(f"chunk index: {i}")
        logging.debug(f"{filename} row수: {len(chunk)}")
        logging.debug(f"{med_filename} row수: {len(source_med)}")
        logging.debug(f"{exm_filename} row수: {len(source_exm)}")
        logging.debug(f"{etc_filename} row수: {len(source_etc)}")

def test(config):
    """
    DW_MMCUHORT -> DW_MMCUHORT_MED, DW_MMCUHORT_EXM, DW_MMCUHORT_ETC로 분리 
    """
    chunk_size = 10000
    path = config["source_path"]
    filename = "dw_stexmrst.csv"
    for i, chunk in enumerate(pd.read_csv(os.path.join(path, filename), chunksize=chunk_size, dtype=str, encoding="utf-8")):
        hb = chunk[chunk["EXAMCODE"]=="L200203"]
        print(i, len(hb))

if __name__ == "__main__":
    warnings.showwarning = custom_warning_handler
    setup_logging()
    config = load_config("./config.yaml")
    split_DW(config)
    # test(config)