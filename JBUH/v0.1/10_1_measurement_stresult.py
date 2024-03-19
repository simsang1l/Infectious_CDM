import os
import pandas as pd
import numpy as np
from datetime import datetime

def transform_to_measurement_stresult(**kwargs):
    # kwargs가 모두 입력됐는지 확인 및 변수 정의
    if "source_path" in kwargs :
        source_path = kwargs["source_path"]
    else :
        raise ValueError("source_path가 없습니다.")

    if "CDM_path" in kwargs:
        CDM_path = kwargs["CDM_path"]
    else :
        raise ValueError("CDM_path가 없습니다.")

    if "source_data1" in kwargs :
        source1 = pd.read_csv(os.path.join(source_path, kwargs["source_data1"]), dtype=str)
    else :
        raise ValueError("source_data1(원천 데이터)가 없습니다.")
    
    if "source_data2" in kwargs :
        source2 = pd.read_csv(os.path.join(source_path, kwargs["source_data2"]), dtype=str)
    else :
        raise ValueError("source_data2(원천 데이터)가 없습니다.")
    
    if "local_edi" in kwargs :
        local_edi = pd.read_csv(os.path.join(CDM_path, kwargs["local_edi"]), dtype=str)
    else :
        raise ValueError("local_edi가 없습니다.")
    
    if "care_site_data" in kwargs:
        care_site_data = pd.read_csv(os.path.join(CDM_path, kwargs["care_site_data"]), dtype=str)
    else :
        raise ValueError("care_site_data가 없습니다.")
    
    if "provider_data" in kwargs:
        provider_data = pd.read_csv(os.path.join(CDM_path, kwargs["provider_data"]), dtype=str)
    else :
        raise ValueError("provider_data가 없습니다.")

    if "person_data" in kwargs:
        person_data = pd.read_csv(os.path.join(CDM_path, kwargs["person_data"]), dtype=str)
    else :
        raise ValueError("person_data가 없습니다.")
    
    if "visit_data" in kwargs:
        visit_data = pd.read_csv(os.path.join(CDM_path, kwargs["visit_data"]), dtype=str)
    else :
        raise ValueError("visit_data가 없습니다.")
    
    if "visit_detail" in kwargs:
        visit_detail = pd.read_csv(os.path.join(CDM_path, kwargs["visit_detail"]), dtype=str)
    else :
        raise ValueError("visit_detail가 없습니다.")

    if "unit_data" in kwargs:
        unit_data = pd.read_csv(os.path.join(CDM_path, kwargs["unit_data"]), dtype=str)
    else :
        raise ValueError("unit_data가 없습니다.")   
    
    if "meddept" in kwargs:
            meddept = kwargs["meddept"]
    else :
        raise ValueError("meddept 없습니다.")
    
    if "provider" in kwargs:
        provider = kwargs["provider"]
    else :
        raise ValueError("provider 없습니다.")
    
    #####
    if "orddate" in kwargs :
        orddate = kwargs["orddate"]
    else :
        raise ValueError("orddate 없습니다.")
    
    if "exectime" in kwargs :
        exectime = kwargs["exectime"]
    else :
        raise ValueError("exectime 없습니다.")
    
    if "measurement_source_value" in kwargs:
        measurement_source_value = kwargs["measurement_source_value"]
    else :
        raise ValueError("measurement_source_value 없습니다.")
    
    if "value_source_value" in kwargs:
        value_source_value = kwargs["value_source_value"]
    else :
        raise ValueError("value_source_value 없습니다.")
    
    if "unit_source_value" in kwargs:
        unit_source_value = kwargs["unit_source_value"]
    else :
        raise ValueError("unit_source_value 없습니다.")
    
    if "range_low" in kwargs:
        range_low = kwargs["range_low"]
    else :
        raise ValueError("range_low 없습니다.")
    
    if "range_high" in kwargs:
        range_high = kwargs["range_high"]
    else :
        raise ValueError("range_high 없습니다.")
    
    print('원천 데이터 개수:', len(source1), len(source2))

    # 원천에서 조건걸기
    source1 = source1[(source1[orddate] <= "2023-08-31") & ((source1["DCYN"] == "N" )| (source1["DCYN"] == None))]
    source1 = source1[["PATNO", orddate, exectime, "ORDSEQNO", meddept, provider, "PATFG", "MEDTIME"]]
    source1[orddate] = pd.to_datetime(source1[orddate], format="%Y%m%d")
    source1[exectime] = pd.to_datetime(source1[exectime], format="%Y%m%d%H%M%S", errors = "coerce")
    
    source2 = source2[["patno", "orddate", "ordseqno", value_source_value, measurement_source_value, unit_source_value, range_low, range_high]]
    source2["orddate"] = pd.to_datetime(source2["orddate"], format="%Y%m%d")
    source2 = source2[(source2["orddate"] <= "2023-08-31") & (source2[measurement_source_value].str[:1].isin(["L", "P"]))]
    # value_as_number float형태로 저장되게 값 변경
    source2["value_as_number"] = source2[value_source_value].str.extract('(-?\d+\.\d+|\d+)')
    source2["value_as_number"] = source2["value_as_number"].astype(float)
    source2[range_low] = source2[range_low].str.extract('(-?\d+\.\d+|\d+)')
    source2[range_low] = source2[range_low].astype(float)
    source2[range_high] = source2[range_high].str.extract('(-?\d+\.\d+|\d+)')
    source2[range_high] = source2[range_high].astype(float)

    print('조건 적용후 원천 데이터 개수:', len(source1), len(source2))

    source = pd.merge(source2, source1, left_on=["patno", "orddate", "ordseqno"], right_on=["PATNO", orddate, "ORDSEQNO"], how="inner")
    # 201903081045같은 데이터가 2019-03-08 10:04:05로 바뀌는 문제 발견 
    source["MEDTIME"] = source["MEDTIME"].apply(lambda x : x[:4] + "-" + x[4:6] + "-" + x[6:8] + " " + x[8:10] + ":" + x[10:])
    source["MEDTIME"] = pd.to_datetime(source["MEDTIME"], errors = "coerce")
    print('원천 병합 후 데이터 개수:', len(source2))
    del source1
    del source2

    # local_edi 전처리
    local_edi = local_edi[["ORDCODE", "FROMDATE", "TODATE", "INSEDICODE", "concept_id"]]
    local_edi["FROMDATE"] = pd.to_datetime(local_edi["FROMDATE"] , format="%Y%m%d", errors="coerce")
    local_edi["FROMDATE"].fillna(pd.Timestamp('1900-01-01'), inplace = True)
    local_edi["TODATE"] = pd.to_datetime(local_edi["TODATE"] , format="%Y%m%d", errors="coerce")
    local_edi["TODATE"].fillna(pd.Timestamp('2099-12-31'), inplace = True)

    # LOCAL코드와 EDI코드 매핑 테이블과 병합
    source = pd.merge(source, local_edi, left_on=measurement_source_value, right_on="ORDCODE", how="inner")
    print('EDI코드 테이블과 병합 후 데이터 개수', len(source))
    source = source[(source[orddate] >= source["FROMDATE"]) & (source[orddate] <= source["TODATE"])]
    print("EDI코드 사용기간별 필터 적용 후 데이터 개수", len(source))

    # 데이터 컬럼 줄이기
    person_data = person_data[["person_id", "person_source_value"]]
    care_site_data = care_site_data[["care_site_id", "care_site_source_value"]]
    provider_data = provider_data[["provider_id", "provider_source_value"]]
    visit_data = visit_data[["visit_occurrence_id", "visit_start_datetime", "care_site_id", "visit_source_value", "person_id"]]
    visit_detail = visit_detail[["visit_detail_id", "visit_occurrence_id"]]
    unit_data = unit_data[["concept_id", "concept_code"]]

    # person table과 병합
    source = pd.merge(source, person_data, left_on="patno", right_on="person_source_value", how="inner")
    # care_site table과 병합
    source = pd.merge(source, care_site_data, left_on=meddept, right_on="care_site_source_value", how="left")
    # provider table과 병합
    source = pd.merge(source, provider_data, left_on=provider, right_on="provider_source_value", how="left", suffixes=('', '_y'))

    # visit_start_datetime 형태 변경
    visit_data["visit_start_datetime"] = pd.to_datetime(visit_data["visit_start_datetime"])

    # visit_occurrence table과 병합
    source = pd.merge(source, visit_data, left_on=["person_id", "care_site_id", "PATFG", "MEDTIME"], right_on=["person_id", "care_site_id", "visit_source_value", "visit_start_datetime"], how="left", suffixes=('', '_y'))

    # visit_detail table과 병합
    source = pd.merge(source, visit_detail, left_on=["visit_occurrence_id"], right_on=["visit_occurrence_id"], how="left", suffixes=('', '_y'))
    print(len(source))

    # concept_unit과 병합
    source = pd.merge(source, unit_data, left_on=unit_source_value, right_on="concept_code", how="left", suffixes=["", "_unit"])
    # 값이 없는 경우 0으로 값 입력
    source.loc[source["care_site_id"].isna(), "care_site_id"] = 0
    source.loc[source["concept_id"].isna(), "concept_id"] = 0

    print(source.columns.to_list())

    measurement_date_condition = [source[exectime].notna()]
    measurement_date_value = [source[exectime].dt.date]
    measurement_datetime_value = [source[exectime]]
    measurement_time_value = [source[exectime].dt.time]

    operator_condition = [
            source[value_source_value].isin([">"])
            , source[value_source_value].isin([">="])
            , source[value_source_value].isin(["="])
            , source[value_source_value].isin(["<="])
            , source[value_source_value].isin(["<"])
    ]
    operator_value = [
            4172704
            , 4171755
            , 4172703
            , 4171754
            , 4171756
    ]

    value_concept_condition = [
        source[value_source_value] == "+"
        , source[value_source_value] == "++"
        , source[value_source_value] == "+++"
        , source[value_source_value] == "++++"
        , source[value_source_value].str.lower() == "negative"
        , source[value_source_value].str.lower() == "positive"
    ]
    value_concept_value = [
        4123508
        , 4126673
        , 4125547
        , 4126674
        , 9189
        , 9191
    ]
    print(source.dtypes.tolist())
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
        "range_low": source[range_low],
        "range_high": source[range_high],
        "provider_id": source["provider_id"],
        "visit_occurrence_id": source["visit_occurrence_id"],
        "visit_detail_id": source["visit_detail_id"],
        "measurement_source_value": source[measurement_source_value],
        "measurement_source_concept_id": source["INSEDICODE"],
        "unit_source_value": source[unit_source_value],
        "value_source_value": source[value_source_value].str[:50],
        "vocabulary_id": "EDI"
        })

    # cdm["measurement_date"] = pd.to_datetime(cdm["measurement_date"])
    # cdm["measurement_datetime"] = pd.to_datetime(cdm["measurement_datetime"])

    # csv파일로 저장
    cdm.to_csv(os.path.join(CDM_path, "measurement_stresult.csv"), index=False)

    print(cdm.isnull().sum())
    print(cdm.shape)
    print('CDM 데이터 개수', len(cdm))
    

start_time = datetime.now()
transform_to_measurement_stresult( source_path = "F:\\01.감염병데이터베이스\\data\\2023_infectious_DW"
                            , CDM_path = "F:\\01.감염병데이터베이스\\data\\2023_infectious_CDM"
                            , source_data1 = "ods_mmexmort.csv"
                            , source_data2 = "ods_stresult.csv"
                            , care_site_data = "care_site.csv"
                            , person_data = "person.csv"
                            , provider_data = "provider.csv"
                            , visit_data = "visit_occurrence.csv"
                            , visit_detail = "visit_detail.csv"
                            , local_edi = "local_edi.csv"
                            , unit_data = "concept_unit.csv"
                            , meddept = "MEDDEPT"
                            , provider = "ORDDR"

                            , orddate = "ORDDATE"
                            , exectime = "EXECTIME"
                            , measurement_source_value = "examcode"
                            , value_source_value = "rsltnum"
                            , unit_source_value = "rsltunit"
                            , range_low = "norlow"
                            , range_high = "norhigh"
)

print(f"transform and Load end, elapsed_time is : {datetime.now() - start_time}")