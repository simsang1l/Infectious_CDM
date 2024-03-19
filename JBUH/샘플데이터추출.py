import pandas as pd
# 질환별 환자 추출

path = 'F:\\01.감염병데이터베이스\\data\\2023_infectious_DW'
save_path = 'F:\\01.감염병데이터베이스\\data\\2023_infectious_DW\\sample'

diagnosis = ["covid", "sfts", "쯔쯔가무시"]

filename = ["ods_apipdlst"
            , "ods_apopdlst"
            , "ods_appatbat"
            , "ods_mesearct"
            , "ods_mipantit"
            , "ods_mmbasort"
            , "ods_mmbldort"
            , "ods_mmconslt"
            , "ods_mmexmort"
            , "ods_mmmedort"
            , "ods_mmopscet"
            , "ods_mmpdiagt"
            , "ods_mmtrtort"
            , "ods_mnicupat"
            , "ods_mnnisemt"
            , "ods_mnopnrmt"
            , "ods_mtcurort"
            , "ods_sdorddet"
            , "ods_sicancht"
            , "ods_sidiglht"
            , "ods_siidscht"
            , "ods_stresult"]

# covid, SFTS, 쯔쯔가무시
for diag in diagnosis :
    # patient = pd.read_csv(f'{path}/ods_appatbat_{diag}.csv', dtype=str)
    # patient = patient['PATNO'][:3].tolist()

    for table in filename:
        if  table == 'ods_stresult' and diag == 'sfts':
            data = pd.read_csv(f'{path}/{table}.csv', dtype = str)
        else :
            data = pd.read_csv(f'{path}/{table}.csv', dtype = str)
        # save_df = data[data['PATNO'].isin(patient)].head(3)
        save_df = data.head(3)
        print(f'{table}_{diag} process...')
        save_df.to_csv(f'{save_path}/{table}_{diag}.csv', index = False)
