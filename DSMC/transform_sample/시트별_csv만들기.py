import pandas as pd
import os

# 엑셀 파일 경로를 지정합니다.
excel_file_path = "F:\\01.감염병데이터베이스\\data\\dsmc\\감염병 샘플데이터_추가요청_20240722.xlsx"

# 엑셀 파일을 읽어옵니다.
excel_file = pd.ExcelFile(excel_file_path)

# CSV 파일을 저장할 경로를 지정합니다.
output_dir = 'F:\\01.감염병데이터베이스\\data\\dsmc\\emr'

# 각 시트를 별도의 CSV 파일로 저장합니다.
for sheet_name in excel_file.sheet_names:
    df = pd.read_excel(excel_file, sheet_name=sheet_name)
    csv_file_path = os.path.join(output_dir, f'{sheet_name}.csv')
    df.to_csv(csv_file_path, index=False)
    print(f"시트 '{sheet_name}'이(가) CSV 파일 '{csv_file_path}'로 저장되었습니다.")
