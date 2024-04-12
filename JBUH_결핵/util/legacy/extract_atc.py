#################################################################
# 스크롤 내려서 데이터 가져오는 부분 기능이 정상 작동하지 않아 폐기  #
#################################################################

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
import pandas as pd

# WebDriver 초기화
driver = webdriver.Chrome(service = ChromeService(ChromeDriverManager().install()))

# 웹 페이지 열기
driver.get('https://biz.kpis.or.kr/kpis_biz/index.jsp?sso=ok')

WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

# '코드매핑조회'를 찾아 클릭
search_druginfo = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.ID, "mainframe.VFrameSet00.HomeFrame.form.divMain.form.Div02.form.divLink.form.btnConnFileUpLoad:icontext")))
search_druginfo.click()

# 'ATC 정보조회'를 찾아 클릭
atc = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.ID, "mainframe.VFrameSet00.HFrameSet00.LeftFrame.form.grdMenu.body.gridrow_1.cell_1_0.celltreeitem.treeitemtext:text")))
atc.click()

# 'ATC index 및 가이드라인'을 찾아 클릭
atc_guide = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.ID, "mainframe.VFrameSet00.HFrameSet00.LeftFrame.form.grdMenu.body.gridrow_3.cell_3_0.celltreeitem.treeitemtext:text")))
atc_guide.click()

# 데이터 추출을 위한 설정
data_list = []

# 스크롤이 포함된 div를 기다림
scrollable_div = WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, "mainframe.VFrameSet00.HFrameSet00.VFrameSet00.FrameSet00.M_MP00000197.form.divWork.form.grdAtcIndexList.vscrollbar.incbutton:icontext")))

i = 0
while "V20" not in data_list :
    current_rows_count = len(driver.find_elements(By.CLASS_NAME, "GridRowControl"))
    
    # 현재 페이지의 데이터 추출
    rows = driver.find_elements(By.CLASS_NAME, "GridRowControl")
    # rows = WebDriverWait(driver, 20).until(
    #     lambda d: d.find_elements(By.CLASS_NAME, "GridRowControl") if len(d.find_elements(By.CLASS_NAME, "GridRowControl")) > current_rows_count else False
    # )
    for row in rows:
        # 예시: ATC 코드, ATC 코드명, 가이드라인 정보 추출
        cells = row.find_elements(By.CLASS_NAME, "GridCellControl")
        if len(cells) >= 2:
            atc_code = cells[0].text.strip()
            atc_name = cells[1].text.strip()
            guideline = cells[2].text.strip() if len(cells) > 2 else ""
            
            data_list.append((atc_code, atc_name, guideline))

    # 스크롤 실행
    for _ in range(12):
        scrollable_div.click()
        time.sleep(0.3)
    # driver.execute_script("arguments[0].scrollTo(0, 200)", scrollable_div)
    # 페이지가 로드되기를 기다림
    time.sleep(3)
    i += 1

# WebDriver 종료
driver.quit()

df = pd.DataFrame(data_list, columns =["ATC코드", "ATC코드명", "가이드라인"])
df = df.drop_duplicates()
df.to_csv("util/atccode.csv")
# 추출된 데이터 출력
for item in data_list:
    print(item)


# i = 0
# result = []
# key_id = "mainframe.VFrameSet00.HFrameSet00.VFrameSet00.FrameSet00.M_MP00000197.form.divWork.form.grdAtcIndexList.body.gridrow_"
# while True:    
#     grid_row = soup.find('div', id= key_id + str(i))

#     # 해당 행에서 모든 셀 데이터 찾기
#     cells = grid_row.find_all('div', class_='GridCellControl')

#     # 각 셀에서 데이터 추출
#     for cell in cells:
#         cell_text = cell.text.strip()
#         result.append(cell_text)
#     print(result)
#         # 셀 내의 텍스트 데이터를 포함하는 div 찾기
#         # text_div = cell.find('div', class_='nexacontentsbox')
#         # if text_div:
#         #     print(';;',text_div.text)

#     i += 1


# df = pd.DataFrame(result, columns=["ATC코드", "ATC코드명", "가이드라인"])
# df.to_csv("./QC/atccode.csv")


