# 사전 준비사항
1. CDM으로 변환할 원천 데이터가 있는 폴더 생성
   1. 해당 폴더에 원천 데이터 저장(.csv, .xml...)
2. CDM이 저장될 폴더 생성  
<br>  

# 코드 실행 순서
>파일에 붙은 순번대로 실행해주세요.
1. 전달드리는 <u>1_location.csv</u>, <u>concept_EDI_KDC.csv</u>, <u>concept_KCD7.csv</u>, <u>concept_unit.csv</u> 파일을 CDM이 저장될 폴더에 저장
2. 1_location.csv는 location.csv로 파일명 변경
3. care_site.py 부터 observation_period.py 까지 순서대로 실행  
<br>
<br>

# 코드 실행시 참고 사항
## care_site.py
코드 가장 하단에 있는 `transform_to_care_site`의 변수 값 변경!  
`source_path`: 원천이 저장된 폴더 경로 입력  
`CDM_path`: <u>**care_site.csv**</u>파일이 저장될 폴더 경로 입력  
`source_data`: care_site를 만들 <u>원천 파일명</u> 입력  
`location_data`: <u>**location.csv**</u>입력. 파일명이 다르다면 그에 맞게 수정  
`care_site_name`: care_site_name컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`care_site_source_value`: care_site_source_value에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`place_of_service_source_value`: place_of_service_source_value에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  

## provider.py
코드 가장 하단에 있는 `transform_to_provider`의 변수 값 변경!  
`source_path`: 원천이 저장된 폴더 경로 입력  
`CDM_path`: <u>**provider.csv**</u>파일이 저장될 폴더 경로 입력  
`source_data`: provider를 만들 <u>원천 파일명</u> 입력  
`care_site_data`: care_site.py단계에서 생성된 care_site파일명 입력  
`provider_name`: provider_name컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`year_of_birth`: year_of_birth컬럼에 들어갈 값이 저징된 <u>원천 컬럼명</u> 입력  
`provider_source_value`: provider_source_value컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`specialty_source_value`: specialty_source_value컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력.  
`gender_source_value`: gender_source_value컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`care_site_source_value`: care_site_source_value컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  

## person.py
코드 가장 하단에 있는 `transform_to_person`의 변수 값 변경!  
`source_path`: 원천이 저장된 폴더 경로 입력  
`CDM_path`: <u>**person.csv**</u>파일이 저장될 폴더 경로 입력  
`source_data`: person을 만들 <u>원천 파일명</u> 입력  
`location_data`: location.csv 입력  
`death_data`: 사망일 정보가 저장된 <u>원천 파일명</u> 입력  
`race_source_value`: 외국인 구분 정보가 저장된 <u>원천 컬럼명</u> 입력  
`gender_source_value`: gender_source_value컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`person_source_value`: person_source_value컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`death_datetime`: death_datetime컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`birth_date`: 환자의 생년월일 정보가 저장된 <u>원천 컬럼명</u> 입력  
`location_source_value`: 환자의 우편번호 정보가 저장된 <u>원천 컬럼명</u> 입력  

## visit_occurrence.py
코드 가장 하단에 있는 `transform_to_visit_occurrence`의 변수 값 변경!  
`source_path`: 원천이 저장된 폴더 경로 입력  
`CDM_path`: <u>**visit_occurrence.csv**</u>파일이 저장될 폴더 경로 입력  
`source_data`: visit_occurrence를 만들 <u>원천 파일명</u> 입력  
`person_data`: person.py단계에서 생성된 person파일명 입력  
`person_source_value`: 환자번호에 해당하는 <u>원천 컬럼명</u> 입력  
`admitting_from_source_value`:admitting_from_source_value컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`discharge_to_source_value`: discharge_to_source_value컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`visit_start_datetime`: 내원시작일시에 대한 <u>원천 컬럼명</u> 입력  
`visit_end_datetime`: 내원종료일시에 대한 <u>원천 컬럼명</u> 입력  
`visit_source_value`: visit_source_value컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`visit_source_key`: 방문에 관련된 key정보가 저장된 <u>원천 컬럼명</u> 입력  

## visit_detail.py
코드 가장 하단에 있는 `transform_to_visit_detail`의 변수 값 변경!  
`source_path`: 원천이 저장된 폴더 경로 입력  
`CDM_path`: <u>**visit_detail.csv**</u>파일이 저장될 폴더 경로 입력  
`source_data`: visit_detail을 만들 <u>원천 파일명</u> 입력  
`person_data`: person.py단계에서 생성된 person파일명 입력  
`visit_data`: visit_occurrence.py 단계에서 생성된 visit_occurrence파일명 입력  
`person_source_value`: 환자번호에 해당하는 <u>원천 컬럼명</u> 입력  
`visit_detail_start_datetime`: visit_detail_start_datetime컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`visit_detail_end_datetime`: visit_detail_end_datetime컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`visit_source_key`: 방문에 관련된 key정보가 저장된 <u>원천 컬럼명</u> 입력  

## condition_occurrence.py
코드 가장 하단에 있는 `transform_to_condition_occurrence`의 변수 값 변경!  
`source_path`: 원천이 저장된 폴더 경로 입력  
`CDM_path`: <u>**condition_occurrence.csv**</u>파일이 저장될 폴더 경로 입력  
`source_data`: condition_occurrence을 만들 <u>원천 파일명</u> 입력  
`person_data`: person.py단계에서 생성된 person파일명 입력  
`visit_data`: visit_occurrence.py 단계에서 생성된 visit_occurrence파일명 입력  
`visit_detail`: visit_detail.py 단계에서 생성된 visit_detail파일명 입력  
`person_source_value`: 환자번호에 해당하는 <u>원천 컬럼명</u> 입력  
`condition_start_datetime`: condition_start_datetime컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`condition_type`: condition_type_concept_id들어갈 concept_id 구분을위해 사용될 <u>원천 컬럼명</u> 입력  
`condition_source_value`: condition_source_value컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`condition_status_source_value`: condition_status_source_value컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`visit_source_key`: 방문에 관련된 key정보가 저장된 <u>원천 컬럼명</u> 입력  

## local_edi.py
>원천 코드와 EDI코드가 매핑된 파일을 생성하기 위한 단계. 처방과 관련된 사용된 모든 테이블에 사용됨  

코드 가장 하단에 있는 `transform_to_local_edi`의 변수 값 변경!  
`source_path`: 원천이 저장된 폴더 경로 입력  
`CDM_path`: <u>**local_edi.csv**</u>파일이 저장될 폴더 경로 입력  
`order_data`: <u>처방코드 마스터</u>가 저장된 원천 파일명 입력  
`concept_data`: EDI코드와 EDI concept_id매핑을 위해 전달해드린 <u>concept_EDI_KDC.csv</u> 입력  
`ordercode`: 처방코드가 저장된 <u>원천 컬럼명</u> 입력  
`edicode`: edicode가 저장된 <u>원천 컬럼명</u> 입력  
`fromdate`: 처방 코드의 사용 시작일에 대한 <u>원천 컬럼명</u> 입력  
`todate`: 처방 코드의 사용 종료일에 대한 <u>원천 컬럼명</u> 입력  

## drug_exposure.py
코드 가장 하단에 있는 `transform_to_drug_exposure`의 변수 값 변경!  
`source_path`: 원천이 저장된 폴더 경로 입력  
`CDM_path`: <u>**drug_exposure.csv**</u>파일이 저장될 폴더 경로 입력  
`source_data`: drug_exposure를 만들 <u>원천 파일명</u> 입력  
`person_data`: person.py단계에서 생성된 person파일명 입력
`visit_data`: visit_occurrence.py 단계에서 생성된 visit_occurrence파일명 입력  
`visit_detail`: visit_detail.py 단계에서 생성된 visit_detail파일명 입력  
`local_edi`: local_edi.py 단계에서 생성된 local_edi 파일명 입력  
`ordcode`: local_edi.csv의 처방코드에 대한 컬럼명 입력  
`edicode`: local_edi.csv의 EDI코드에 대한 컬럼명 입력  
`fromdate`: local_edi.csv의 코드 시작일에 대한 컬럼명 입력  
`todate`: local_edi.csv의 코드 종료일에 대한 컬럼명 입력  
`person_source_value`: 환자번호에 해당하는 <u>원천 컬럼명</u> 입력  
`drug_exposure_start_datetime`: drug_exposure_start_datetime컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`drug_source_value`: drug_source_value컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`days_supply`: days_supply컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`qty`: 1회 복용 복용량 대한 원천 컬럼명 입력  
`cnt`: 1일 복용 횟수에 대한 원천 컬럼명 입력  
`route_source_value`: 약물 투여경로에 대한 원천 컬럼명 입력
`dose_unit_source_value`: dose_unit_source_value컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`visit_source_key`: 방문에 관련된 key정보가 저장된 <u>원천 컬럼명</u> 입력  

## measurement.py
코드 가장 하단에 있는 `transform_to_measurement`의 변수 값 변경! 
`source_path`: 원천이 저장된 폴더 위치 입력  
`CDM_path`: <u>**measurement.csv**</u>파일이 저장될 폴더 입력  
`source_data1`: measurement를 만들 <u>원천 파일명</u> 입력  
`person_data`: person.py단계에서 생성된 person파일명 입력
`visit_data`: visit_occurrence.py 단계에서 생성된 visit_occurrence파일명 입력  
`visit_detail`: visit_detail.py 단계에서 생성된 visit_detail파일명 입력  
`local_edi`: local_edi.py 단계에서 생성된 local_edi 파일명 입력  
`unit_data`: 전달드린 concept_unit.csv 파일명 입력  
`person_source_value`: 환자번호에 해당하는 <u>원천 컬럼명</u> 입력  
`ordcode`: local_edi.csv의 처방코드에 대한 컬럼명 입력  
`edicode`: local_edi.csv의 EDI코드에 대한 컬럼명 입력  
`fromdate`: local_edi.csv의 코드 시작일에 대한 컬럼명 입력  
`todate`: local_edi.csv의 코드 종료일에 대한 컬럼명 입력  
`measurement_date`: 검사일에 대한 <u>원천 컬럼명</u> 입력  
`measurement_source_value`: measurement_source_value컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`value_source_value`: 검사 결과값에 대한 원천 컬럼명 입력  
`unit_source_value`: 검사 결과 단위에 대한 원천 컬럼명 입력  
`result_range`: 검사 결과값의 정상치 범위에 대한 원천 컬럼명 입력  
`visit_source_key`: 방문에 관련된 key정보가 저장된 <u>원천 컬럼명</u> 입력  


## procedure_occurrence.py
코드 가장 하단에 있는 `transform_to_procedure_occurrence`의 변수 값 변경!  
수술, 처치, 마취관련 처방 데이터 변환  
`source_path`: 원천이 저장된 폴더 위치 입력  
`CDM_path`: <u>**procedure_occurrence.csv**</u>파일이 저장될 폴더 입력  
`source_data1`: procedure_occurrence를 만들 <u>원천 파일명</u> 입력  
`person_data`: person.py단계에서 생성된 person파일명 입력
`visit_data`: visit_occurrence.py 단계에서 생성된 visit_occurrence파일명 입력  
`visit_detail`: visit_detail.py 단계에서 생성된 visit_detail파일명 입력  
`local_edi`: local_edi.py 단계에서 생성된 local_edi 파일명 입력  
`person_source_value`: 환자번호에 해당하는 <u>원천 컬럼명</u> 입력  
`ordcode`: local_edi.csv의 처방코드에 대한 컬럼명 입력  
`edicode`: local_edi.csv의 EDI코드에 대한 컬럼명 입력  
`fromdate`: local_edi.csv의 코드 시작일에 대한 컬럼명 입력  
`todate`: local_edi.csv의 코드 종료일에 대한 컬럼명 입력  
`orddate`: 처방일에 대한 원천 컬럼명  
`procedure_date`: 시술/처치일에 대한 원천 컬럼명 입력  
`procedure_source_value`: procedure_source_value컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`visit_source_key`: 방문에 관련된 key정보가 저장된 <u>원천 컬럼명</u> 입력  

## observation_period.py
코드 가장 하단에 있는 `transform_to_observation_period`의 변수 값 변경!  
`CDM_path`: <u>**CDM데이터가 저장된**</u> 폴더 경로 입력  
`visit_data`: visit_occurrence.csv 입력  
`condition_data`: condition_occurrence.csv 입력  
`drug_data`: drug_exposure.csv 입력  
`measurement_data`: measurement.csv 입력  
`procedure_data`: procedure_occurrence.csv 입력  