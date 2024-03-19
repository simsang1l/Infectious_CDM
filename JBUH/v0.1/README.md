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
`source_path`: 원천이 저장된 폴더 위치 입력  
`CDM_path`: <u>**care_site.csv**</u>파일이 저장될 폴더 입력  
`source_data`: care_site를 만들 <u>원천 파일명</u> 입력  
`location_data`: <u>**location.csv**</u>입력. 파일명이 다르다면 그에 맞게 수정  
`care_site_name`: care_site_name컬럼에 들어갈 값이 저장된  <u>원천 컬럼명</u> 입력  
`care_site_source_value`: care_site_source_value에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`place_of_service_source_value`: place_of_service_source_value에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  

## provider.py
코드 가장 하단에 있는 `transform_to_provider`의 변수 값 변경!  
`source_path`: 원천이 저장된 폴더 위치 입력  
`CDM_path`: <u>**provider.csv**</u>파일이 저장될 폴더 입력  
`source_data`: provider를 만들 <u>원천 파일명</u> 입력  
`care_site_data`: care_site.py단계에서 생성된 care_site파일명 입력  
`provider_name`: provider_name컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`provider_source_value`: provider_source_value컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`specialty_source_value`: specialty_source_value컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력.  
`gender_source_value`: gender_source_value컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`care_site_source_value`: care_site_source_value컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  

## person.py
코드 가장 하단에 있는 `transform_to_person`의 변수 값 변경!  
`source_path`: 원천이 저장된 폴더 위치 입력  
`CDM_path`: <u>**person.csv**</u>파일이 저장될 폴더 입력  
`source_data`: person을 만들 <u>원천 파일명</u> 입력  
`location_data`: location.csv 입력  
`gender_source_value`: gender_source_value컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`person_source_value`: person_source_value컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`death_datetime`: death_datetime컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`birth_resno1`: 주민번호 앞자리가 저장된 <u>원천 컬럼명</u> 입력  
`birth_resno2`: 주민번호 뒷자리가 저장된 <u>원천 컬럼명</u> 입력  

## visit_occurrence.py
코드 가장 하단에 있는 `transform_to_visit_occurrence`의 변수 값 변경!  
`source_path`: 원천이 저장된 폴더 위치 입력  
`CDM_path`: <u>**visit_occurrence.csv**</u>파일이 저장될 폴더 입력  
`source_data`: visit_occurrence를 만들 <u>원천 파일명</u> 입력  
`source_data2`: visit_occurrence를 만들 <u>원천 파일명</u> 입력  
`care_site_data`: care_site.py단계에서 생성된 care_site파일명 입력  
`person_data`: person.py단계에서 생성된 person파일명 입력  
`provider_data`: provider.py단계에서 생성된 provider파일명 입력  
`admitting_from_source_value`:admitting_from_source_value컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`discharge_to_source_value`: discharge_to_source_value컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`medtime`: 진료시간에 대한 원천 컬럼명 입력  
`admtime`: 입원 시간에 대한 원천 컬럼명 입력  
`dschtime`: 퇴원 시간에 대한 원천 컬럼명 입력  
`meddept`: 진료 부서에 대한 원천 컬럼명 입력  
`visit_source_value`: visit_source_value컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  

## visit_detail.py
코드 가장 하단에 있는 `transform_to_visit_detail`의 변수 값 변경!  
`source_path`: 원천이 저장된 폴더 위치 입력  
`CDM_path`: <u>**visit_detail.csv**</u>파일이 저장될 폴더 입력  
`source_data`: visit_detail을 만들 <u>원천 파일명</u> 입력  
`care_site_data`: care_site.py단계에서 생성된 care_site파일명 입력  
`person_data`: person.py단계에서 생성된 person파일명 입력
`provider_data`: provider.py단계에서 생성된 provider파일명 입력  
`visit_data`: visit_occurrence.py 단계에서 생성된 visit_occurrence파일명 입력  
`visit_detail_start_datetime`: visit_detail_start_datetime컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`visit_detail_end_datetime`: visit_detail_end_datetime컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`admitted_from_source_value`: admitted_from_source_value컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`discharged_to_source_value`: discharged_to_source_value컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`meddept`: 진료 부서에 대한 원천 컬럼명 입력  
`provider`: 진료의에 대한 원천 컬럼명 입력  

## condition_occurrence.py
코드 가장 하단에 있는 `transform_to_condition_occurrence`의 변수 값 변경!  
`source_path`: 원천이 저장된 폴더 위치 입력  
`CDM_path`: <u>**condition_occurrence.csv**</u>파일이 저장될 폴더 입력  
`source_data`: condition_occurrence을 만들 <u>원천 파일명</u> 입력  
`care_site_data`: care_site.py단계에서 생성된 care_site파일명 입력  
`person_data`: person.py단계에서 생성된 person파일명 입력
`provider_data`: provider.py단계에서 생성된 provider파일명 입력  
`visit_data`: visit_occurrence.py 단계에서 생성된 visit_occurrence파일명 입력  
`visit_detail`: visit_detail.py 단계에서 생성된 visit_detail파일명 입력  
`meddept`: 진료 부서에 대한 원천 컬럼명 입력  
`provider`: 진료의에 대한 원천 컬럼명 입력  
`condition_start_datetime`: condition_start_datetime컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`condition_type`: condition_type_concept_id들어갈 concept_id 구분을위해 사용될 <u>원천 컬럼명</u> 입력  
`condition_source_value`: condition_source_value컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`condition_status_source_value`: condition_status_source_value컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  

## local_edi.py
>원천 코드와 EDI코드가 매핑된 파일을 생성하기 위한 단계. 처방과 관련된 사용된 모든 테이블에 사용됨  

코드 가장 하단에 있는 `transform_to_local_edi`의 변수 값 변경!  
`source_path`: 원천이 저장된 폴더 위치 입력  
`CDM_path`: <u>**local_edi.csv**</u>파일이 저장될 폴더 입력  
`order_data`: <u>처방코드 마스터</u>가 저장된 원천 파일명 입력  
`edi_data`: 처방코드와 EDI코드가 매핑된 테이블에 대한 원천 파일명 입력  
`concept_data`: EDI코드와 EDI concept_id매핑을 위해 전달해드린 <u>concept_EDI_KDC.csv</u> 입력  
`ordercode`: 처방코드가 저장된 원천 컬럼명 입력  
`sugacode`: edi_data의 처방코드가 저장된 원천 컬럼명 입력  
`edicode`: edicode가 저장된 원천 컬럼명 입력  
`fromdate`: 처방 코드의 사용 시작일 컬럼 입력  
`todate`: 처방 코드의 사용 종료일 컬럼 입력  

## drug_exposure.py
코드 가장 하단에 있는 `transform_to_drug_exposure`의 변수 값 변경! 
`source_path`: 원천이 저장된 폴더 위치 입력  
`CDM_path`: <u>**drug_exposure.csv**</u>파일이 저장될 폴더 입력  
`source_data`: drug_exposure를 만들 <u>원천 파일명</u> 입력  
`care_site_data`: care_site.py단계에서 생성된 care_site파일명 입력  
`person_data`: person.py단계에서 생성된 person파일명 입력
`provider_data`: provider.py단계에서 생성된 provider파일명 입력  
`visit_data`: visit_occurrence.py 단계에서 생성된 visit_occurrence파일명 입력  
`visit_detail`: visit_detail.py 단계에서 생성된 visit_detail파일명 입력  
`local_edi`: local_edi.py 단계에서 생성된 local_edi 파일명 입력  
`meddept`: 진료 부서에 대한 원천 컬럼명 입력  
`provider`: 진료의에 대한 원천 컬럼명 입력  
`drug_exposure_start_datetime`: drug_exposure_start_datetime컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`drug_source_value`: drug_source_value컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`days_supply`: days_supply컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`qty`: 1회 복용 복용량 대한 원천 컬럼명 입력  
`cnt`: 1일 복용 횟수에 대한 원천 컬럼명 입력  
`dose_unit_source_value`: dose_unit_source_value컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  


## measurement_stresult.py
코드 가장 하단에 있는 `transform_to_measurement_stresult`의 변수 값 변경! 
`source_path`: 원천이 저장된 폴더 위치 입력  
`CDM_path`: <u>**measurement_result.csv**</u>파일이 저장될 폴더 입력  
`source_data1`: measurement_result를 만들 첫 번째 <u>원천 파일명</u> 입력  
`source_data2`: measurement_result를 만들 두 번째 <u>원천 파일명</u> 입력  
`care_site_data`: care_site.py단계에서 생성된 care_site파일명 입력  
`person_data`: person.py단계에서 생성된 person파일명 입력
`provider_data`: provider.py단계에서 생성된 provider파일명 입력  
`visit_data`: visit_occurrence.py 단계에서 생성된 visit_occurrence파일명 입력  
`visit_detail`: visit_detail.py 단계에서 생성된 visit_detail파일명 입력  
`local_edi`: local_edi.py 단계에서 생성된 local_edi 파일명 입력  
`meddept`: 진료 부서에 대한 원천 컬럼명 입력  
`provider`: 진료의에 대한 원천 컬럼명 입력  
`orddate`: 처방일에 대한 원천 컬럼명 입력
`exectime`: 검사일에 대한 원천 컬럼명 입력
`measurement_source_value`: measurement_source_value컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력  
`value_source_value`: 검사 결과값에 대한 원천 컬럼명 입력  
`unit_source_value`: 검사 결과 단위에 대한 원천 컬럼명 입력  
`range_low`: 검사 결과값의 정상치 하한 값에 대한 원천 컬럼명 입력  
`range_high`: 검사 결과값의 정상치 상한 값에 대한 원천 컬럼명 입력 

## measurement_bmi.py
코드 가장 하단에 있는 `transform_to_measurement_bmi`의 변수 값 변경! 
`source_path`: 원천이 저장된 폴더 위치 입력  
`CDM_path`: <u>**measurement_bmi.csv**</u>파일이 저장될 폴더 입력  
`source_data1`: measurement_bmi를 만들 <u>원천 파일명</u> 입력  
`care_site_data`: care_site.py단계에서 생성된 care_site파일명 입력  
`person_data`: person.py단계에서 생성된 person파일명 입력
`provider_data`: provider.py단계에서 생성된 provider파일명 입력  
`visit_data`: visit_occurrence.py 단계에서 생성된 visit_occurrence파일명 입력  
`visit_detail`: visit_detail.py 단계에서 생성된 visit_detail파일명 입력  
`meddept`: 진료 부서에 대한 원천 컬럼명 입력  
`provider`: 진료의에 대한 원천 컬럼명 입력  
`admtime`: 입원일에 대한 원천 컬럼명 입력  
`height`: 키에 대한 원천 컬럼명 입력  
`weight`: 체중에 대한 원천 컬럼명 입력  

## merge_measurement.py
코드 가장 하단에 있는 `merge_measurement`의 변수 값 변경! 
최종 measurement.csv로 만드는 작업 수행  
`source_data1`: measurement를 만들 <u>measurement_result.csv</u> 입력  
`source_data2`: measurement를 만들 <u>measurement_bmi.csv</u> 입력  

## procedure_occurrence_trt.py
코드 가장 하단에 있는 `transform_to_procedure_order_trt`의 변수 값 변경!  
수술, 처치, 마취관련 처방 데이터 변환  
`source_path`: 원천이 저장된 폴더 위치 입력  
`CDM_path`: <u>**procedure_order_trt.csv**</u>파일이 저장될 폴더 입력  
`source_data1`: procedure_order_trt를 만들 <u>원천 파일명</u> 입력  
`care_site_data`: care_site.py단계에서 생성된 care_site파일명 입력  
`person_data`: person.py단계에서 생성된 person파일명 입력
`provider_data`: provider.py단계에서 생성된 provider파일명 입력  
`visit_data`: visit_occurrence.py 단계에서 생성된 visit_occurrence파일명 입력  
`visit_detail`: visit_detail.py 단계에서 생성된 visit_detail파일명 입력  
`local_edi`: local_edi.py 단계에서 생성된 local_edi 파일명 입력  
`meddept`: 진료 부서에 대한 원천 컬럼명 입력  
`provider`: 진료의에 대한 원천 컬럼명 입력  
`orddate`: 처방일에 대한 원천 컬럼명  
`exectime`: 시행일에 대한 원천 컬럼명  
`opdate`: 수술 날짜에 대한 원천 컬럼명  
`procedure_source_value`: procedure_source_value컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력

## procedure_occurrence_result.py
코드 가장 하단에 있는 `transform_to_procedure_stresult`의 변수 값 변경!  
진검, 병리 관련 검사를 제외한 검사 데이터 변환  
`source_path`: 원천이 저장된 폴더 위치 입력  
`CDM_path`: <u>**procedure_result.csv**</u>파일이 저장될 폴더 입력  
`source_data1`: procedure_result를 만들 첫 번째 <u>원천 파일명</u> 입력  
`source_data2`: procedure_result를 만들 두 번째 <u>원천 파일명</u> 입력  
`care_site_data`: care_site.py단계에서 생성된 care_site파일명 입력  
`person_data`: person.py단계에서 생성된 person파일명 입력
`provider_data`: provider.py단계에서 생성된 provider파일명 입력  
`visit_data`: visit_occurrence.py 단계에서 생성된 visit_occurrence파일명 입력  
`visit_detail`: visit_detail.py 단계에서 생성된 visit_detail파일명 입력  
`local_edi`: local_edi.py 단계에서 생성된 local_edi 파일명 입력  
`unit_data`: 제공한 concept_unit.csv파일명 입력. 검사 단위에 대한 concept_id 입력에 사용됨  
`meddept`: 진료 부서에 대한 원천 컬럼명 입력  
`provider`: 진료의에 대한 원천 컬럼명 입력  
`orddate`: 처방일에 대한 원천 컬럼명 입력  
`exectime`: 시행일에 대한 원천 컬럼명 입력  
`procedure_source_value`: procedure_source_value컬럼에 들어갈 값이 저장된 <u>원천 컬럼명</u> 입력
`value_source_value`: 검사 결과값에 대한 원천 컬럼명 입력  
`unit_source_value`: 검사 결과 단위에 대한 원천 컬럼명 입력  
`range_low`: 검사 결과값의 정상치 하한 값에 대한 원천 컬럼명 입력  
`range_high`: 검사 결과값의 정상치 상한 값에 대한 원천 컬럼명 입력  

## merge_procedure.py
코드 가장 하단에 있는 `merge_procedure`의 변수 값 변경! 
최종 procedure_occurrence.csv로 만드는 작업 수행  
`source_data1`: procedure_occurrence를 만들 <u>procedure_order_trt.csv</u> 입력  
`source_data2`: procedure_occurrence를 만들 <u>procedure_result.csv</u> 입력  

## observation_period.py
코드 가장 하단에 있는 `transform_to_observation_period`의 변수 값 변경! 
`CDM_path`: <u>**CDM데이터가 저장된**</u> 폴더 경로 입력  
`visit_data`: visit_occurrence.csv 입력  
`condition_data`: condition_occurrence.csv 입력  
`drug_data`: drug_exposure.csv 입력  
`measurement_data`: measurement.csv 입력  
`procedure_data`: procedure_occurrence.csv 입력  