import yaml
from QC.src import (table_person_row_count, table_provider_row_count, 
                    table_visit_occurrence_row_count, table_visit_detail_row_count,
                    table_condition_occurrence_row_count, table_drug_exposure_row_count,
                    table_measurement_stexmrst_row_count, table_measurement_vs_row_count,
                    table_procedure_order_row_count, table_procedure_stexmrst_row_count,
                    field_care_site_summary, field_provider_summary, field_person_summary,
                    field_visit_occurrence_summary, field_visit_detail_summary,
                    field_condition_occurrence_summary, field_drug_exposure_summary,
                    field_measurement_summary, field_procedure_occurrence_summary,
                    field_observation_period_summary, field_local_edi_summary
                    )

def load_config(config_path):
    with open(config_path, 'r', encoding="utf-8") as file:
        return yaml.safe_load(file)
    
if __name__ == "__main__":
    config_path = "config.yaml"
    config = load_config(config_path)
    cdm_path = config["CDM_path"]
    source_path = config["source_path"]
    excel_path = config["excel_path"]
    sheetname_table = config["sheet_table_count"]
    sheetname_field = config["sheet_field_summary"]

    ### table 품질 진단 ###
    # table_person_row_count.person_row_count(config, cdm_path, source_path, excel_path)
    # table_provider_row_count.provider_row_count(config, cdm_path, source_path, excel_path)
    # table_visit_occurrence_row_count.visit_occurrence_row_count(config, cdm_path, source_path, excel_path, config_path, sheetname)
    # table_visit_detail_row_count.visit_detail_row_count(config, cdm_path, source_path, excel_path, config_path, sheetname)
    # table_condition_occurrence_row_count.condition_occurrence_row_count(config, cdm_path, source_path, excel_path, config_path, sheetname)
    # table_drug_exposure_row_count.drug_exposure_row_count(config, cdm_path, source_path, excel_path, config_path, sheetname)
    # table_measurement_stexmrst_row_count.measurement_stexmrst_row_count(config, cdm_path, source_path, excel_path, config_path, sheetname)
    # table_measurement_vs_row_count.measurement_vs_row_count(config, cdm_path, source_path, excel_path, config_path, sheetname)
    # table_procedure_order_row_count.procedure_order_row_count(config, cdm_path, source_path, excel_path, config_path, sheetname)
    # table_procedure_stexmrst_row_count.procedure_stexmrst_row_count(config, cdm_path, source_path, excel_path, config_path, sheetname)

    ### field 품질 진단 ###
    # field_care_site_summary.care_site_field_summary(cdm_path, excel_path, sheetname_field)
    # field_provider_summary.provider_field_summary(cdm_path, excel_path, sheetname_field)
    # field_person_summary.person_field_summary(cdm_path, excel_path, sheetname_field)
    # field_visit_occurrence_summary.visit_occurrence_field_summary(cdm_path, excel_path, sheetname_field)
    # field_visit_detail_summary.visit_detail_field_summary(cdm_path, excel_path, sheetname_field)
    # field_condition_occurrence_summary.condition_occurrence_field_summary(cdm_path, excel_path, sheetname_field)
    # field_drug_exposure_summary.drug_exposure_field_summary(cdm_path, excel_path, sheetname_field)
    # field_measurement_summary.measurement_field_summary(cdm_path, excel_path, sheetname_field)
    # field_procedure_occurrence_summary.procedure_occurrence_field_summary(cdm_path, excel_path, sheetname_field)
    # field_observation_period_summary.observation_period_field_summary(cdm_path, excel_path, sheetname_field)
    field_local_edi_summary.local_edi_field_summary(cdm_path, excel_path, sheetname_field)