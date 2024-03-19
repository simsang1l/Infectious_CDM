from DataTransformer import *
import logging

if __name__ == "__main__":
    config = "config.yaml"

    start_time = datetime.now()
    try : 
        care_site = CareSiteTransformer(config)    
        care_site.transform()

        provider = ProviderTransformer(config)    
        provider.transform()

        person = PersonTransformer(config)    
        person.transform()

        visit_occurrence = VisitOccurrenceTransformer(config)    
        visit_occurrence.transform()

        condition_occurrence = ConditionOccurrenceTransformer(config)    
        condition_occurrence.transform()

        drug_edi = DrugEDITransformer(config)
        drug_edi.transform()

        drug_exposure = DrugexposureTransformer(config)    
        drug_exposure.transform()

        local_edi = LocalEDITransformer(config)    
        local_edi.transform()

        measurement_diag = MeasurementDiagTransformer(config)
        measurement_diag.transform()

        measurement_pth = MeasurementpthTransformer(config)
        measurement_pth.transform()

        measurement_bmi = MeasurementVSTransformer(config)
        measurement_bmi.transform()

        merge_measurement = MergeMeasurementTransformer(config)
        merge_measurement.transform()

        procedure = ProcedureTransformer(config)
        procedure.transform()

        observation_period = ObservationPeriodTransformer(config)
        observation_period.transform()
        
        
    except Exception as e :
        logging.error(f"Exucution failed: {e}", exc_info=True)
    
    logging.info(f"transform and Load end, elapsed_time is : {datetime.now() - start_time}")