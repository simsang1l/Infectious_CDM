from DataTransformer import *
import logging

if __name__ == "__main__":
    config = "config.yaml"

    start_time = datetime.now()
    try : 
        # care_site = CareSiteTransformer(config)    
        # care_site.transform()

        # provider = ProviderTransformer(config)    
        # provider.transform()

        # person = PersonTransformer(config)    
        # person.transform()

        # visit_occurrence = VisitOccurrenceTransformer(config)    
        # visit_occurrence.transform()

        # visit_detail = VisitDetailTransformer(config)    
        # visit_detail.transform()
        
        # local_kcd = LocalKCDTransformer(config)    
        # local_kcd.transform()

        # condition_occurrence = ConditionOccurrenceTransformer(config)    
        # condition_occurrence.transform()

        # local_edi = LocalEDITransformer(config)    
        # local_edi.transform()
        
        # drug_exposure = DrugexposureTransformer(config)    
        # drug_exposure.transform()

        measurement_stresult = MeasurementTransformer(config)
        measurement_stresult.transform()                   

        # procedure = ProcedureTransformer(config)
        # procedure.transform()

        # observation_period = ObservationPeriodTransformer(config)
        # observation_period.transform()
        
        
    except Exception as e :
        logging.error(f"Exucution failed: {e}", exc_info=True)
    
    logging.info(f"transform and Load end, elapsed_time is : {datetime.now() - start_time}")