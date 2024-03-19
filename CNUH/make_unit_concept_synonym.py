import pandas as pd
import yaml
import os

def load_config(config_path):
    """
    YAML 설정 파일을 로드합니다.
    """
    with open(config_path, 'r', encoding="utf-8") as file:
        return yaml.safe_load(file)
    
unit_concept_synonym = [
	[9438, "/10^5"],
	[8549, "/10^6"],
	[8786, "/HPF"],
	[8647, "/ul"],
	[8647, "/㎕"],
	[8695, "EU/mL"],
	[8829, "EU/mL"],
	[9157, "GPL-U/ml"],
	[9157, "GPL-U/mL"],
	[8923, "IU/L"],
	[8985, "IU/mL"],
	[8985, "IU/ml"],
	[8985, "IU/㎖"],
    [8985, "_IU/mL"],
	[8529, "Index"],
	[9158, "MPL-U/mL"],
	[9158, "MPL-U/ml"],
	[9388, "PRU"],
	[8645, "U/L"],
	[8645, "U/l"],
	[8763, "U/mL"],
	[8763, "U/ml"],
	[8859, "UG/ML"],
	[8859, "ug/ml"],
    [8859, "μg/mL"],
    [8859, "㎍/mL"],
	[720869, "mL/min/1.73 m^2"],
	[8799, "copies/mL"],
	[8799, "copies/ml"],
	[8713, "g/dl"],
    [8636, "g/l"],
	[8583, "fl"],
	[8576, "mg/㎗"],
	[8840, "mg/dl"],
	[8876, "mmHg"],
	[8751, "mg/l"],
	[8587, "ml"],
    [9570, "ml/dl"],
    [8752, "mm/hr"],
    [9572, "mm²"],
    [8817, "ng/dl"],
    [8817, "ng/㎗"],
    [8842, "ng/ml"],
    [8842, "ng/㎖"],
    [9020, "ng/ml/hr"],
    [8763, "u/mL"], 
    [8763, "u/ml"],
    [8845, "pg/㎖"],
    [8523, "ratio"],
    [44777566, "score"],
    [8555, "sec"],
    [8749, "uMol/L"],
    [8749, "μmol/ℓ"],
    [8837, "ug/dl"],
    [9014, "ug/g creat"],
    [8748, "μg/L"],
    [8748, "ug/ℓ"],
    [9448, "세"]
]

config = load_config("./config.yaml")

df = pd.DataFrame(unit_concept_synonym, columns = ["concept_id", "concept_synonym_name"])
df.to_csv(os.path.join(config["CDM_path"],"concept_synonym.csv"), index = False)