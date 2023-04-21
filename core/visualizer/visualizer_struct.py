from dataclasses import dataclass
# Viszualizer Vars
@dataclass
class VISUALIZER_STRUCT:
    rule_name : str
    pattern_name : str
    recipe_name : str
    triggering_path : str    
    event_id : str = ""
    job_id : str= ""
    vs_id : str = ""
    