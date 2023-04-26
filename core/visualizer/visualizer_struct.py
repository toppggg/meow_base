from dataclasses import dataclass
from  meow_base.core.vars import EVENT_RULE, EVENT_TYPE, EVENT_PATH, EVENT_ID, JOB_ID, JOB_EVENT
# Viszualizer Vars
@dataclass
class VISUALIZER_STRUCT:
    rule_name : str
    pattern_name : str
    recipe_name : str
    triggering_path : str    
    event_time : str = ""
    job_id : str= ""
    struct_id = ""
    message = ""
    # def struct_id(self): return str(self.rule_name) + str(self.triggering_path) + str(self.event_time)


    @staticmethod
    def event_to_struct (event):
        result = VISUALIZER_STRUCT(event[EVENT_RULE].name,event[EVENT_RULE].pattern.name, event[EVENT_RULE].recipe.name, event[EVENT_PATH], event[EVENT_ID])
        return result

    @staticmethod
    def job_to_struct (job) :
        event = job[JOB_EVENT]
        result = VISUALIZER_STRUCT(event.name,event[EVENT_RULE].pattern.name, event[EVENT_RULE].recipe.name, event[EVENT_PATH], event[EVENT_ID], job[JOB_ID])
        return result
