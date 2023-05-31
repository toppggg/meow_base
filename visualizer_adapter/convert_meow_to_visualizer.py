import time

from visualizer.visualizer_struct import VISUALIZER_STRUCT 
from meow_base.core.vars import EVENT_RULE, EVENT_TYPE, EVENT_PATH, JOB_ID, JOB_EVENT, EVENT_TIME
# from meow_base.visualizer_adapter.vars import MONITOR, HANDLER_QUEUE, HANDLER, CONDUCTOR_QUEUE, CONDUCTOR, END

class ConvertMeowToVisualizer:

    def __init__(self) -> None:
        pass

    def event_to_base_struct (self, event) -> VISUALIZER_STRUCT :
        event_type = event[EVENT_RULE].name
        event_id = event[EVENT_RULE].name + event[EVENT_PATH] + str(event[EVENT_TIME])
        event_origin_time = event[EVENT_TIME]
        event_time = str(time.time())
        result = VISUALIZER_STRUCT(event_type = event_type,event_id = event_id, event_origin_time = event_origin_time, event_time = event_time) 
        return result
    
    def meow_job_to_visualizer_struct (self, job) -> VISUALIZER_STRUCT :
        event = job[JOB_EVENT]
        # print(event)
        result = self.event_to_base_struct(event)
        # print(result)
        # print("25")
        result.optional_info = job #this is too much info, meow should be queried for it, not the visualizer?
        result.event_origin_time = event[EVENT_TIME] # Is this set by meow?
        result.event_time = str(time.time())
        return result