from meow_base.core.visualizer.visualizer_struct import VISUALIZER_STRUCT
from meow_base.core.rule import Rule
from typing import List
class EventQueueData:

    event_queue: List[VISUALIZER_STRUCT]
    average_queue_time : dict[VISUALIZER_STRUCT.rule_name,float]
    
    def __init__(self) -> None:
        pass

    def rule_matched(visualizer_struct):
        #throw exception not implemented
        raise NotImplementedError("Not yet implemented.")
    
    def remove_rule (visualizer_struct):
        raise NotImplementedError("Not yet implemented.")

    def get_rules_awaiting_jobs():
        raise NotImplementedError("Not yet implemented.")
