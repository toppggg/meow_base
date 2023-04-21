from meow_base.core.visualizer.visualizer import Visualizer
from meow_base.core.visualizer.visualizer_struct import VISUALIZER_STRUCT
from multiprocessing import Pipe
from multiprocessing.connection import Connection
from  meow_base.core.vars import EVENT_RULE, EVENT_TYPE, EVENT_PATH, EVENT_ID, JOB_ID, JOB_EVENT, \
    TO_EVENT_QUEUE, TO_HANDLER, TO_JOB_QUEUE, TO_CONDUCTOR

class To_Visualizer:

    visualizer_channel:Connection
    def __init__(self, visualizer:Visualizer)->None:
        reader, self.visualizer_channel = Pipe(False)
        visualizer.receive_channel = reader

    def to_event_queue(self, event):
        data = self.event_to_struct(event)
        self.visualizer_channel.send((TO_EVENT_QUEUE,data))

    def to_handler(self, event):
        data = self.event_to_struct(event)
        self.visualizer_channel.send((TO_HANDLER,data))

    def to_job_queue(self, job):
        data = self.job_to_struct(job)
        self.visualizer_channel.send((TO_JOB_QUEUE,data))
        
    def to_conductor(self, job):
        data = self.job_to_struct(job)
        self.visualizer_channel.send((TO_CONDUCTOR,data))

    def event_to_struct (event) -> VISUALIZER_STRUCT :
        result = VISUALIZER_STRUCT(event[EVENT_RULE].name,event[EVENT_RULE].pattern.name, event[EVENT_RULE].recipe.name, event[EVENT_PATH], event[EVENT_ID])
        return result

    def job_to_struct (job) -> VISUALIZER_STRUCT :
        event = job[JOB_EVENT]
        result = VISUALIZER_STRUCT(event.name,event[EVENT_RULE].pattern.name, event[EVENT_RULE].recipe.name, event[EVENT_PATH], event[EVENT_ID], job[JOB_ID])
        return result