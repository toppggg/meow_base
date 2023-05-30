from multiprocessing import Queue
# import time

from visualizer.i_visualizer_receive_data import IVisualizerReceiveData
from visualizer.visualizer_struct import VISUALIZER_STRUCT

# from meow_base.core.vars import EVENT_RULE, EVENT_TYPE, EVENT_PATH, JOB_ID, JOB_EVENT
from meow_base.visualizer_adapter.convert_meow_to_visualizer import ConvertMeowToVisualizer
from meow_base.visualizer_adapter.vars import MONITOR, HANDLER_QUEUE, HANDLER, CONDUCTOR_QUEUE, CONDUCTOR, END


class To_Visualizer:
    visualizer_channel:Queue

    def __init__(self, visualizer:IVisualizerReceiveData)->None:
        self.visualizer_channel = visualizer.receive_channel   

    def to_event_queue(self, event)->None: 
        print("to_event_queue")
        data = ConvertMeowToVisualizer.event_to_base_struct(event)
        data.current_state = MONITOR
        self.visualizer_channel.put(data)

    def to_handler(self, event)->None:
        print("to_handler")
        data = ConvertMeowToVisualizer.event_to_base_struct(event)
        data.previous_state = MONITOR
        data.current_state = HANDLER_QUEUE
        # data.event_time = time.time()
        self.visualizer_channel.put(data)
    
    def to_job_queue(self, job)->None:
        print("to_job_queue")
        data.previous_state = HANDLER_QUEUE
        data = ConvertMeowToVisualizer.meow_job_to_visualizer_struct(job)
        self.visualizer_channel.put(data)
        data.current_state = HANDLER

    def to_conductor(self, job)->None:
        data = ConvertMeowToVisualizer.meow_job_to_visualizer_struct(job)
        data.previous_state = HANDLER
        data.current_state = CONDUCTOR_QUEUE
        self.visualizer_channel.put(data)
        print("to_conductor")

    def conductor_finished(self, job)->None:
        data = ConvertMeowToVisualizer.meow_job_to_visualizer_struct(job)
        data.previous_state = CONDUCTOR_QUEUE
        data.current_state = CONDUCTOR
        self.visualizer_channel.put(data)


    def debug_message(self, msg)->None:
        data =VISUALIZER_STRUCT.debug_message = msg
        self.visualizer_channel.put(data)

    def debug_job(self, msg, job)->None:
        data = ConvertMeowToVisualizer.meow_job_to_visualizer_struct(job)
        data.debug_message = msg
        self.visualizer_channel.put(data)     

    def debug_event(self, msg, event)->None:
        data = ConvertMeowToVisualizer.event_to_base_struct(event)
        data.debug_message = msg
        self.visualizer_channel.put(data)
