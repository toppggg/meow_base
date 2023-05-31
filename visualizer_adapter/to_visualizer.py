from multiprocessing import Queue
# import time

from visualizer.i_visualizer_receive_data import IVisualizerReceiveData
from visualizer.visualizer_struct import VISUALIZER_STRUCT

# from meow_base.core.vars import EVENT_RULE, EVENT_TYPE, EVENT_PATH, JOB_ID, JOB_EVENT
from meow_base.visualizer_adapter.convert_meow_to_visualizer import ConvertMeowToVisualizer
from meow_base.visualizer_adapter.vars import MONITOR, HANDLER_QUEUE, HANDLER, CONDUCTOR_QUEUE, CONDUCTOR, END


class To_Visualizer:
    visualizer_channel:Queue
    converter : ConvertMeowToVisualizer

    def __init__(self, visualizer:IVisualizerReceiveData)->None:
        self.visualizer_channel = visualizer.receive_channel 
        self.converter = ConvertMeowToVisualizer()  

    def from_monitor(self, event)->None: 
        print("to_event_queue")
        data = self.converter.event_to_base_struct(event)
        data.current_state = MONITOR
        self.visualizer_channel.put(data)

    def to_handler(self, event)->None:
        print("to_handler")
        data = self.converter.event_to_base_struct(event)
        data.previous_state = MONITOR
        data.current_state = HANDLER_QUEUE
        # data.event_time = time.time()
        self.visualizer_channel.put(data)
    
    def from_handler(self, job)->None:
        print("from_handler")
        print(job)
        data = self.converter.meow_job_to_visualizer_struct(job)
        data.previous_state = HANDLER_QUEUE
        data.current_state = HANDLER
        print(data)
        self.visualizer_channel.put(data)

    def to_conductor(self, job)->None:
        print("to_conductor")
        data = self.converter.meow_job_to_visualizer_struct(job)
        data.previous_state = HANDLER
        data.current_state = CONDUCTOR_QUEUE
        self.visualizer_channel.put(data)

    def conductor_finished(self, job)->None:
        data = self.converter.meow_job_to_visualizer_struct(job)
        data.previous_state = CONDUCTOR_QUEUE
        data.current_state = CONDUCTOR
        self.visualizer_channel.put(data)


    def debug_message(self, msg)->None:
        data = VISUALIZER_STRUCT(debug_message = msg)
        self.visualizer_channel.put(data)

    def debug_job(self, msg, job)->None:
        data = self.converter.meow_job_to_visualizer_struct(job)
        data.debug_message = msg
        self.visualizer_channel.put(data)     

    def debug_event(self, msg, event)->None:
        data = self.converter.event_to_base_struct(event)
        data.debug_message = msg
        self.visualizer_channel.put(data)
