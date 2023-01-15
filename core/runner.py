
import sys
import threading

from multiprocessing import Pipe
from random import randrange
from typing import Any, Union

from core.correctness.vars import DEBUG_WARNING, DEBUG_INFO, EVENT_TYPE, \
    VALID_CHANNELS
from core.correctness.validation import setup_debugging, check_type, \
    valid_list
from core.functionality import  print_debug, wait
from core.meow import BaseHandler, BaseMonitor


class MeowRunner:
    monitors:list[BaseMonitor]
    handlers:dict[str:BaseHandler]
    from_monitor: list[VALID_CHANNELS]
    def __init__(self, monitors:Union[BaseMonitor,list[BaseMonitor]], 
            handlers:Union[BaseHandler,list[BaseHandler]], 
            print:Any=sys.stdout, logging:int=0) -> None:
        self._is_valid_handlers(handlers)
        if not type(handlers) == list:
            handlers = [handlers]
        self.handlers = {}
        for handler in handlers:
            handler_events = handler.valid_event_types()
            for event in handler_events:
                if event in self.handlers.keys():
                    self.handlers[event].append(handler)
                else:
                    self.handlers[event] = [handler]

        self._is_valid_monitors(monitors)
        if not type(monitors) == list:
            monitors = [monitors]
        self.monitors = monitors
        self.from_monitors = []
        for monitor in self.monitors:
            monitor_to_runner_reader, monitor_to_runner_writer = Pipe()
            monitor.to_runner = monitor_to_runner_writer
            self.from_monitors.append(monitor_to_runner_reader)

        self._stop_pipe = Pipe()
        self._worker = None
        self._print_target, self.debug_level = setup_debugging(print, logging)

    def run(self)->None:
        all_inputs = self.from_monitors + [self._stop_pipe[0]]
        while True:
            ready = wait(all_inputs)

            if self._stop_pipe[0] in ready:
                return
            else:
                for from_monitor in self.from_monitors:
                    if from_monitor in ready:
                        message = from_monitor.recv()
                        event = message
                        if not self.handlers[event[EVENT_TYPE]]:
                            print_debug(self._print_target, self.debug_level, 
                                "Could not process event as no relevent "
                                f"handler for '{EVENT_TYPE}'", DEBUG_INFO)
                            return
                        if len(self.handlers[event[EVENT_TYPE]]) == 1:
                            self.handlers[event[EVENT_TYPE]][0].handle(event)
                        else:
                            self.handlers[event[EVENT_TYPE]][
                                randrange(len(self.handlers[event[EVENT_TYPE]]))
                            ].handle(event)

    def start(self)->None:
        for monitor in self.monitors:
            monitor.start()
        startable = []
        for handler_list in self.handlers.values():
            for handler in handler_list:
                if hasattr(handler, "start") and handler not in startable:
                    startable.append()
        for handler in startable:
            handler.start()

        if self._worker is None:
            self._worker = threading.Thread(
                target=self.run,
                args=[])
            self._worker.daemon = True
            self._worker.start()
            print_debug(self._print_target, self.debug_level, 
                "Starting MeowRunner run...", DEBUG_INFO)
        else:
            msg = "Repeated calls to start have no effect."
            print_debug(self._print_target, self.debug_level, 
                msg, DEBUG_WARNING)
            raise RuntimeWarning(msg)


    def stop(self)->None:
        for monitor in self.monitors:
            monitor.stop()

        stopable = []
        for handler_list in self.handlers.values():
            for handler in handler_list:
                if hasattr(handler, "stop") and handler not in stopable:
                    stopable.append()
        for handler in stopable:
            handler.stop()

        if self._worker is None:
            msg = "Cannot stop thread that is not started."
            print_debug(self._print_target, self.debug_level, 
                msg, DEBUG_WARNING)
            raise RuntimeWarning(msg)
        else:
            self._stop_pipe[1].send(1)
            self._worker.join()
        print_debug(self._print_target, self.debug_level, 
            "Worker thread stopped", DEBUG_INFO)


    def _is_valid_monitors(self, 
            monitors:Union[BaseMonitor,list[BaseMonitor]])->None:
        check_type(monitors, BaseMonitor, alt_types=[list[BaseMonitor]])
        if type(monitors) == list:
            valid_list(monitors, BaseMonitor, min_length=1)

    def _is_valid_handlers(self, 
            handlers:Union[BaseHandler,list[BaseHandler]])->None:
        check_type(handlers, BaseHandler, alt_types=[list[BaseHandler]])
        if type(handlers) == list:
            valid_list(handlers, BaseHandler, min_length=1)
