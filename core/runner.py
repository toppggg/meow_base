
import sys
import threading

from inspect import signature
from multiprocessing import Pipe
from random import randrange
from typing import Any, Union

from core.correctness.vars import DEBUG_WARNING, DEBUG_INFO, EVENT_TYPE, \
    VALID_CHANNELS, JOB_TYPE, JOB_ID
from core.correctness.validation import setup_debugging, check_type, \
    valid_list
from core.functionality import  print_debug, wait
from core.meow import BaseHandler, BaseMonitor, BaseConductor


class MeowRunner:
    monitors:list[BaseMonitor]
    handlers:dict[str:BaseHandler]
    conductors:dict[str:BaseConductor]
    from_monitors: list[VALID_CHANNELS]
    from_handlers: list[VALID_CHANNELS]
    def __init__(self, monitors:Union[BaseMonitor,list[BaseMonitor]], 
            handlers:Union[BaseHandler,list[BaseHandler]], 
            conductors:Union[BaseConductor,list[BaseConductor]],
            print:Any=sys.stdout, logging:int=0)->None:

        self._is_valid_conductors(conductors)
        if not type(conductors) == list:
            conductors = [conductors]
        self.conductors = {}
        for conductor in conductors:
            conductor_jobs = conductor.valid_job_types()
            if not conductor_jobs:
                raise ValueError(
                    "Cannot start runner with conductor that does not "
                    f"implement '{BaseConductor.valid_job_types.__name__}"
                    f"({signature(BaseConductor.valid_job_types)})' and "
                    "return a list of at least one conductable job.")
            for job in conductor_jobs:
                if job in self.conductors.keys():
                    self.conductors[job].append(conductor)
                else:
                    self.conductors[job] = [conductor]

        self._is_valid_handlers(handlers)
        if not type(handlers) == list:
            handlers = [handlers]
        self.handlers = {}
        self.from_handlers = []
        for handler in handlers:
            handler_events = handler.valid_event_types()
            if not handler_events:
                raise ValueError(
                    "Cannot start runner with handler that does not "
                    f"implement '{BaseHandler.valid_event_types.__name__}"
                    f"({signature(BaseHandler.valid_event_types)})' and "
                    "return a list of at least one handlable event.")
            for event in handler_events:
                if event in self.handlers.keys():
                    self.handlers[event].append(handler)
                else:
                    self.handlers[event] = [handler]
            handler_to_runner_reader, handler_to_runner_writer = Pipe()
            handler.to_runner = handler_to_runner_writer
            self.from_handlers.append(handler_to_runner_reader)

        self._is_valid_monitors(monitors)
        if not type(monitors) == list:
            monitors = [monitors]
        self.monitors = monitors
        self.from_monitors = []
        for monitor in self.monitors:
            monitor_to_runner_reader, monitor_to_runner_writer = Pipe()
            monitor.to_runner = monitor_to_runner_writer
            self.from_monitors.append(monitor_to_runner_reader)

        self._stop_mon_han_pipe = Pipe()
        self._mon_han_worker = None

        self._stop_han_con_pipe = Pipe()
        self._han_con_worker = None

        self._print_target, self.debug_level = setup_debugging(print, logging)

    def run_monitor_handler_interaction(self)->None:
        all_inputs = self.from_monitors + [self._stop_mon_han_pipe[0]]
        while True:
            ready = wait(all_inputs)

            if self._stop_mon_han_pipe[0] in ready:
                return
            else:
                for from_monitor in self.from_monitors:
                    if from_monitor in ready:
                        message = from_monitor.recv()
                        event = message
                        if not self.handlers[event[EVENT_TYPE]]:
                            print_debug(self._print_target, self.debug_level, 
                                "Could not process event as no relevent "
                                f"handler for '{event[EVENT_TYPE]}'", 
                                DEBUG_INFO)
                            return
                        if len(self.handlers[event[EVENT_TYPE]]) == 1:
                            self.handlers[event[EVENT_TYPE]][0].handle(event)
                        else:
                            self.handlers[event[EVENT_TYPE]][
                                randrange(len(self.handlers[event[EVENT_TYPE]]))
                            ].handle(event)

    def run_handler_conductor_interaction(self)->None:
        all_inputs = self.from_handlers + [self._stop_han_con_pipe[0]]
        while True:
            ready = wait(all_inputs)

            if self._stop_han_con_pipe[0] in ready:
                return
            else:
                for from_handler in self.from_handlers:
                    if from_handler in ready:
                        message = from_handler.recv()
                        job = message
                        if not self.conductors[job[JOB_TYPE]]:
                            print_debug(self._print_target, self.debug_level, 
                                "Could not process job as no relevent "
                                f"conductor for '{job[JOB_TYPE]}'", DEBUG_INFO)
                            return
                        if len(self.conductors[job[JOB_TYPE]]) == 1:
                            conductor = self.conductors[job[JOB_TYPE]][0] 
                            self.execute_job(conductor, job)
                        else:
                            conductor = self.conductors[job[JOB_TYPE]][
                                randrange(len(self.conductors[job[JOB_TYPE]]))
                            ]
                            self.execute_job(conductor, job)

    def execute_job(self, conductor:BaseConductor, job:dict[str:Any])->None:
        print_debug(self._print_target, self.debug_level, 
            f"Starting execution for job: '{job[JOB_ID]}'", DEBUG_INFO)
        try:
            conductor.execute(job)
            print_debug(self._print_target, self.debug_level, 
                f"Completed execution for job: '{job[JOB_ID]}'", DEBUG_INFO)
        except Exception as e:
            print_debug(self._print_target, self.debug_level, 
                "Something went wrong during execution for job "
                f"'{job[JOB_ID]}'. {e}", DEBUG_INFO)

    def start(self)->None:
        for monitor in self.monitors:
            monitor.start()
        startable = []
        for handler_list in self.handlers.values():
            for handler in handler_list:
                if hasattr(handler, "start") and handler not in startable:
                    startable.append()
        for conductor_list in self.conductors.values():
            for conductor in conductor_list:
                if hasattr(conductor, "start") and conductor not in startable:
                    startable.append()
        for starting in startable:
            starting.start()
            
        if self._mon_han_worker is None:
            self._mon_han_worker = threading.Thread(
                target=self.run_monitor_handler_interaction,
                args=[])
            self._mon_han_worker.daemon = True
            self._mon_han_worker.start()
            print_debug(self._print_target, self.debug_level, 
                "Starting MeowRunner event handling...", DEBUG_INFO)
        else:
            msg = "Repeated calls to start MeowRunner event handling have " \
                "no effect."
            print_debug(self._print_target, self.debug_level, 
                msg, DEBUG_WARNING)
            raise RuntimeWarning(msg)

        if self._han_con_worker is None:
            self._han_con_worker = threading.Thread(
                target=self.run_handler_conductor_interaction,
                args=[])
            self._han_con_worker.daemon = True
            self._han_con_worker.start()
            print_debug(self._print_target, self.debug_level, 
                "Starting MeowRunner job conducting...", DEBUG_INFO)
        else:
            msg = "Repeated calls to start MeowRunner job conducting have " \
                "no effect."
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
        for conductor_list in self.conductors.values():
            for conductor in conductor_list:
                if hasattr(conductor, "stop") and conductor not in stopable:
                    stopable.append()
        for stopping in stopable:
            stopping.stop()

        if self._mon_han_worker is None:
            msg = "Cannot stop event handling thread that is not started."
            print_debug(self._print_target, self.debug_level, 
                msg, DEBUG_WARNING)
            raise RuntimeWarning(msg)
        else:
            self._stop_mon_han_pipe[1].send(1)
            self._mon_han_worker.join()
        print_debug(self._print_target, self.debug_level, 
            "Event handler thread stopped", DEBUG_INFO)

        if self._han_con_worker is None:
            msg = "Cannot stop job conducting thread that is not started."
            print_debug(self._print_target, self.debug_level, 
                msg, DEBUG_WARNING)
            raise RuntimeWarning(msg)
        else:
            self._stop_han_con_pipe[1].send(1)
            self._han_con_worker.join()
        print_debug(self._print_target, self.debug_level, 
            "Job conductor thread stopped", DEBUG_INFO)

    def _is_valid_monitors(self, 
            monitors:Union[BaseMonitor,list[BaseMonitor]])->None:
        check_type(monitors, BaseMonitor, alt_types=[list])
        if type(monitors) == list:
            valid_list(monitors, BaseMonitor, min_length=1)

    def _is_valid_handlers(self, 
            handlers:Union[BaseHandler,list[BaseHandler]])->None:
        check_type(handlers, BaseHandler, alt_types=[list])
        if type(handlers) == list:
            valid_list(handlers, BaseHandler, min_length=1)

    def _is_valid_conductors(self, 
            conductors:Union[BaseConductor,list[BaseConductor]])->None:
        check_type(conductors, BaseConductor, alt_types=[list])
        if type(conductors) == list:
            valid_list(conductors, BaseConductor, min_length=1)
