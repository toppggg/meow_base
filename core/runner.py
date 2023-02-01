
"""
This file contains the defintion for the MeowRunner, the main construct used 
for actually orchestration MEOW analysis. It is intended as a modular system, 
with monitors, handlers, and conductors being swappable at initialisation.

Author(s): David Marchant
"""
import os
import sys
import threading

from inspect import signature
from multiprocessing import Pipe
from random import randrange
from typing import Any, Union

from core.correctness.vars import DEBUG_WARNING, DEBUG_INFO, EVENT_TYPE, \
    VALID_CHANNELS, JOB_TYPE, JOB_ID, META_FILE
from core.correctness.validation import setup_debugging, check_type, \
    valid_list
from core.functionality import  print_debug, wait, read_yaml
from core.meow import BaseHandler, BaseMonitor, BaseConductor


class MeowRunner:
    # A collection of all monitors in the runner
    monitors:list[BaseMonitor]
    # A collection of all handlers in the runner
    handlers:dict[str:BaseHandler]
    # A collection of all conductors in the runner
    conductors:dict[str:BaseConductor]
    # A collection of all channels from each monitor
    from_monitors: list[VALID_CHANNELS]
    # A collection of all channels from each handler
    from_handlers: list[VALID_CHANNELS]
    def __init__(self, monitors:Union[BaseMonitor,list[BaseMonitor]], 
            handlers:Union[BaseHandler,list[BaseHandler]], 
            conductors:Union[BaseConductor,list[BaseConductor]],
            print:Any=sys.stdout, logging:int=0)->None:
        """MeowRunner constructor. This connects all provided monitors, 
        handlers and conductors according to what events and jobs they produce 
        or consume."""

        self._is_valid_conductors(conductors)
        # If conductors isn't a list, make it one
        if not type(conductors) == list:
            conductors = [conductors]
        self.conductors = {}
        # Create a dictionary of conductors, keyed by job type, and valued by a
        # list of conductors for that job type
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
        # If handlers isn't a list, make it one
        if not type(handlers) == list:
            handlers = [handlers]
        self.handlers = {}
        self.from_handlers = []
        # Create a dictionary of handlers, keyed by event type, and valued by a
        # list of handlers for that event type
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
            # Create a channel from the handler back to this runner
            handler_to_runner_reader, handler_to_runner_writer = Pipe()
            handler.to_runner = handler_to_runner_writer
            self.from_handlers.append(handler_to_runner_reader)

        self._is_valid_monitors(monitors)
        # If monitors isn't a list, make it one
        if not type(monitors) == list:
            monitors = [monitors]
        self.monitors = monitors
        self.from_monitors = []
        for monitor in self.monitors:
            # Create a channel from the monitor back to this runner
            monitor_to_runner_reader, monitor_to_runner_writer = Pipe()
            monitor.to_runner = monitor_to_runner_writer
            self.from_monitors.append(monitor_to_runner_reader)

        # Create channel to send stop messages to monitor/handler thread
        self._stop_mon_han_pipe = Pipe()
        self._mon_han_worker = None

        # Create channel to send stop messages to handler/conductor thread
        self._stop_han_con_pipe = Pipe()
        self._han_con_worker = None

        # Setup debugging
        self._print_target, self.debug_level = setup_debugging(print, logging)

    def run_monitor_handler_interaction(self)->None:
        """Function to be run in its own thread, to handle any inbound messages
        from monitors. These will be events, which should be matched to an 
        appropriate handler and handled."""
        all_inputs = self.from_monitors + [self._stop_mon_han_pipe[0]]
        while True:
            ready = wait(all_inputs)

            # If we get a message from the stop channel, then finish
            if self._stop_mon_han_pipe[0] in ready:
                return
            else:
                for from_monitor in self.from_monitors:
                    if from_monitor in ready:
                        # Read event from the monitor channel
                        message = from_monitor.recv()
                        event = message
                        # Abort if we don't have a relevent handler.
                        if not self.handlers[event[EVENT_TYPE]]:
                            print_debug(self._print_target, self.debug_level, 
                                "Could not process event as no relevent "
                                f"handler for '{event[EVENT_TYPE]}'", 
                                DEBUG_INFO)
                            continue
                        # If we've only one handler, use that
                        if len(self.handlers[event[EVENT_TYPE]]) == 1:
                            handler = self.handlers[event[EVENT_TYPE]][0]
                            self.handle_event(handler, event)
                        # If multiple handlers then randomly pick one
                        else:
                            handler = self.handlers[event[EVENT_TYPE]][
                                randrange(len(self.handlers[event[EVENT_TYPE]]))
                            ]
                            self.handle_event(handler, event)

    def run_handler_conductor_interaction(self)->None:
        """Function to be run in its own thread, to handle any inbound messages
        from handlers. These will be jobs, which should be matched to an 
        appropriate conductor and executed."""
        all_inputs = self.from_handlers + [self._stop_han_con_pipe[0]]
        while True:
            ready = wait(all_inputs)

            # If we get a message from the stop channel, then finish
            if self._stop_han_con_pipe[0] in ready:
                return
            else:
                for from_handler in self.from_handlers:
                    if from_handler in ready:
                        # Read job directory from the handler channel
                        job_dir = from_handler.recv()
                        try:
                            metafile = os.path.join(job_dir, META_FILE)
                            job = read_yaml(metafile)
                        except Exception as e:
                            print_debug(self._print_target, self.debug_level, 
                                "Could not load necessary job definitions for "
                                f"job at '{job_dir}'. {e}", DEBUG_INFO)
                            continue

                        # Abort if we don't have a relevent conductor.
                        if not self.conductors[job[JOB_TYPE]]:
                            print_debug(self._print_target, self.debug_level, 
                                "Could not process job as no relevent "
                                f"conductor for '{job[JOB_TYPE]}'", DEBUG_INFO)
                            continue
                        # If we've only one conductor, use that
                        if len(self.conductors[job[JOB_TYPE]]) == 1:
                            conductor = self.conductors[job[JOB_TYPE]][0] 
                            self.execute_job(conductor, job)
                        # If multiple conductors then randomly pick one
                        else:
                            conductor = self.conductors[job[JOB_TYPE]][
                                randrange(len(self.conductors[job[JOB_TYPE]]))
                            ]
                            self.execute_job(conductor, job)
   
    def handle_event(self, handler:BaseHandler, event:dict[str:Any])->None:
        """Function for a given handler to handle a given event, without 
        crashing the runner in the event of a problem."""
        print_debug(self._print_target, self.debug_level, 
            f"Starting handling for event: '{event[EVENT_TYPE]}'", DEBUG_INFO)
        try:
            handler.handle(event)
            print_debug(self._print_target, self.debug_level, 
                f"Completed handling for event: '{event[EVENT_TYPE]}'",
                 DEBUG_INFO)
        except Exception as e:
            print_debug(self._print_target, self.debug_level, 
                "Something went wrong during handling for event "
                f"'{event[EVENT_TYPE]}'. {e}", DEBUG_INFO)

    def execute_job(self, conductor:BaseConductor, job:dict[str:Any])->None:
        """Function for a given conductor to execute a given job, without 
        crashing the runner in the event of a problem."""
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
        """Function to start the runner by starting all of the constituent 
        monitors, handlers and conductors, along with managing interaction 
        threads."""
        # Start all monitors
        for monitor in self.monitors:
            monitor.start()
        startable = []
        # Start all handlers, if they need it
        for handler_list in self.handlers.values():
            for handler in handler_list:
                if hasattr(handler, "start") and handler not in startable:
                    startable.append()
        # Start all conductors, if they need it
        for conductor_list in self.conductors.values():
            for conductor in conductor_list:
                if hasattr(conductor, "start") and conductor not in startable:
                    startable.append()
        for starting in startable:
            starting.start()
        
        # If we've not started the monitor/handler interaction thread yet, then
        # do so
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

        # If we've not started the handler/conductor interaction thread yet, 
        # then do so
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
        """Function to stop the runner by stopping all of the constituent 
        monitors, handlers and conductors, along with managing interaction 
        threads."""
        # Stop all the monitors
        for monitor in self.monitors:
            monitor.stop()

        stopable = []
        # Stop all handlers, if they need it
        for handler_list in self.handlers.values():
            for handler in handler_list:
                if hasattr(handler, "stop") and handler not in stopable:
                    stopable.append()
        # Stop all conductors, if they need it
        for conductor_list in self.conductors.values():
            for conductor in conductor_list:
                if hasattr(conductor, "stop") and conductor not in stopable:
                    stopable.append()
        for stopping in stopable:
            stopping.stop()

        # If we've started the monitor/handler interaction thread, then stop it
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

        # If we've started the handler/conductor interaction thread, then stop 
        # it
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
        """Validation check for 'monitors' variable from main constructor."""
        check_type(monitors, BaseMonitor, alt_types=[list])
        if type(monitors) == list:
            valid_list(monitors, BaseMonitor, min_length=1)

    def _is_valid_handlers(self, 
            handlers:Union[BaseHandler,list[BaseHandler]])->None:
        """Validation check for 'handlers' variable from main constructor."""
        check_type(handlers, BaseHandler, alt_types=[list])
        if type(handlers) == list:
            valid_list(handlers, BaseHandler, min_length=1)

    def _is_valid_conductors(self, 
            conductors:Union[BaseConductor,list[BaseConductor]])->None:
        """Validation check for 'conductors' variable from main constructor."""
        check_type(conductors, BaseConductor, alt_types=[list])
        if type(conductors) == list:
            valid_list(conductors, BaseConductor, min_length=1)
