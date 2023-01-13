
import threading
import sys
import os

from fnmatch import translate
from re import match
from time import time, sleep
from typing import Any
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

from core.correctness.validation import check_type, valid_string, \
    valid_dict, valid_list, valid_path, valid_existing_dir_path, \
    setup_debugging
from core.correctness.vars import VALID_RECIPE_NAME_CHARS, \
    VALID_VARIABLE_NAME_CHARS, FILE_EVENTS, FILE_CREATE_EVENT, \
    FILE_MODIFY_EVENT, FILE_MOVED_EVENT, VALID_CHANNELS, DEBUG_INFO, \
    DEBUG_ERROR, DEBUG_WARNING, WATCHDOG_TYPE, WATCHDOG_SRC, WATCHDOG_RULE, \
    WATCHDOG_BASE
from core.functionality import print_debug, create_event
from core.meow import BasePattern, BaseMonitor, BaseRule

_DEFAULT_MASK = [
    FILE_CREATE_EVENT,
    FILE_MODIFY_EVENT,
    FILE_MOVED_EVENT
]

SWEEP_START = "start"
SWEEP_STOP = "stop"
SWEEP_JUMP = "jump"

class FileEventPattern(BasePattern):
    triggering_path:str
    triggering_file:str
    event_mask:list[str]
    sweep:dict[str,Any]

    def __init__(self, name:str, triggering_path:str, recipe:str, 
            triggering_file:str, event_mask:list[str]=_DEFAULT_MASK, 
            parameters:dict[str,Any]={}, outputs:dict[str,Any]={}, 
            sweep:dict[str,Any]={}):
        super().__init__(name, recipe, parameters, outputs)
        self._is_valid_triggering_path(triggering_path)
        self.triggering_path = triggering_path
        self._is_valid_triggering_file(triggering_file)
        self.triggering_file = triggering_file
        self._is_valid_event_mask(event_mask)
        self.event_mask = event_mask
        self._is_valid_sweep(sweep)
        self.sweep = sweep

    def _is_valid_recipe(self, recipe:str)->None:
        valid_string(recipe, VALID_RECIPE_NAME_CHARS)

    def _is_valid_triggering_path(self, triggering_path:str)->None:
        valid_path(triggering_path)
        if len(triggering_path) < 1:
            raise ValueError (
                f"triggiering path '{triggering_path}' is too short. " 
                "Minimum length is 1"
        )

    def _is_valid_triggering_file(self, triggering_file:str)->None:
        valid_string(triggering_file, VALID_VARIABLE_NAME_CHARS)

    def _is_valid_parameters(self, parameters:dict[str,Any])->None:
        valid_dict(parameters, str, Any, strict=False, min_length=0)
        for k in parameters.keys():
            valid_string(k, VALID_VARIABLE_NAME_CHARS)

    def _is_valid_output(self, outputs:dict[str,str])->None:
        valid_dict(outputs, str, str, strict=False, min_length=0)
        for k in outputs.keys():
            valid_string(k, VALID_VARIABLE_NAME_CHARS)

    def _is_valid_event_mask(self, event_mask)->None:
        valid_list(event_mask, str, min_length=1)
        for mask in event_mask:
            if mask not in FILE_EVENTS:
                raise ValueError(f"Invalid event mask '{mask}'. Valid are: "
                    f"{FILE_EVENTS}")

    def _is_valid_sweep(self, sweep)->None:
        check_type(sweep, dict)
        if not sweep:
            return
        for k, v in sweep.items():
            valid_dict(
                v, str, Any, [
                    SWEEP_START, SWEEP_STOP, SWEEP_JUMP
                ], strict=True)

            check_type(
                v[SWEEP_START], expected_type=int, alt_types=[float, complex])
            check_type(
                v[SWEEP_STOP], expected_type=int, alt_types=[float, complex])
            check_type(
                v[SWEEP_JUMP], expected_type=int, alt_types=[float, complex])
            # Try to check that this loop is not infinite
            if v[SWEEP_JUMP] == 0:
                raise ValueError(
                    f"Cannot create sweep with a '{SWEEP_JUMP}' value of zero"
                )
            elif v[SWEEP_JUMP] > 0:
                if not v[SWEEP_STOP] > v[SWEEP_START]:
                    raise ValueError(
                        "Cannot create sweep with a positive '{SWEEP_JUMP}' "
                        "value where the end point is smaller than the start."
                    )
            elif v[SWEEP_JUMP] < 0:
                if not v[SWEEP_STOP] < v[SWEEP_START]:
                    raise ValueError(
                        "Cannot create sweep with a negative '{SWEEP_JUMP}' "
                        "value where the end point is smaller than the start."
                    )


class WatchdogMonitor(BaseMonitor):
    event_handler:PatternMatchingEventHandler
    monitor:Observer
    base_dir:str
    debug_level:int
    _print_target:Any
    _rules_lock:threading.Lock

    def __init__(self, base_dir:str, rules:dict[str, BaseRule], 
            autostart=False, settletime:int=1, print:Any=sys.stdout, 
            logging:int=0)->None:
        super().__init__(rules)
        self._is_valid_base_dir(base_dir)
        self.base_dir = base_dir
        check_type(settletime, int)
        self._print_target, self.debug_level = setup_debugging(print, logging)       
        self._rules_lock = threading.Lock()
        self.event_handler = WatchdogEventHandler(self, settletime=settletime)
        self.monitor = Observer()
        self.monitor.schedule(
            self.event_handler,
            self.base_dir,
            recursive=True
        )
        print_debug(self._print_target, self.debug_level, 
            "Created new WatchdogMonitor instance", DEBUG_INFO)

        if autostart:
            self.start()

    def start(self)->None:
        print_debug(self._print_target, self.debug_level, 
            "Starting WatchdogMonitor", DEBUG_INFO)
        self.monitor.start()

    def stop(self)->None:
        print_debug(self._print_target, self.debug_level, 
            "Stopping WatchdogMonitor", DEBUG_INFO)
        self.monitor.stop()

    def match(self, event)->None:
        src_path = event.src_path
        event_type = "dir_"+ event.event_type if event.is_directory \
            else "file_" + event.event_type

        handle_path = src_path.replace(self.base_dir, '', 1)
        while handle_path.startswith(os.path.sep):
            handle_path = handle_path[1:]

        self._rules_lock.acquire()
        try:
            for rule in self.rules.values():
                
                if event_type not in rule.pattern.event_mask:
                    continue
                                
                target_path = rule.pattern.triggering_path
                recursive_regexp = translate(target_path)
                direct_regexp = recursive_regexp.replace('.*', '[^/]*')
                recursive_hit = match(recursive_regexp, handle_path)
                direct_hit = match(direct_regexp, handle_path)

                if direct_hit or recursive_hit:
                    meow_event = create_event(
                        WATCHDOG_TYPE, {
                            WATCHDOG_SRC: event.src_path,
                            WATCHDOG_BASE: self.base_dir,
                            WATCHDOG_RULE: rule
                    })
                    print_debug(self._print_target, self.debug_level,  
                        f"Event at {src_path} of type {event_type} hit rule "
                        f"{rule.name}", DEBUG_INFO)
                    self.to_runner.send(meow_event)

        except Exception as e:
            self._rules_lock.release()
            raise Exception(e)            

        self._rules_lock.release()

    def _is_valid_base_dir(self, base_dir:str)->None:
        valid_existing_dir_path(base_dir)

    def _is_valid_rules(self, rules:dict[str, BaseRule])->None:
        valid_dict(rules, str, BaseRule, min_length=0, strict=False)


class WatchdogEventHandler(PatternMatchingEventHandler):
    monitor:WatchdogMonitor
    _settletime:int
    _recent_jobs:dict[str, Any]
    _recent_jobs_lock:threading.Lock
    def __init__(self, monitor:WatchdogMonitor, settletime:int=1):
        super().__init__()
        self.monitor = monitor
        self._settletime = settletime
        self._recent_jobs = {}
        self._recent_jobs_lock = threading.Lock()

    def threaded_handler(self, event):
        self._recent_jobs_lock.acquire()
        try:
            if event.src_path in self._recent_jobs:
                recent_timestamp = self._recent_jobs[event.src_path]
                difference = event.time_stamp - recent_timestamp

                if difference <= self._settletime:
                    self._recent_jobs[event.src_path] = \
                        max(recent_timestamp, event.time_stamp)
                    self._recent_jobs_lock.release()
                    return
                else:
                    self._recent_jobs[event.src_path] = event.time_stamp
            else:
                self._recent_jobs[event.src_path] = event.time_stamp
        except Exception as ex:
            self._recent_jobs_lock.release()
            raise Exception(ex)
        self._recent_jobs_lock.release()

        self.monitor.match(event)

    def handle_event(self, event):
        event.time_stamp = time()

        waiting_for_threaded_resources = True
        while waiting_for_threaded_resources:
            try:
                worker = threading.Thread(
                    target=self.threaded_handler,
                    args=[event])
                worker.daemon = True
                worker.start()
                waiting_for_threaded_resources = False
            except threading.ThreadError:
                sleep(1)
    
    def on_created(self, event):
        self.handle_event(event)

    def on_modified(self, event):
        self.handle_event(event)

    def on_moved(self, event):
        self.handle_event(event)

    def on_deleted(self, event):
        self.handle_event(event)
    
    def on_closed(self, event):
        self.handle_event(event)
