
import threading
import os

from fnmatch import translate
from re import match
from time import time, sleep
from typing import Any
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler, FileCreatedEvent, \
    FileModifiedEvent, FileMovedEvent, FileClosedEvent, FileDeletedEvent, \
    DirCreatedEvent, DirDeletedEvent, DirModifiedEvent, DirMovedEvent

from core.correctness.validation import check_type, valid_string, \
    valid_dict, valid_list, valid_path
from core.correctness.vars import VALID_RECIPE_NAME_CHARS, \
    VALID_VARIABLE_NAME_CHARS, FILE_EVENTS, FILE_CREATE_EVENT, \
    FILE_MODIFY_EVENT, FILE_MOVED_EVENT, FILE_CLOSED_EVENT, \
    FILE_DELETED_EVENT, DIR_CREATE_EVENT, DIR_DELETED_EVENT, \
    DIR_MODIFY_EVENT, DIR_MOVED_EVENT, VALID_CHANNELS
from core.meow import BasePattern, BaseMonitor, BaseRule

_EVENT_TRANSLATIONS = {
    FileCreatedEvent: FILE_CREATE_EVENT,
    FileModifiedEvent: FILE_MODIFY_EVENT,
    FileMovedEvent: FILE_MOVED_EVENT,
    FileClosedEvent: FILE_CLOSED_EVENT,
    FileDeletedEvent: FILE_DELETED_EVENT,
    DirCreatedEvent: DIR_CREATE_EVENT,
    DirDeletedEvent: DIR_DELETED_EVENT,
    DirModifiedEvent: DIR_MODIFY_EVENT,
    DirMovedEvent: DIR_MOVED_EVENT
}

class FileEventPattern(BasePattern):
    triggering_path:str
    triggering_file:str
    event_mask:list[str]

    def __init__(self, name:str, triggering_path:str, recipe:str, 
            triggering_file:str, event_mask:list[str]=FILE_EVENTS, 
            parameters:dict[str,Any]={}, outputs:dict[str,Any]={}):
        super().__init__(name, recipe, parameters, outputs)
        self._is_valid_triggering_path(triggering_path)
        self.triggering_path = triggering_path
        self._is_valid_triggering_file(triggering_file)
        self.triggering_file = triggering_file
        self._is_valid_event_mask(event_mask)
        self.event_mask = event_mask

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


class WatchdogMonitor(BaseMonitor):
    event_handler:PatternMatchingEventHandler
    monitor:Observer
    base_dir:str
    _rules_lock:threading.Lock

    def __init__(self, base_dir:str, rules:dict[str, BaseRule], 
            report:VALID_CHANNELS, autostart=False, 
            settletime:int=1)->None:
        super().__init__(rules, report)
        self._is_valid_base_dir(base_dir)
        self.base_dir = base_dir
        check_type(settletime, int)
        self._rules_lock = threading.Lock()
        self.event_handler = WatchdogEventHandler(self, settletime=settletime)
        self.monitor = Observer()
        self.monitor.schedule(
            self.event_handler,
            self.base_dir,
            recursive=True
        )

        if autostart:
            self.start()

    def start(self)->None:
        self.monitor.start()

    def stop(self)->None:
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
                    self.report.send((event, rule))

        except Exception as e:
            self._rules_lock.release()
            raise Exception(e)            

        self._rules_lock.release()


    def _is_valid_base_dir(self, base_dir:str)->None:
        valid_path(base_dir)

    def _is_valid_report(self, report:VALID_CHANNELS)->None:
        check_type(report, VALID_CHANNELS)

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
