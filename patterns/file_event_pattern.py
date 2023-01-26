
import glob
import threading
import sys
import os

from copy import deepcopy
from fnmatch import translate
from re import match
from time import time, sleep
from typing import Any, Union
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

from core.correctness.validation import check_type, valid_string, \
    valid_dict, valid_list, valid_path, valid_existing_dir_path, \
    setup_debugging
from core.correctness.vars import VALID_RECIPE_NAME_CHARS, \
    VALID_VARIABLE_NAME_CHARS, FILE_EVENTS, FILE_CREATE_EVENT, \
    FILE_MODIFY_EVENT, FILE_MOVED_EVENT, DEBUG_INFO, WATCHDOG_TYPE, \
    WATCHDOG_RULE, WATCHDOG_BASE, FILE_RETROACTIVE_EVENT, WATCHDOG_HASH, SHA256
from core.functionality import print_debug, create_event, get_file_hash
from core.meow import BasePattern, BaseMonitor, BaseRule, BaseRecipe, \
    create_rule

_DEFAULT_MASK = [
    FILE_CREATE_EVENT,
    FILE_MODIFY_EVENT,
    FILE_MOVED_EVENT,
    FILE_RETROACTIVE_EVENT
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
    _patterns_lock:threading.Lock
    _recipes_lock:threading.Lock
    _rules_lock:threading.Lock

    def __init__(self, base_dir:str, patterns:dict[str,FileEventPattern], 
            recipes:dict[str,BaseRecipe], autostart=False, settletime:int=1, 
            print:Any=sys.stdout, logging:int=0)->None:
        super().__init__(patterns, recipes)
        self._is_valid_base_dir(base_dir)
        self.base_dir = base_dir
        check_type(settletime, int)
        self._print_target, self.debug_level = setup_debugging(print, logging)       
        self._patterns_lock = threading.Lock()
        self._recipes_lock = threading.Lock()
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
        self._apply_retroactive_rules()
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
            for rule in self._rules.values():
                
                if event_type not in rule.pattern.event_mask:
                    continue
                                
                target_path = rule.pattern.triggering_path
                recursive_regexp = translate(target_path)
                direct_regexp = recursive_regexp.replace('.*', '[^/]*')
                recursive_hit = match(recursive_regexp, handle_path)
                direct_hit = match(direct_regexp, handle_path)

                if direct_hit or recursive_hit:
                    meow_event = create_event(
                        WATCHDOG_TYPE, 
                        event.src_path, 
                        { 
                            WATCHDOG_BASE: self.base_dir, 
                            WATCHDOG_RULE: rule, 
                            WATCHDOG_HASH: get_file_hash(
                                event.src_path, 
                                SHA256
                            ) 
                        }
                    )
                    print_debug(self._print_target, self.debug_level,  
                        f"Event at {src_path} of type {event_type} hit rule "
                        f"{rule.name}", DEBUG_INFO)
                    self.to_runner.send(meow_event)

        except Exception as e:
            self._rules_lock.release()
            raise e

        self._rules_lock.release()

    def add_pattern(self, pattern:FileEventPattern)->None:
        check_type(pattern, FileEventPattern)
        self._patterns_lock.acquire()
        try:
            if pattern.name in self._patterns:
                raise KeyError(f"An entry for Pattern '{pattern.name}' "
                    "already exists. Do you intend to update instead?")
            self._patterns[pattern.name] = pattern
        except Exception as e:
            self._patterns_lock.release()
            raise e            
        self._patterns_lock.release()

        self._identify_new_rules(new_pattern=pattern)

    def update_pattern(self, pattern:FileEventPattern)->None:
        check_type(pattern, FileEventPattern)
        self.remove_pattern(pattern.name)
        print(f"adding pattern w/ recipe {pattern.recipe}")
        self.add_pattern(pattern)

    def remove_pattern(self, pattern: Union[str,FileEventPattern])->None:
        check_type(pattern, str, alt_types=[FileEventPattern])
        lookup_key = pattern
        if isinstance(lookup_key, FileEventPattern):
            lookup_key = pattern.name
        self._patterns_lock.acquire()
        try:
            if lookup_key not in self._patterns:
                raise KeyError(f"Cannot remote Pattern '{lookup_key}' as it "
                    "does not already exist")
            self._patterns.pop(lookup_key)
        except Exception as e:
            self._patterns_lock.release()
            raise e 
        self._patterns_lock.release()

        if isinstance(pattern, FileEventPattern):
            self._identify_lost_rules(lost_pattern=pattern.name)
        else:
            self._identify_lost_rules(lost_pattern=pattern)
        
    def get_patterns(self)->None:
        to_return = {}
        self._patterns_lock.acquire()
        try:
            to_return = deepcopy(self._patterns)
        except Exception as e:
            self._patterns_lock.release()
            raise e
        self._patterns_lock.release()
        return to_return

    def add_recipe(self, recipe: BaseRecipe)->None:
        check_type(recipe, BaseRecipe)
        self._recipes_lock.acquire()
        try:
            if recipe.name in self._recipes:
                raise KeyError(f"An entry for Recipe '{recipe.name}' already "
                    "exists. Do you intend to update instead?")
            self._recipes[recipe.name] = recipe
        except Exception as e:
            self._recipes_lock.release()
            raise e
        self._recipes_lock.release()

        self._identify_new_rules(new_recipe=recipe)

    def update_recipe(self, recipe: BaseRecipe)->None:
        check_type(recipe, BaseRecipe)
        self.remove_recipe(recipe.name)
        self.add_recipe(recipe)
    
    def remove_recipe(self, recipe:Union[str,BaseRecipe])->None:
        check_type(recipe, str, alt_types=[BaseRecipe])
        lookup_key = recipe
        if isinstance(lookup_key, BaseRecipe):
            lookup_key = recipe.name
        self._recipes_lock.acquire()
        try:
            if lookup_key not in self._recipes:
                raise KeyError(f"Cannot remote Recipe '{lookup_key}' as it "
                    "does not already exist")
            self._recipes.pop(lookup_key)
        except Exception as e:
            self._recipes_lock.release()
            raise e
        self._recipes_lock.release()

        if isinstance(recipe, BaseRecipe):
            self._identify_lost_rules(lost_recipe=recipe.name)
        else:
            self._identify_lost_rules(lost_recipe=recipe)

    def get_recipes(self)->None:
        to_return = {}
        self._recipes_lock.acquire()
        try:
            to_return = deepcopy(self._recipes)
        except Exception as e:
            self._recipes_lock.release()
            raise e
        self._recipes_lock.release()
        return to_return
    
    def get_rules(self)->None:
        to_return = {}
        self._rules_lock.acquire()
        try:
            to_return = deepcopy(self._rules)
        except Exception as e:
            self._rules_lock.release()
            raise e
        self._rules_lock.release()
        return to_return

    def _identify_new_rules(self, new_pattern:FileEventPattern=None, 
            new_recipe:BaseRecipe=None)->None:

        if new_pattern:
            self._patterns_lock.acquire()
            self._recipes_lock.acquire()
            try:
                if new_pattern.name not in self._patterns:
                    self._patterns_lock.release()
                    self._recipes_lock.release()
                    return
                if new_pattern.recipe in self._recipes:
                    self._create_new_rule(
                        new_pattern,
                        self._recipes[new_pattern.recipe],
                    )
            except Exception as e:
                self._patterns_lock.release()
                self._recipes_lock.release()
                raise e
            self._patterns_lock.release()
            self._recipes_lock.release()

        if new_recipe:
            self._patterns_lock.acquire()
            self._recipes_lock.acquire()
            try:
                if new_recipe.name not in self._recipes:
                    self._patterns_lock.release()
                    self._recipes_lock.release()
                    return
                for pattern in self._patterns.values():
                    if pattern.recipe == new_recipe.name:
                        self._create_new_rule(
                            pattern,
                            new_recipe,
                        )
            except Exception as e:
                self._patterns_lock.release()
                self._recipes_lock.release()
                raise e
            self._patterns_lock.release()
            self._recipes_lock.release()

    def _identify_lost_rules(self, lost_pattern:str=None, 
            lost_recipe:str=None)->None:
        to_delete = []
        self._rules_lock.acquire()
        try:
            for name, rule in self._rules.items():
                if lost_pattern and rule.pattern.name == lost_pattern:
                        to_delete.append(name)
                if lost_recipe and rule.recipe.name == lost_recipe:
                        to_delete.append(name)
            for delete in to_delete:
                if delete in self._rules.keys():
                    self._rules.pop(delete)
        except Exception as e:
            self._rules_lock.release()
            raise e
        self._rules_lock.release()

    def _create_new_rule(self, pattern:FileEventPattern, recipe:BaseRecipe)->None:
        rule = create_rule(pattern, recipe)
        self._rules_lock.acquire()
        try:
            if rule.name in self._rules:
                raise KeyError("Cannot create Rule with name of "
                    f"'{rule.name}' as already in use")
            self._rules[rule.name] = rule
        except Exception as e:
            self._rules_lock.release()
            raise e
        self._rules_lock.release()

        self._apply_retroactive_rule(rule)
        
    def _is_valid_base_dir(self, base_dir:str)->None:
        valid_existing_dir_path(base_dir)

    def _is_valid_patterns(self, patterns:dict[str,FileEventPattern])->None:
        valid_dict(patterns, str, FileEventPattern, min_length=0, strict=False)

    def _is_valid_recipes(self, recipes:dict[str,BaseRecipe])->None:
        valid_dict(recipes, str, BaseRecipe, min_length=0, strict=False)

    def _apply_retroactive_rules(self)->None:
        for rule in self._rules.values():
            self._apply_retroactive_rule(rule)

    def _apply_retroactive_rule(self, rule:BaseRule)->None:
        self._rules_lock.acquire()
        try:
            if rule.name not in self._rules:
                self._rules_lock.release()
                return
            if FILE_RETROACTIVE_EVENT in rule.pattern.event_mask:
            
                testing_path = os.path.join(
                    self.base_dir, rule.pattern.triggering_path)

                globbed = glob.glob(testing_path)

                for globble in globbed:

                    meow_event = create_event(
                        WATCHDOG_TYPE, 
                        globble,
                        { WATCHDOG_BASE: self.base_dir, WATCHDOG_RULE: rule }
                    )
                    print_debug(self._print_target, self.debug_level,  
                        f"Retroactive event for file at at {globble} hit rule "
                        f"{rule.name}", DEBUG_INFO)
                    self.to_runner.send(meow_event)

        except Exception as e:
            self._rules_lock.release()
            raise e
        self._rules_lock.release()


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
