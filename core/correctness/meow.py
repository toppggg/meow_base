
from datetime import datetime
from typing import Any, Dict, Type

from core.base_rule import BaseRule
from core.correctness.validation import check_type
from core.correctness.vars import EVENT_TYPE, EVENT_PATH, JOB_EVENT, \
    JOB_TYPE, JOB_ID, JOB_PATTERN, JOB_RECIPE, JOB_RULE, JOB_STATUS, \
    JOB_CREATE_TIME, EVENT_RULE, WATCHDOG_BASE, WATCHDOG_HASH, PATTERN_NAME, RECIPE_NAME

# Required keys in event dict
EVENT_KEYS = {
    EVENT_TYPE: str,
    EVENT_PATH: str,
    # Should be a Rule but can't import here due to circular dependencies
    EVENT_RULE: BaseRule
}

WATCHDOG_EVENT_KEYS = {
    WATCHDOG_BASE: str,
    WATCHDOG_HASH: str,    
    **EVENT_KEYS
}

# Required keys in job dict
JOB_KEYS = {
    JOB_TYPE: str,
    JOB_EVENT: Dict,
    JOB_ID: str,
    JOB_PATTERN: Any,
    JOB_RECIPE: Any,
    JOB_RULE: str,
    JOB_STATUS: str,
    JOB_CREATE_TIME: datetime,
}


def valid_meow_dict(meow_dict:Dict[str,Any], msg:str, 
        keys:Dict[str,Type])->None:
    """Check given dictionary expresses a meow construct. This won't do much 
    directly, but is called by more specific validation functions."""
    check_type(meow_dict, Dict)
    # Check we have all the required keys, and they are all of the expected 
    # type
    for key, value_type in keys.items():
        if not key in meow_dict.keys():
            raise KeyError(f"{msg} require key '{key}'")
        check_type(meow_dict[key], value_type)

def valid_event(event:Dict[str,Any])->None:
    """Check that a given dict expresses a meow event."""
    valid_meow_dict(event, "Event", EVENT_KEYS)

def valid_job(job:Dict[str,Any])->None:
    """Check that a given dict expresses a meow job."""
    valid_meow_dict(job, "Job", JOB_KEYS)

def valid_watchdog_event(event:Dict[str,Any])->None:
    valid_meow_dict(event, "Watchdog event", WATCHDOG_EVENT_KEYS)
