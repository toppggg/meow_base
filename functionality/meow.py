"""
This file contains functions for meow specific functionality.

Author(s): David Marchant
"""

from datetime import datetime
from os.path import basename, dirname, relpath, splitext
from typing import Any, Dict

from core.correctness.vars import EVENT_PATH, EVENT_RULE, EVENT_TYPE, \
    EVENT_TYPE_WATCHDOG, JOB_CREATE_TIME, JOB_EVENT, JOB_ID, JOB_PATTERN, \
    JOB_RECIPE, JOB_REQUIREMENTS, JOB_RULE, JOB_STATUS, JOB_TYPE, \
    STATUS_QUEUED, WATCHDOG_BASE, WATCHDOG_HASH
from functionality.naming import generate_id


# mig trigger keyword replacements
KEYWORD_PATH = "{PATH}"
KEYWORD_REL_PATH = "{REL_PATH}"
KEYWORD_DIR = "{DIR}"
KEYWORD_REL_DIR = "{REL_DIR}"
KEYWORD_FILENAME = "{FILENAME}"
KEYWORD_PREFIX = "{PREFIX}"
KEYWORD_BASE = "{VGRID}"
KEYWORD_EXTENSION = "{EXTENSION}"
KEYWORD_JOB = "{JOB}"


def replace_keywords(old_dict:Dict[str,str], job_id:str, src_path:str, 
            monitor_base:str)->Dict[str,str]:
    """Function to replace all MEOW magic words in a dictionary with dynamic 
    values."""
    new_dict = {}

    filename = basename(src_path)
    dir = dirname(src_path)
    relativepath = relpath(src_path, monitor_base)
    reldirname = dirname(relativepath)
    (prefix, extension) = splitext(filename)

    for var, val in old_dict.items():
        if isinstance(val, str):
            val = val.replace(KEYWORD_PATH, src_path)
            val = val.replace(KEYWORD_REL_PATH, relativepath)
            val = val.replace(KEYWORD_DIR, dir)
            val = val.replace(KEYWORD_REL_DIR, reldirname)
            val = val.replace(KEYWORD_FILENAME, filename)
            val = val.replace(KEYWORD_PREFIX, prefix)
            val = val.replace(KEYWORD_BASE, monitor_base)
            val = val.replace(KEYWORD_EXTENSION, extension)
            val = val.replace(KEYWORD_JOB, job_id)

            new_dict[var] = val
        else:
            new_dict[var] = val

    return new_dict

def create_event(event_type:str, path:str, rule:Any, extras:Dict[Any,Any]={}
        )->Dict[Any,Any]:
    """Function to create a MEOW dictionary."""
    return {
        **extras, 
        EVENT_PATH: path, 
        EVENT_TYPE: event_type, 
        EVENT_RULE: rule
    }

def create_watchdog_event(path:str, rule:Any, base:str, hash:str, 
            extras:Dict[Any,Any]={})->Dict[Any,Any]:
    """Function to create a MEOW event dictionary."""
    return create_event(
        EVENT_TYPE_WATCHDOG, 
        path, 
        rule,
        extras={
            **extras,
            **{
                WATCHDOG_HASH: hash,
                WATCHDOG_BASE: base
            }
        }
    )

def create_job(job_type:str, event:Dict[str,Any], extras:Dict[Any,Any]={}
        )->Dict[Any,Any]:
    """Function to create a MEOW job dictionary."""
    job_dict = {
        #TODO compress event?
        JOB_ID: generate_id(prefix="job_"),
        JOB_EVENT: event,
        JOB_TYPE: job_type,
        JOB_PATTERN: event[EVENT_RULE].pattern.name,
        JOB_RECIPE: event[EVENT_RULE].recipe.name,
        JOB_RULE: event[EVENT_RULE].name,
        JOB_STATUS: STATUS_QUEUED,
        JOB_CREATE_TIME: datetime.now(),
        JOB_REQUIREMENTS: event[EVENT_RULE].recipe.requirements
    }

    return {**extras, **job_dict}

