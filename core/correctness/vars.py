
"""
This file contains a variety of constants used throughout the package. 
Constants specific to only one file should be stored there, and only shared 
here.

Author(s): David Marchant
"""
import os

from multiprocessing import Queue
from multiprocessing.connection import Connection
from inspect import signature

from typing import Union

# validation
CHAR_LOWERCASE = 'abcdefghijklmnopqrstuvwxyz'
CHAR_UPPERCASE = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
CHAR_NUMERIC = '0123456789'

VALID_NAME_CHARS = CHAR_UPPERCASE + CHAR_LOWERCASE + CHAR_NUMERIC + "_-"

VALID_RECIPE_NAME_CHARS = VALID_NAME_CHARS
VALID_PATTERN_NAME_CHARS = VALID_NAME_CHARS
VALID_RULE_NAME_CHARS = VALID_NAME_CHARS
VALID_VARIABLE_NAME_CHARS = CHAR_UPPERCASE + CHAR_LOWERCASE + CHAR_NUMERIC + "_"

VALID_JUPYTER_NOTEBOOK_FILENAME_CHARS = VALID_NAME_CHARS + "." + os.path.sep
VALID_JUPYTER_NOTEBOOK_EXTENSIONS = [".ipynb"]

VALID_PATH_CHARS = VALID_NAME_CHARS + "." + os.path.sep
VALID_TRIGGERING_PATH_CHARS = VALID_NAME_CHARS + ".*" + os.path.sep

VALID_CHANNELS = Union[Connection,Queue]

# hashing
HASH_BUFFER_SIZE = 65536
SHA256 = "sha256"

# meow events
EVENT_TYPE = "event_type"
EVENT_PATH = "event_path"
EVENT_RULE = "rule"

# watchdog events
EVENT_TYPE_WATCHDOG = "watchdog"
WATCHDOG_BASE = "monitor_base"
WATCHDOG_HASH = "file_hash"

# inotify events
FILE_CREATE_EVENT = "file_created"
FILE_MODIFY_EVENT = "file_modified"
FILE_MOVED_EVENT = "file_moved"
FILE_CLOSED_EVENT = "file_closed"
FILE_DELETED_EVENT = "file_deleted"
FILE_RETROACTIVE_EVENT = "retroactive_file_event"
FILE_EVENTS = [
    FILE_CREATE_EVENT, 
    FILE_MODIFY_EVENT, 
    FILE_MOVED_EVENT, 
    FILE_CLOSED_EVENT, 
    FILE_DELETED_EVENT,
    FILE_RETROACTIVE_EVENT
]

DIR_CREATE_EVENT = "dir_created"
DIR_MODIFY_EVENT = "dir_modified"
DIR_MOVED_EVENT = "dir_moved"
DIR_DELETED_EVENT = "dir_deleted"
DIR_RETROACTIVE_EVENT = "retroactive_dir_event"
DIR_EVENTS = [
    DIR_CREATE_EVENT,
    DIR_MODIFY_EVENT,
    DIR_MOVED_EVENT,
    DIR_DELETED_EVENT,
    DIR_RETROACTIVE_EVENT
]

# runner defaults
DEFAULT_JOB_QUEUE_DIR = "job_queue"
DEFAULT_JOB_OUTPUT_DIR = "job_output"

# meow jobs
JOB_TYPE = "job_type"
JOB_TYPE_PYTHON = "python"
JOB_TYPE_PAPERMILL = "papermill"
PYTHON_FUNC = "func"

JOB_TYPES = {
    JOB_TYPE_PAPERMILL: [
        "base.ipynb",
        "job.ipynb",
        "result.ipynb",
    ],
    JOB_TYPE_PYTHON: [
        "base.py",
        "job.py",
        "result.py",
    ]
}

# job definitions
JOB_ID = "id"
JOB_EVENT = "event"
JOB_PATTERN = "pattern"
JOB_RECIPE = "recipe"
JOB_RULE = "rule"
JOB_HASH = "hash"
JOB_STATUS = "status"
JOB_CREATE_TIME = "create"
JOB_START_TIME = "start"
JOB_END_TIME = "end"
JOB_ERROR = "error"
JOB_REQUIREMENTS = "requirements"
JOB_PARAMETERS = "parameters"

# job statuses
STATUS_QUEUED = "queued"
STATUS_RUNNING = "running"
STATUS_SKIPPED = "skipped"
STATUS_FAILED = "failed"
STATUS_DONE = "done"

# job definition files
META_FILE = "job.yml"
PARAMS_FILE = "params.yml"

# Parameter sweep keys
SWEEP_START = "start"
SWEEP_STOP = "stop"
SWEEP_JUMP = "jump"

# debug printing levels
DEBUG_ERROR = 1
DEBUG_WARNING = 2
DEBUG_INFO = 3

# debug message functions
def get_drt_imp_msg(base_class):
    return f"{base_class.__name__} may not be instantiated directly. " \
        f"Implement a child class."

def get_not_imp_msg(parent_class, class_function):
    return f"Children of the '{parent_class.__name__}' class must implement " \
        f"the '{class_function.__name__}({signature(class_function)})' " \
        "function"

def get_base_file(job_type:str):
    return JOB_TYPES[job_type][0]

def get_job_file(job_type:str):
    return JOB_TYPES[job_type][1]

def get_result_file(job_type:str):
    return JOB_TYPES[job_type][2]
