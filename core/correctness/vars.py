
import os

from multiprocessing import Queue
from multiprocessing.connection import Connection
from inspect import signature

from typing import Union

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

BAREBONES_NOTEBOOK = {
    "cells": [],
    "metadata": {},
    "nbformat": 4,
    "nbformat_minor": 4
}

FILE_CREATE_EVENT = "file_created"
FILE_MODIFY_EVENT = "file_modified"
FILE_MOVED_EVENT = "file_moved"
FILE_CLOSED_EVENT = "file_closed"
FILE_DELETED_EVENT = "file_deleted"
FILE_EVENTS = [
    FILE_CREATE_EVENT, 
    FILE_MODIFY_EVENT, 
    FILE_MOVED_EVENT, 
    FILE_CLOSED_EVENT, 
    FILE_DELETED_EVENT
]

DIR_CREATE_EVENT = "dir_created"
DIR_MODIFY_EVENT = "dir_modified"
DIR_MOVED_EVENT = "dir_moved"
DIR_DELETED_EVENT = "dir_deleted"
DIR_EVENTS = [
    DIR_CREATE_EVENT,
    DIR_MODIFY_EVENT,
    DIR_MOVED_EVENT,
    DIR_DELETED_EVENT
]

PIPE_READ = 0
PIPE_WRITE = 1

def get_drt_imp_msg(base_class):
    return f"{base_class.__name__} may not be instantiated directly. " \
        f"Implement a child class."

def get_not_imp_msg(parent_class, class_function):
    return f"Children of the '{parent_class.__name__}' class must implement " \
        f"the '{class_function.__name__}({signature(class_function)})' " \
        "function"
