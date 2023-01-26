
from datetime import datetime
from inspect import signature
from os.path import sep, exists, isfile, isdir, dirname
from typing import Any, _SpecialForm, Union, Tuple, get_origin, get_args

from core.correctness.vars import VALID_PATH_CHARS, get_not_imp_msg, \
    EVENT_TYPE, EVENT_PATH, JOB_EVENT, JOB_TYPE, JOB_ID, JOB_PATTERN, \
    JOB_RECIPE, JOB_RULE, JOB_STATUS, JOB_CREATE_TIME

EVENT_KEYS = {
    EVENT_TYPE: str,
    EVENT_PATH: str
}

JOB_KEYS = {
    JOB_TYPE: str,
    JOB_EVENT: dict,
    JOB_ID: str,
    JOB_PATTERN: Any,
    JOB_RECIPE: Any,
    JOB_RULE: str,
    JOB_STATUS: str,
    JOB_CREATE_TIME: datetime,
}

def check_type(variable:Any, expected_type:type, alt_types:list[type]=[], 
        or_none:bool=False)->None:
    """
    Checks if a given variable is of the expected type. Raises TypeError or
    ValueError as appropriate if any issues are encountered.

    :param variable: (any) variable to check type of

    :param expected_type: (type) expected type of the provided variable

    :param alt_types: (optional)(list) additional types that are also 
    acceptable

    :param or_none: (optional) boolean of if the variable can be unset.
    Default value is False.

    :return: No return.
    """

    type_list = [expected_type]
    if get_origin(expected_type) is Union:
        type_list = list(get_args(expected_type))
    type_list = type_list + alt_types

    if variable is None:
        if or_none == False:
            raise TypeError(
                f'Not allowed None for variable. Expected {expected_type}.'
            )
        else:
            return

    if expected_type == Any:
        return

    if not isinstance(variable, tuple(type_list)):
        print("egh")

        raise TypeError(
            'Expected type(s) are %s, got %s'
            % (get_args(expected_type), type(variable))
        )

def check_implementation(child_func, parent_class):
    if not hasattr(parent_class, child_func.__name__):
        raise AttributeError(
            f"Parent class {parent_class} does not implement base function "
            f"{child_func.__name__} for children to override.")
    parent_func = getattr(parent_class, child_func.__name__)
    if (child_func == parent_func):
        msg = get_not_imp_msg(parent_class, parent_func)
        raise NotImplementedError(msg)
    child_sig = signature(child_func).parameters
    parent_sig = signature(parent_func).parameters

    if child_sig.keys() != parent_sig.keys():
        msg = get_not_imp_msg(parent_class, parent_func)
        raise NotImplementedError(msg)

def valid_string(variable:str, valid_chars:str, min_length:int=1)->None:
    """
    Checks that all characters in a given string are present in a provided
    list of characters. Will raise an ValueError if unexpected character is
    encountered.

    :param variable: (str) variable to check.

    :param valid_chars: (str) collection of valid characters.

    :param min_length: (int) minimum length of variable.

    :return: No return.
    """
    check_type(variable, str)
    check_type(valid_chars, str)

    if len(variable) < min_length:
        raise ValueError (
            f"String '{variable}' is too short. Minimum length is {min_length}"
        )

    for char in variable:
        if char not in valid_chars:
            raise ValueError(
                "Invalid character '%s'. Only valid characters are: "
                "%s" % (char, valid_chars)
            )

def valid_dict(variable:dict[Any, Any], key_type:type, value_type:type, 
        required_keys:list[Any]=[], optional_keys:list[Any]=[], 
        strict:bool=True, min_length:int=1)->None:
    check_type(variable, dict)
    check_type(key_type, type, alt_types=[_SpecialForm])
    check_type(value_type, type, alt_types=[_SpecialForm])
    check_type(required_keys, list)
    check_type(optional_keys, list)
    check_type(strict, bool)

    if len(variable) < min_length:
        raise ValueError(f"Dictionary '{variable}' is below minimum length of "
            f"{min_length}")

    for k, v in variable.items():
        if key_type != Any and not isinstance(k, key_type):
            raise TypeError(f"Key {k} had unexpected type '{type(k)}' "
                f"rather than expected '{key_type}' in dict '{variable}'")
        if value_type != Any and not isinstance(v, value_type):
            raise TypeError(f"Value {v} had unexpected type '{type(v)}' "
                f"rather than expected '{value_type}' in dict '{variable}'")

    for rk in required_keys:
        if rk not in variable.keys():
            raise KeyError(f"Missing required key '{rk}' from dict "
                f"'{variable}'")
    
    if strict:
        for k in variable.keys():
            if k not in required_keys and k not in optional_keys:
                raise ValueError(f"Unexpected key '{k}' should not be present "
                    f"in dict '{variable}'")

def valid_list(variable:list[Any], entry_type:type,
        alt_types:list[type]=[], min_length:int=1)->None:
    check_type(variable, list)
    if len(variable) < min_length:
        raise ValueError(f"List '{variable}' is too short. Should be at least "
            f"of length {min_length}")
    for entry in variable:
        check_type(entry, entry_type, alt_types=alt_types)

def valid_path(variable:str, allow_base:bool=False, extension:str="", 
        min_length:int=1):
    valid_string(variable, VALID_PATH_CHARS, min_length=min_length)
    if not allow_base and variable.startswith(sep):
        raise ValueError(f"Cannot accept path '{variable}'. Must be relative.")
    if extension and not variable.endswith(extension):
        raise ValueError(f"Path '{variable}' does not have required "
            f"extension '{extension}'.")

def valid_existing_file_path(variable:str, allow_base:bool=False, 
        extension:str=""):
    valid_path(variable, allow_base=allow_base, extension=extension)
    if not exists(variable):
        raise FileNotFoundError(
            f"Requested file path '{variable}' does not exist.")
    if not isfile(variable):
        raise ValueError(
            f"Requested file '{variable}' is not a file.")

def valid_existing_dir_path(variable:str, allow_base:bool=False):
    valid_path(variable, allow_base=allow_base, extension="")
    if not exists(variable):
        raise FileNotFoundError(
            f"Requested dir path '{variable}' does not exist.")
    if not isdir(variable):
        raise ValueError(
            f"Requested dir '{variable}' is not a directory.")

def valid_non_existing_path(variable:str, allow_base:bool=False):
    valid_path(variable, allow_base=allow_base, extension="")
    if exists(variable):
        raise ValueError(f"Requested path '{variable}' already exists.")
    if dirname(variable) and not exists(dirname(variable)):
        raise ValueError(
            f"Route to requested path '{variable}' does not exist.")

def setup_debugging(print:Any=None, logging:int=0)->Tuple[Any,int]:
    check_type(logging, int)
    if print is None:
        return None, 0
    else:
        if not isinstance(print, object):
            raise TypeError(f"Invalid print location provided")
        writeable = getattr(print, "write", None)
        if not writeable or not callable(writeable):
            raise TypeError(f"Print object does not implement required "
                "'write' function")

    return print, logging

def valid_meow_dict(meow_dict:dict[str,Any], msg:str, keys:dict[str,type])->None:
    check_type(meow_dict, dict)
    for key, value_type in keys.items():
        if not key in meow_dict.keys():
            raise KeyError(f"{msg} require key '{key}'")
        check_type(meow_dict[key], value_type)

def valid_event(event:dict[str,Any])->None:
    valid_meow_dict(event, "Event", EVENT_KEYS)

def valid_job(job:dict[str,Any])->None:
    valid_meow_dict(job, "Job", JOB_KEYS)
