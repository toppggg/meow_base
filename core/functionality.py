
import copy
import hashlib
import json
import nbformat
import os
import yaml

from datetime import datetime

from multiprocessing.connection import Connection, wait as multi_wait
from multiprocessing.queues import Queue
from papermill.translators import papermill_translators
from typing import Any
from random import SystemRandom

from core.correctness.validation import check_type, valid_existing_file_path, \
    valid_path
from core.correctness.vars import CHAR_LOWERCASE, CHAR_UPPERCASE, \
    VALID_CHANNELS, HASH_BUFFER_SIZE, SHA256, DEBUG_WARNING, DEBUG_INFO, \
    EVENT_TYPE, EVENT_PATH, JOB_EVENT, JOB_TYPE, JOB_ID, JOB_PATTERN, \
    JOB_RECIPE, JOB_RULE, WATCHDOG_RULE, JOB_STATUS, STATUS_QUEUED, \
    JOB_CREATE_TIME, JOB_REQUIREMENTS

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


#TODO Make this guaranteed unique
def generate_id(prefix:str="", length:int=16, existing_ids:list[str]=[], 
        charset:str=CHAR_UPPERCASE+CHAR_LOWERCASE, attempts:int=24):
    random_length = max(length - len(prefix), 0)
    for _ in range(attempts):
        id = prefix + ''.join(SystemRandom().choice(charset) 
            for _ in range(random_length))
        if id not in existing_ids:
            return id
    raise ValueError(f"Could not generate ID unique from '{existing_ids}' "
        f"using values '{charset}' and length of '{length}'.")

def wait(inputs:list[VALID_CHANNELS])->list[VALID_CHANNELS]:
    all_connections = [i for i in inputs if type(i) is Connection] \
        + [i._reader for i in inputs if type(i) is Queue]

    ready = multi_wait(all_connections)
    ready_inputs = [i for i in inputs if \
        (type(i) is Connection and i in ready) \
        or (type(i) is Queue and i._reader in ready)]
    return ready_inputs

def _get_file_sha256(file_path):
    sha256_hash = hashlib.sha256()
    
    with open(file_path, 'rb') as file_to_hash:
        while True:
            buffer = file_to_hash.read(HASH_BUFFER_SIZE)
            if not buffer:
                break
            sha256_hash.update(buffer)
    
    return sha256_hash.hexdigest()

def get_file_hash(file_path:str, hash:str):
    check_type(hash, str)

    import os
    valid_existing_file_path(file_path)

    valid_hashes = {
        SHA256: _get_file_sha256
    }
    if hash not in valid_hashes:
        raise KeyError(f"Cannot use hash '{hash}'. Valid are "
            "'{list(valid_hashes.keys())}")

    return valid_hashes[hash](file_path)

def rmtree(directory:str):
    """
    Remove a directory and all its contents. 
    Should be faster than shutil.rmtree
    
    :param: (str) The firectory to empty and remove

    :return: No return
    """
    if not os.path.exists(directory):
        return
    for root, dirs, files in os.walk(directory, topdown=False):
        for file in files:
            os.remove(os.path.join(root, file))
        for dir in dirs:
            rmtree(os.path.join(root, dir))
    os.rmdir(directory)

def make_dir(path:str, can_exist:bool=True, ensure_clean:bool=False):
    """
    Creates a new directory at the given path.

    :param path: (str) The directory path.

    :param can_exist: (boolean) [optional] A toggle for if a previously
    existing directory at the path will throw an error or not. Default is
    true (e.g. no error is thrown if the path already exists)

    :param ensure_clean: (boolean) [optional] A toggle for if a previously
    existing directory at the path will be replaced with a new emtpy directory.
    Default is False.

    :return: No return
    """
    if os.path.exists(path):
        if os.path.isfile(path):
            raise ValueError(
                f"Cannot make directory in {path} as it already exists and is "
                "a file")
        if ensure_clean:
            rmtree(path)
                
    os.makedirs(path, exist_ok=can_exist)
    
def read_yaml(filepath:str):
    """
    Reads a file path as a yaml object.

    :param filepath: (str) The file to read.

    :return: (object) An object read from the file.
    """
    with open(filepath, 'r') as yaml_file:
        return yaml.load(yaml_file, Loader=yaml.Loader)

def write_yaml(source:Any, filename:str):
    """
    Writes a given objcet to a yaml file.

    :param source: (any) A python object to be written.

    :param filename: (str) The filename to be written to.

    :return: No return
    """
    with open(filename, 'w') as param_file:
        yaml.dump(source, param_file, default_flow_style=False)

def read_notebook(filepath:str):
    valid_path(filepath, extension="ipynb")
    with open(filepath, 'r') as read_file:
        return json.load(read_file)

def write_notebook(source:dict[str,Any], filename:str):
    """
    Writes the given notebook source code to a given filename.

    :param source: (dict) The notebook source dictionary.

    :param filename: (str) The filename to write to.

    :return: No return
    """
    with open(filename, 'w') as job_file:
        json.dump(source, job_file)

# Adapted from: https://github.com/rasmunk/notebook_parameterizer
def parameterize_jupyter_notebook( jupyter_notebook:dict[str,Any], 
        parameters:dict[str,Any], expand_env_values:bool=False)->dict[str,Any]:
    nbformat.validate(jupyter_notebook)
    check_type(parameters, dict)

    if jupyter_notebook["nbformat"] != 4:
        raise Warning(
            "Parameterization designed to work with nbformat version 4. "
            f"Differing version of '{jupyter_notebook['nbformat']}' may "
            "produce unexpeted results.")

    # Load input notebook
    if "kernelspec" in jupyter_notebook["metadata"]:
        kernel_name = jupyter_notebook["metadata"]["kernelspec"]["name"]
        language = jupyter_notebook["metadata"]["kernelspec"]["language"]
    if "language_info" in jupyter_notebook["metadata"]:
        kernel_name = jupyter_notebook["metadata"]["language_info"]["name"]
        language = jupyter_notebook["metadata"]["language_info"]["name"]
    else:
        raise AttributeError(
            f"Notebook lacks key language and/or kernel_name attributes "
            "within metadata")

    translator = papermill_translators.find_translator(kernel_name, language)

    output_notebook = copy.deepcopy(jupyter_notebook)

    # Find each
    cells = output_notebook["cells"]
    code_cells = [
        (idx, cell) for idx, cell in enumerate(cells) \
            if cell["cell_type"] == "code"
    ]
    for idx, cell in code_cells:
        cell_updated = False
        source = cell["source"]
        # Either single string or a list of strings
        if isinstance(source, str):
            lines = source.split("\n")
        else:
            lines = source

        for idy, line in enumerate(lines):
            if "=" in line:
                d_line = list(map(lambda x: x.replace(" ", ""), 
                    line.split("=")))
                # Matching parameter name
                if len(d_line) == 2 and d_line[0] in parameters:
                    value = parameters[d_line[0]]
                    # Whether to expand value from os env
                    if (
                        expand_env_values
                        and isinstance(value, str)
                        and value.startswith("ENV_")
                    ):
                        env_var = value.replace("ENV_", "")
                        value = os.getenv(
                            env_var, 
                            "MISSING ENVIRONMENT VARIABLE: {}".format(env_var)
                        )
                    lines[idy] = translator.assign(
                        d_line[0], translator.translate(value)
                    )

                    cell_updated = True
        if cell_updated:
            cells[idx]["source"] = "\n".join(lines)

    # Validate that the parameterized notebook is still valid
    nbformat.validate(output_notebook, version=4)

    return output_notebook

def print_debug(print_target, debug_level, msg, level)->None:
        if print_target is None:
            return
        else:
            if level <= debug_level:
                status = "ERROR"
                if level == DEBUG_INFO:
                    status = "INFO"
                elif level == DEBUG_WARNING:
                    status = "WARNING"
                print(f"{status}: {msg}", file=print_target)

def replace_keywords(old_dict:dict[str,str], job_id:str, src_path:str, 
            monitor_base:str)->dict[str,str]:
        new_dict = {}

        filename = os.path.basename(src_path)
        dirname = os.path.dirname(src_path)
        relpath = os.path.relpath(src_path, monitor_base)
        reldirname = os.path.dirname(relpath)
        (prefix, extension) = os.path.splitext(filename)

        for var, val in old_dict.items():
            if isinstance(val, str):
                val = val.replace(KEYWORD_PATH, src_path)
                val = val.replace(KEYWORD_REL_PATH, relpath)
                val = val.replace(KEYWORD_DIR, dirname)
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

def create_event(event_type:str, path:str, source:dict[Any,Any]={}
        )->dict[Any,Any]:
    return {**source, EVENT_PATH: path, EVENT_TYPE: event_type}

def create_job(job_type:str, event:dict[str,Any], source:dict[Any,Any]={}
        )->dict[Any,Any]:
    job_dict = {
        #TODO compress event?
        JOB_ID: generate_id(prefix="job_"),
        JOB_EVENT: event,
        JOB_TYPE: job_type,
        JOB_PATTERN: event[WATCHDOG_RULE].pattern.name,
        JOB_RECIPE: event[WATCHDOG_RULE].recipe.name,
        JOB_RULE: event[WATCHDOG_RULE].name,
        JOB_STATUS: STATUS_QUEUED,
        JOB_CREATE_TIME: datetime.now(),
        JOB_REQUIREMENTS: event[WATCHDOG_RULE].recipe.requirements
    }

    return {**source, **job_dict}
