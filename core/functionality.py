
import copy
import hashlib
import inspect
import json
import nbformat
import os
import sys
import yaml

from multiprocessing.connection import Connection, wait as multi_wait
from multiprocessing.queues import Queue
from papermill.translators import papermill_translators
from typing import Any, Union
from random import SystemRandom

from core.meow import BasePattern, BaseRecipe, BaseRule
from core.correctness.validation import check_type, valid_dict, valid_list, \
    valid_existing_file_path, valid_path
from core.correctness.vars import CHAR_LOWERCASE, CHAR_UPPERCASE, \
    VALID_CHANNELS, HASH_BUFFER_SIZE, SHA256, DEBUG_WARNING, DEBUG_ERROR, \
    DEBUG_INFO

def check_pattern_dict(patterns, min_length=1):
    valid_dict(patterns, str, BasePattern, strict=False, min_length=min_length)
    for k, v in patterns.items():
        if k != v.name:
            raise KeyError(f"Key '{k}' indexes unexpected Pattern '{v.name}' "
                "Pattern dictionaries must be keyed with the name of the "
                "Pattern.")

def check_recipe_dict(recipes, min_length=1):
    valid_dict(recipes, str, BaseRecipe, strict=False, min_length=min_length)
    for k, v in recipes.items():
        if k != v.name:
            raise KeyError(f"Key '{k}' indexes unexpected Recipe '{v.name}' "
                "Recipe dictionaries must be keyed with the name of the "
                "Recipe.")

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

def create_rules(patterns:Union[dict[str,BasePattern],list[BasePattern]], 
        recipes:Union[dict[str,BaseRecipe],list[BaseRecipe]],
        new_rules:list[BaseRule]=[])->dict[str,BaseRule]:
    check_type(patterns, dict, alt_types=[list])
    check_type(recipes, dict, alt_types=[list])
    valid_list(new_rules, BaseRule, min_length=0)

    if isinstance(patterns, list):
        valid_list(patterns, BasePattern, min_length=0)
        patterns = {pattern.name:pattern for pattern in patterns}
    else:
        check_pattern_dict(patterns, min_length=0)

    if isinstance(recipes, list):
        valid_list(recipes, BaseRecipe, min_length=0)
        recipes = {recipe.name:recipe for recipe in recipes}
    else:
        check_recipe_dict(recipes, min_length=0)

    # Imported here to avoid circular imports at top of file
    import rules
    rules = {}
    all_rules ={(r.pattern_type, r.recipe_type):r for r in [r[1] \
        for r in inspect.getmembers(sys.modules["rules"], inspect.isclass) \
        if (issubclass(r[1], BaseRule))]}

    for pattern in patterns.values():
        if pattern.recipe in recipes:
            key = (type(pattern).__name__, 
                type(recipes[pattern.recipe]).__name__)
            if (key) in all_rules:
                rule = all_rules[key](
                    generate_id(prefix="Rule_"), 
                    pattern, 
                    recipes[pattern.recipe]
                )
                rules[rule.name] = rule
    return rules

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
    if not os.path.exists(path):
        os.mkdir(path)
    elif os.path.isfile(path):
        raise ValueError('Cannot make directory in %s as it already '
                         'exists and is a file' % path)
    else:
        if not can_exist:
            if ensure_clean:
                rmtree(path)
                os.mkdir(path)
            else:
                raise ValueError("Directory %s already exists. " % path)

def read_yaml(filepath:str):
    """
    Reads a file path as a yaml object.

    :param filepath: (str) The file to read.

    :return: (object) An object read from the file.
    """
    with open(filepath, 'r') as yaml_file:
        return yaml.load(yaml_file, Loader=yaml.Loader)

def write_yaml(source:Any, filename:str, mode:str='w'):
    """
    Writes a given objcet to a yaml file.

    :param source: (any) A python object to be written.

    :param filename: (str) The filename to be written to.

    :return: No return
    """
    with open(filename, mode) as param_file:
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