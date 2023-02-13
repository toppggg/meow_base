"""
This file contains functions and definitions used in pattern and recipe 
requirements, and assessed by handlers and conductors.

Author(s): David Marchant
"""

from typing import Any, Dict, List, Tuple, Union

from core.correctness.validation import check_type

REQUIREMENT_PYTHON = "python"
REQ_PYTHON_MODULES = "modules"
REQ_PYTHON_VERSION = "version"
REQ_PYTHON_ENVIRONMENT = "environment"


def create_requirement_dict(key:str, entires:Dict[str,Any]
        )->Tuple[str,Dict[str,Any]]:
    return key, entires

def create_python_requirements(modules:Union[str,List[str]]="", 
        version:str="", environment:str="")->Dict[str,Any]:
    check_type(modules, str, alt_types=[List])
    if not isinstance(modules, List):
        modules = [modules]
    for i, module in enumerate(modules):
        check_type(module, str, hint="create_python_requirement.modules[i]")
    check_type(version, str)
    check_type(environment, str)
    
    python_reqs = {}

    if modules != [""]:
        python_reqs[REQ_PYTHON_MODULES] = modules

    if version:
        python_reqs[REQ_PYTHON_VERSION] = version
    
    if environment:
        python_reqs[REQ_PYTHON_ENVIRONMENT] = environment

    return create_requirement_dict(REQUIREMENT_PYTHON, python_reqs)
