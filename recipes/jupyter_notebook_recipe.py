
"""
This file contains definitions for a MEOW recipe based off of jupyter notebooks,
along with an appropriate handler for said events.

Author(s): David Marchant
"""
import os
import itertools
import nbformat
import sys

from typing import Any

from core.correctness.validation import check_type, valid_string, \
    valid_dict, valid_path, valid_existing_dir_path, setup_debugging
from core.correctness.vars import VALID_VARIABLE_NAME_CHARS, PYTHON_FUNC, \
    DEBUG_INFO, WATCHDOG_TYPE, JOB_HASH, PYTHON_EXECUTION_BASE, \
    WATCHDOG_RULE, EVENT_PATH, PYTHON_TYPE, WATCHDOG_HASH, JOB_PARAMETERS, \
    PYTHON_OUTPUT_DIR, JOB_ID, WATCHDOG_BASE, META_FILE, BASE_FILE, \
    PARAMS_FILE, JOB_STATUS, STATUS_QUEUED
from core.functionality import print_debug, create_job, replace_keywords, \
    make_dir, write_yaml, write_notebook
from core.meow import BaseRecipe, BaseHandler
from patterns.file_event_pattern import SWEEP_START, SWEEP_STOP, SWEEP_JUMP


class JupyterNotebookRecipe(BaseRecipe):
    # A path to the jupyter notebook used to create this recipe
    source:str
    def __init__(self, name:str, recipe:Any, parameters:dict[str,Any]={}, 
            requirements:dict[str,Any]={}, source:str=""):
        """JupyterNotebookRecipe Constructor. This is used to execute analysis 
        code using the papermill module."""
        super().__init__(name, recipe, parameters, requirements)
        self._is_valid_source(source)
        self.source = source

    def _is_valid_source(self, source:str)->None:
        """Validation check for 'source' variable from main constructor."""
        if source:
            valid_path(source, extension=".ipynb", min_length=0)

    def _is_valid_recipe(self, recipe:dict[str,Any])->None:
        """Validation check for 'recipe' variable from main constructor. 
        Called within parent BaseRecipe constructor."""
        check_type(recipe, dict)
        nbformat.validate(recipe)

    def _is_valid_parameters(self, parameters:dict[str,Any])->None:
        """Validation check for 'parameters' variable from main constructor. 
        Called within parent BaseRecipe constructor."""
        valid_dict(parameters, str, Any, strict=False, min_length=0)
        for k in parameters.keys():
            valid_string(k, VALID_VARIABLE_NAME_CHARS)

    def _is_valid_requirements(self, requirements:dict[str,Any])->None:
        """Validation check for 'requirements' variable from main constructor. 
        Called within parent BaseRecipe constructor."""
        valid_dict(requirements, str, Any, strict=False, min_length=0)
        for k in requirements.keys():
            valid_string(k, VALID_VARIABLE_NAME_CHARS)

class PapermillHandler(BaseHandler):
    # handler directory to setup jobs in
    handler_base:str
    # TODO move me to conductor?
    # Final location for job output to be placed
    output_dir:str
    # Config option, above which debug messages are ignored
    debug_level:int
    # Where print messages are sent
    _print_target:Any
    def __init__(self, handler_base:str, output_dir:str, print:Any=sys.stdout, 
            logging:int=0)->None:
        """PapermillHandler Constructor. This creats jobs to be executed using 
        the papermill module. This does not run as a continuous thread to 
        handle execution, but is invoked according to a factory pattern using 
        the handle function."""
        super().__init__()
        self._is_valid_handler_base(handler_base)
        self.handler_base = handler_base
        self._is_valid_output_dir(output_dir)
        self.output_dir = output_dir
        self._print_target, self.debug_level = setup_debugging(print, logging)
        print_debug(self._print_target, self.debug_level, 
            "Created new PapermillHandler instance", DEBUG_INFO)

    def handle(self, event:dict[str,Any])->None:
        """Function called to handle a given event."""
        print_debug(self._print_target, self.debug_level, 
            f"Handling event {event[EVENT_PATH]}", DEBUG_INFO)

        rule = event[WATCHDOG_RULE]

        # Assemble job parameters dict from pattern variables
        yaml_dict = {}
        for var, val in rule.pattern.parameters.items():
            yaml_dict[var] = val
        for var, val in rule.pattern.outputs.items():
            yaml_dict[var] = val
        yaml_dict[rule.pattern.triggering_file] = event[EVENT_PATH]

        # If no parameter sweeps, then one job will suffice
        if not rule.pattern.sweep:
            self.setup_job(event, yaml_dict)
        else:
            # If parameter sweeps, then many jobs created
            values_dict = {}
            for var, val in rule.pattern.sweep.items():
                values_dict[var] = []
                par_val = val[SWEEP_START]
                while par_val <= val[SWEEP_STOP]:
                    values_dict[var].append((var, par_val))
                    par_val += val[SWEEP_JUMP]

            # combine all combinations of sweep values
            values_list = list(itertools.product(
                *[v for v in values_dict.values()]))
            for values in values_list:
                for value in values:
                    yaml_dict[value[0]] = value[1]
                self.setup_job(event, yaml_dict)

    def valid_event_types(self)->list[str]:
        """Function to provide a list of the types of events this handler can 
        process."""
        return [WATCHDOG_TYPE]

    def _is_valid_handler_base(self, handler_base)->None:
        """Validation check for 'handler_base' variable from main 
        constructor."""
        valid_existing_dir_path(handler_base)

    def _is_valid_output_dir(self, output_dir)->None:
        """Validation check for 'output_dir' variable from main 
        constructor."""
        valid_existing_dir_path(output_dir, allow_base=True)

    def setup_job(self, event:dict[str,Any], yaml_dict:dict[str,Any])->None:
        """Function to set up new job dict and send it to the runner to be 
        executed."""
        meow_job = create_job(
            PYTHON_TYPE, 
            event, 
            {
                JOB_PARAMETERS:yaml_dict,
                JOB_HASH: event[WATCHDOG_HASH],
                PYTHON_FUNC:job_func,
                PYTHON_OUTPUT_DIR:self.output_dir,
                PYTHON_EXECUTION_BASE:self.handler_base
            }
        )
        print_debug(self._print_target, self.debug_level,  
            f"Creating job from event at {event[EVENT_PATH]} of type "
            f"{PYTHON_TYPE}.", DEBUG_INFO)

        # replace MEOW keyworks within variables dict
        yaml_dict = replace_keywords(
            meow_job[JOB_PARAMETERS],
            meow_job[JOB_ID],
            event[EVENT_PATH],
            event[WATCHDOG_BASE]
        )

        # Create a base job directory
        job_dir = os.path.join(
            meow_job[PYTHON_EXECUTION_BASE], meow_job[JOB_ID])
        make_dir(job_dir)

        # write a status file to the job directory
        meta_file = os.path.join(job_dir, META_FILE)
        write_yaml(meow_job, meta_file)

        # write an executable notebook to the job directory
        base_file = os.path.join(job_dir, BASE_FILE)
        write_notebook(event[WATCHDOG_RULE].recipe.recipe, base_file)

        # write a parameter file to the job directory
        param_file = os.path.join(job_dir, PARAMS_FILE)
        write_yaml(yaml_dict, param_file)

        meow_job[JOB_STATUS] = STATUS_QUEUED

        # update the status file with queued status
        write_yaml(meow_job, meta_file)
        
        # Send job directory, as actual definitons will be read from within it
        self.to_runner.send(job_dir)

# Papermill job execution code, to be run within the conductor
def job_func(job):
    # Requires own imports as will be run in its own execution environment
    import os
    import papermill
    from datetime import datetime
    from core.functionality import write_yaml, read_yaml, write_notebook, \
        get_file_hash, parameterize_jupyter_notebook
    from core.correctness.vars import JOB_EVENT, WATCHDOG_RULE, JOB_ID, \
        EVENT_PATH, META_FILE, PARAMS_FILE, JOB_FILE, RESULT_FILE, \
        JOB_STATUS, JOB_HASH, SHA256, STATUS_SKIPPED, JOB_END_TIME, \
        JOB_ERROR, STATUS_FAILED, PYTHON_EXECUTION_BASE

    event = job[JOB_EVENT]

    # Identify job files
    job_dir = os.path.join(job[PYTHON_EXECUTION_BASE], job[JOB_ID])
    meta_file = os.path.join(job_dir, META_FILE)
    job_file = os.path.join(job_dir, JOB_FILE)
    result_file = os.path.join(job_dir, RESULT_FILE)
    param_file = os.path.join(job_dir, PARAMS_FILE)

    yaml_dict = read_yaml(param_file)

    # Check the hash of the triggering file, if present. This addresses 
    # potential race condition as file could have been modified since 
    # triggering event
    if JOB_HASH in job:
        # get current hash
        triggerfile_hash = get_file_hash(job[JOB_EVENT][EVENT_PATH], SHA256)
        # If hash doesn't match, then abort the job. If its been modified, then
        # another job will have been scheduled anyway.
        if not triggerfile_hash \
                or triggerfile_hash != job[JOB_HASH]:
            job[JOB_STATUS] = STATUS_SKIPPED
            job[JOB_END_TIME] = datetime.now()
            msg = "Job was skipped as triggering file " + \
                f"'{job[JOB_EVENT][EVENT_PATH]}' has been modified since " + \
                "scheduling. Was expected to have hash " + \
                f"'{job[JOB_HASH]}' but has '{triggerfile_hash}'."
            job[JOB_ERROR] = msg
            write_yaml(job, meta_file)
            return

    # Create a parameterised version of the executable notebook
    try:
        job_notebook = parameterize_jupyter_notebook(
            event[WATCHDOG_RULE].recipe.recipe, yaml_dict
        )
        write_notebook(job_notebook, job_file)
    except Exception as e:
        job[JOB_STATUS] = STATUS_FAILED
        job[JOB_END_TIME] = datetime.now()
        msg = f"Job file {job[JOB_ID]} was not created successfully. {e}"
        job[JOB_ERROR] = msg
        write_yaml(job, meta_file)
        return

    # Execute the parameterised notebook
    try:
        papermill.execute_notebook(job_file, result_file, {})
    except Exception as e:
        job[JOB_STATUS] = STATUS_FAILED
        job[JOB_END_TIME] = datetime.now()
        msg = f"Result file {result_file} was not created successfully. {e}"
        job[JOB_ERROR] = msg
        write_yaml(job, meta_file)
        return
