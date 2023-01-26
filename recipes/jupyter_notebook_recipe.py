
import nbformat
import sys
import threading

from multiprocessing import Pipe
from typing import Any

from core.correctness.validation import check_type, valid_string, \
    valid_dict, valid_path, valid_list, valid_existing_dir_path, \
    setup_debugging
from core.correctness.vars import VALID_VARIABLE_NAME_CHARS, VALID_CHANNELS, \
    PYTHON_FUNC, DEBUG_INFO, WATCHDOG_TYPE, JOB_HASH, PYTHON_EXECUTION_BASE, \
    WATCHDOG_RULE, EVENT_PATH, PYTHON_TYPE, WATCHDOG_HASH, JOB_PARAMETERS, \
    PYTHON_OUTPUT_DIR
from core.functionality import print_debug, create_job, replace_keywords
from core.meow import BaseRecipe, BaseHandler
from patterns.file_event_pattern import SWEEP_START, SWEEP_STOP, SWEEP_JUMP


class JupyterNotebookRecipe(BaseRecipe):
    source:str
    def __init__(self, name:str, recipe:Any, parameters:dict[str,Any]={}, 
            requirements:dict[str,Any]={}, source:str=""):
        super().__init__(name, recipe, parameters, requirements)
        self._is_valid_source(source)
        self.source = source

    def _is_valid_source(self, source:str)->None:
        if source:
            valid_path(source, extension=".ipynb", min_length=0)

    def _is_valid_recipe(self, recipe:dict[str,Any])->None:
        check_type(recipe, dict)
        nbformat.validate(recipe)

    def _is_valid_parameters(self, parameters:dict[str,Any])->None:
        valid_dict(parameters, str, Any, strict=False, min_length=0)
        for k in parameters.keys():
            valid_string(k, VALID_VARIABLE_NAME_CHARS)

    def _is_valid_requirements(self, requirements:dict[str,Any])->None:
        valid_dict(requirements, str, Any, strict=False, min_length=0)
        for k in requirements.keys():
            valid_string(k, VALID_VARIABLE_NAME_CHARS)

class PapermillHandler(BaseHandler):
    handler_base:str
    output_dir:str
    debug_level:int
    _worker:threading.Thread
    _stop_pipe:Pipe
    _print_target:Any
    def __init__(self, handler_base:str, output_dir:str, print:Any=sys.stdout, 
            logging:int=0)->None:
        super().__init__()
        self._is_valid_handler_base(handler_base)
        self.handler_base = handler_base
        self._is_valid_output_dir(output_dir)
        self.output_dir = output_dir
        self._print_target, self.debug_level = setup_debugging(print, logging)
        self._worker = None
        self._stop_pipe = Pipe()
        print_debug(self._print_target, self.debug_level, 
            "Created new PapermillHandler instance", DEBUG_INFO)

    def handle(self, event:dict[str,Any])->None:
        print_debug(self._print_target, self.debug_level, 
            f"Handling event {event[EVENT_PATH]}", DEBUG_INFO)

        rule = event[WATCHDOG_RULE]

        yaml_dict = {}
        for var, val in rule.pattern.parameters.items():
            yaml_dict[var] = val
        for var, val in rule.pattern.outputs.items():
            yaml_dict[var] = val
        yaml_dict[rule.pattern.triggering_file] = event[EVENT_PATH]

        if not rule.pattern.sweep:
            self.setup_job(event, yaml_dict)
        else:
            for var, val in rule.pattern.sweep.items():
                values = []
                par_val = rule.pattern.sweep[SWEEP_START]
                while par_val <= rule.pattern.sweep[SWEEP_STOP]:
                    values.append(par_val)
                    par_val += rule.pattern.sweep[SWEEP_JUMP]

                for value in values:
                    yaml_dict[var] = value
                    self.setup_job(event, yaml_dict)

    def valid_event_types(self)->list[str]:
        return [WATCHDOG_TYPE]

    def _is_valid_inputs(self, inputs:list[VALID_CHANNELS])->None:
        valid_list(inputs, VALID_CHANNELS)

    def _is_valid_handler_base(self, handler_base)->None:
        valid_existing_dir_path(handler_base)

    def _is_valid_output_dir(self, output_dir)->None:
        valid_existing_dir_path(output_dir, allow_base=True)

    def setup_job(self, event:dict[str,Any], yaml_dict:dict[str,Any])->None:
        meow_job = create_job(PYTHON_TYPE, event, {
            JOB_PARAMETERS:yaml_dict,
            JOB_HASH: event[WATCHDOG_HASH],
            PYTHON_FUNC:job_func,
            PYTHON_OUTPUT_DIR:self.output_dir,
            PYTHON_EXECUTION_BASE:self.handler_base,})
        print_debug(self._print_target, self.debug_level,  
            f"Creating job from event at {event[EVENT_PATH]} of type "
            f"{PYTHON_TYPE}.", DEBUG_INFO)
        self.to_runner.send(meow_job)

def job_func(job):
    import os
    import shutil
    import papermill
    from datetime import datetime
    from core.functionality import make_dir, write_yaml, \
        write_notebook, get_file_hash, parameterize_jupyter_notebook
    from core.correctness.vars import JOB_EVENT, WATCHDOG_RULE, \
        JOB_ID, EVENT_PATH, WATCHDOG_BASE, META_FILE, \
        BASE_FILE, PARAMS_FILE, JOB_FILE, RESULT_FILE, JOB_STATUS, \
        JOB_START_TIME, STATUS_RUNNING, JOB_HASH, SHA256, \
        STATUS_SKIPPED, STATUS_DONE, JOB_END_TIME, \
        JOB_ERROR, STATUS_FAILED, PYTHON_EXECUTION_BASE, PYTHON_OUTPUT_DIR

    event = job[JOB_EVENT]

    yaml_dict = replace_keywords(
        job[JOB_PARAMETERS],
        job[JOB_ID],
        event[EVENT_PATH],
        event[WATCHDOG_BASE]
    )

    job_dir = os.path.join(job[PYTHON_EXECUTION_BASE], job[JOB_ID])
    make_dir(job_dir)

    meta_file = os.path.join(job_dir, META_FILE)
    write_yaml(job, meta_file)

    base_file = os.path.join(job_dir, BASE_FILE)
    write_notebook(event[WATCHDOG_RULE].recipe.recipe, base_file)

    param_file = os.path.join(job_dir, PARAMS_FILE)
    write_yaml(yaml_dict, param_file)

    job_file = os.path.join(job_dir, JOB_FILE)
    result_file = os.path.join(job_dir, RESULT_FILE)

    job[JOB_STATUS] = STATUS_RUNNING
    job[JOB_START_TIME] = datetime.now()

    write_yaml(job, meta_file)

    if JOB_HASH in job:
        triggerfile_hash = get_file_hash(job[JOB_EVENT][EVENT_PATH], SHA256)
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

    try:
        papermill.execute_notebook(job_file, result_file, {})
    except Exception as e:
        job[JOB_STATUS] = STATUS_FAILED
        job[JOB_END_TIME] = datetime.now()
        msg = f"Result file {result_file} was not created successfully. {e}"
        job[JOB_ERROR] = msg
        write_yaml(job, meta_file)
        return

    job[JOB_STATUS] = STATUS_DONE
    job[JOB_END_TIME] = datetime.now()
    write_yaml(job, meta_file)

    job_output_dir = os.path.join(job[PYTHON_OUTPUT_DIR], job[JOB_ID])

    shutil.move(job_dir, job_output_dir)
