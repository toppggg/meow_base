
"""
This file contains definitions for the LocalPythonConductor, in order to 
execute Python jobs on the local resource.

Author(s): David Marchant
"""
import os
import shutil

from datetime import datetime
from typing import Any, Tuple, Dict

from core.correctness.vars import JOB_TYPE_PYTHON, PYTHON_FUNC, JOB_STATUS, \
    STATUS_RUNNING, JOB_START_TIME, PYTHON_EXECUTION_BASE, JOB_ID, META_FILE, \
    STATUS_DONE, JOB_END_TIME, STATUS_FAILED, JOB_ERROR, PYTHON_OUTPUT_DIR, \
    JOB_TYPE, JOB_TYPE_PAPERMILL
from core.correctness.validation import valid_job
from core.functionality import read_yaml, write_yaml
from core.meow import BaseConductor


class LocalPythonConductor(BaseConductor):
    def __init__(self)->None:
        """LocalPythonConductor Constructor. This should be used to execute 
        Python jobs, and will then pass any internal job runner files to the 
        output directory."""
        super().__init__()

    def valid_execute_criteria(self, job:Dict[str,Any])->Tuple[bool,str]:
        """Function to determine given an job defintion, if this conductor can 
        process it or not. This conductor will accept any Python job type"""
        try:
            valid_job(job)
            if job[JOB_TYPE] in [JOB_TYPE_PYTHON, JOB_TYPE_PAPERMILL]:
                return True, ""
        except Exception as e:
            pass
        return False, str(e)

    def execute(self, job:Dict[str,Any])->None:
        """Function to actually execute a Python job. This will read job 
        defintions from its meta file, update the meta file and attempt to 
        execute. Some unspecific feedback will be given on execution failure, 
        but depending on what it is it may be up to the job itself to provide 
        more detailed feedback."""
        valid_job(job)

        job_dir = os.path.join(job[PYTHON_EXECUTION_BASE], job[JOB_ID])
        meta_file = os.path.join(job_dir, META_FILE)

        # update the status file with running status
        job[JOB_STATUS] = STATUS_RUNNING
        job[JOB_START_TIME] = datetime.now()
        write_yaml(job, meta_file)

        # execute the job
        try:
            job_function = job[PYTHON_FUNC]
            job_function(job)

            # get up to date job data
            job = read_yaml(meta_file)

            # Update the status file with the finalised status
            job[JOB_STATUS] = STATUS_DONE
            job[JOB_END_TIME] = datetime.now()
            write_yaml(job, meta_file)

        except Exception as e:
            # get up to date job data
            job = read_yaml(meta_file)

            # Update the status file with the error status. Don't overwrite any
            # more specific error messages already created
            if JOB_STATUS not in job:
                job[JOB_STATUS] = STATUS_FAILED
            if JOB_END_TIME not in job:
                job[JOB_END_TIME] = datetime.now()
            if JOB_ERROR not in job:
                job[JOB_ERROR] = f"Job execution failed. {e}"
            write_yaml(job, meta_file)

        # Move the contents of the execution directory to the final output 
        # directory. 
        job_output_dir = os.path.join(job[PYTHON_OUTPUT_DIR], job[JOB_ID])
        shutil.move(job_dir, job_output_dir)
