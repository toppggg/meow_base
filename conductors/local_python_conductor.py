
"""
This file contains definitions for the LocalPythonConductor, in order to 
execute Python jobs on the local resource.

Author(s): David Marchant
"""
from typing import Any

from core.correctness.vars import PYTHON_TYPE, PYTHON_FUNC
from core.correctness.validation import valid_job
from core.meow import BaseConductor


class LocalPythonConductor(BaseConductor):
    def __init__(self)->None:
        super().__init__()

    def valid_job_types(self)->list[str]:
        return [PYTHON_TYPE]

    # TODO expand with more feedback
    def execute(self, job:dict[str,Any])->None:
        valid_job(job)

        job_function = job[PYTHON_FUNC]
        job_function(job)

        return
