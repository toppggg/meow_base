
import nbformat

from typing import Any

from core.correctness.validation import check_input, valid_string, valid_dict
from core.correctness.vars import VALID_JUPYTER_NOTEBOOK_FILENAME_CHARS, \
    VALID_JUPYTER_NOTEBOOK_EXTENSIONS, VALID_VARIABLE_NAME_CHARS
from core.meow import BaseRecipe

class JupyterNotebookRecipe(BaseRecipe):
    source:str
    def __init__(self, name:str, recipe:Any, parameters:dict[str,Any]={}, 
            requirements:dict[str,Any]={}, source:str=""):
        super().__init__(name, recipe, parameters, requirements)
        self._is_valid_source(source)
        self.source = source

    def _is_valid_source(self, source:str)->None:
        valid_string(
            source, VALID_JUPYTER_NOTEBOOK_FILENAME_CHARS, min_length=0)

        if not source:
            return

        matched = False
        for i in VALID_JUPYTER_NOTEBOOK_EXTENSIONS:
            if source.endswith(i):
                matched = True
        if not matched:
            raise ValueError(f"source '{source}' does not end with a valid "
                "jupyter notebook extension.")

    def _is_valid_recipe(self, recipe:dict[str,Any])->None:
        check_input(recipe, dict)
        nbformat.validate(recipe)

    def _is_valid_parameters(self, parameters:dict[str,Any])->None:
        valid_dict(parameters, str, Any, strict=False)
        for k in parameters.keys():
            valid_string(k, VALID_VARIABLE_NAME_CHARS)

    def _is_valid_requirements(self, requirements:dict[str,Any])->None:
        valid_dict(requirements, str, Any, strict=False)
        for k in requirements.keys():
            valid_string(k, VALID_VARIABLE_NAME_CHARS)
