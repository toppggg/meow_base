
from typing import Any

from core.correctness.validation import check_input, valid_string, valid_dict
from core.correctness.vars import VALID_RECIPE_NAME_CHARS, \
    VALID_VARIABLE_NAME_CHARS
from core.meow import BasePattern

class FileEventPattern(BasePattern):
    triggering_path:str
    triggering_file:str

    def __init__(self, name:str, triggering_path:str, recipe:str, 
            triggering_file:str, parameters:dict[str,Any]={}, 
            outputs:dict[str,Any]={}):
        super().__init__(name, recipe, parameters, outputs)
        self._is_valid_triggering_path(triggering_path)
        self.triggering_path = triggering_path
        self._is_valid_triggering_file(triggering_file)
        self.triggering_file = triggering_file

    def _is_valid_recipe(self, recipe:str)->None:
        valid_string(recipe, VALID_RECIPE_NAME_CHARS)

    def _is_valid_triggering_path(self, triggering_path:str)->None:
        check_input(triggering_path, str)
        if len(triggering_path) < 1:
            raise ValueError (
                f"trigginering path '{triggering_path}' is too short. " 
                "Minimum length is 1"
        )

    def _is_valid_triggering_file(self, triggering_file:str)->None:
        valid_string(triggering_file, VALID_VARIABLE_NAME_CHARS)

    def _is_valid_parameters(self, parameters:dict[str,Any])->None:
        valid_dict(parameters, str, Any, strict=False)
        for k in parameters.keys():
            valid_string(k, VALID_VARIABLE_NAME_CHARS)

    def _is_valid_output(self, outputs:dict[str,str])->None:
        valid_dict(outputs, str, str, strict=False)
        for k in outputs.keys():
            valid_string(k, VALID_VARIABLE_NAME_CHARS)
