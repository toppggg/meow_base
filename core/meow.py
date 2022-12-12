
import core.correctness.vars
import core.correctness.validation

from abc import ABC, abstractmethod

from typing import Any

class BaseRecipe:
    name:str
    recipe:Any
    parameters:dict[str, Any]
    requirements:dict[str, Any]
    def __init__(self, name:str, recipe:Any, parameters:dict[str,Any]={}, 
            requirements:dict[str,Any]={}):
        self._is_valid_name(name)
        self.name = name
        self._is_valid_recipe(recipe)
        self.recipe = recipe
        self._is_valid_parameters(parameters)
        self.parameters = parameters
        self._is_valid_requirements(requirements)
        self.requirements = requirements

    def __new__(cls, *args, **kwargs):
        if cls is BaseRecipe:
            raise TypeError("BaseRecipe may not be instantiated directly")
        return object.__new__(cls)

    def _is_valid_name(self, name:str)->None:
        core.correctness.validation.valid_string(
            name, core.correctness.vars.VALID_RECIPE_NAME_CHARS)

    @abstractmethod
    def _is_valid_recipe(self, recipe:Any)->None:
        pass

    @abstractmethod
    def _is_valid_parameters(self, parameters:Any)->None:
        pass

    @abstractmethod
    def _is_valid_requirements(self, requirements:Any)->None:
        pass


class BasePattern(ABC):
    name:str
    recipe:str
    parameters:dict[str, Any]
    outputs:dict[str, Any]
    def __init__(self, name:str, recipe:str, parameters:dict[str,Any]={}, 
            outputs:dict[str,Any]={}):
        self._is_valid_name(name)
        self.name = name
        self._is_valid_recipe(recipe)
        self.recipe = recipe
        self._is_valid_parameters(parameters)
        self.parameters = parameters
        self._is_valid_output(outputs)
        self.outputs = outputs

    def __new__(cls, *args, **kwargs):
        if cls is BasePattern:
            raise TypeError("BasePattern may not be instantiated directly")
        return object.__new__(cls)

    def _is_valid_name(self, name:str)->None:
        core.correctness.validation.valid_string(
            name, core.correctness.vars.VALID_PATTERN_NAME_CHARS)

    @abstractmethod
    def _is_valid_recipe(self, recipe:Any)->None:
        pass

    @abstractmethod
    def _is_valid_parameters(self, parameters:Any)->None:
        pass

    @abstractmethod
    def _is_valid_output(self, outputs:Any)->None:
        pass


class BaseRule(ABC):
    name:str
    pattern:BasePattern
    recipe:BaseRecipe
    pattern_type:str=""
    recipe_type:str=""
    def __init__(self, name:str, pattern:BasePattern, recipe:BaseRecipe):
        self._is_valid_name(name)
        self.name = name
        self._is_valid_pattern(pattern)
        self.pattern = pattern
        self._is_valid_recipe(recipe)
        self.recipe = recipe
        self.__check_types_set()

    def __new__(cls, *args, **kwargs):
        if cls is BaseRule:
            raise TypeError("BaseRule may not be instantiated directly")
        return object.__new__(cls)

    def _is_valid_name(self, name:str)->None:
        core.correctness.validation.valid_string(
            name, core.correctness.vars.VALID_RULE_NAME_CHARS)

    @abstractmethod
    def _is_valid_pattern(self, pattern:Any)->None:
        pass

    @abstractmethod
    def _is_valid_recipe(self, recipe:Any)->None:
        pass

    def __check_types_set(self)->None:
        if self.pattern_type == "":
            raise AttributeError(f"Rule Class '{self.__class__.__name__}' "
                "does not set a pattern_type.")
        if self.recipe_type == "":
            raise AttributeError(f"Rule Class '{self.__class__.__name__}' "
                "does not set a recipe_type.")
