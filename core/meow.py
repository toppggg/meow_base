
import core.correctness.vars
import core.correctness.validation

from typing import Any

class BaseRecipe:
    name: str
    recipe: Any
    paramaters: dict[str, Any]
    def __init__(self, name:str, recipe:Any, parameters:dict[str,Any]={}):
        self.__is_valid_name(name)
        self.name = name
        self.__is_valid_recipe(recipe)
        self.recipe = recipe
        self.__is_valid_parameters(parameters)
        self.paramaters = parameters

    def __new__(cls, *args, **kwargs):
        if cls is BaseRecipe:
            raise TypeError("BaseRecipe may not be instantiated directly")

    def __is_valid_name(self, name):
        core.correctness.validation.valid_string(
            name, core.correctness.vars.VALID_RECIPE_NAME_CHARS)

    def __is_valid_recipe(self, recipe):
        raise NotImplementedError(
            f"Recipe '{self.__class__.__name__}' has not implemented "
            "'__is_valid_recipe(self, recipe)' function.")

    def __is_valid_parameters(self, parameters):
        raise NotImplementedError(
            f"Recipe '{self.__class__.__name__}' has not implemented "
            "'__is_valid_parameters(self, parameters)' function.")

class BasePattern:
    name: str
    recipe: BaseRecipe
    parameters: dict[str, Any]
    outputs: dict[str, Any]
    def __init__(self, name:str, recipe:BaseRecipe, 
            parameters:dict[str,Any]={}, outputs:dict[str,Any]={}):
        self.__is_valid_name(name)
        self.name = name
        self.__is_valid_recipe(recipe)
        self.recipe = recipe
        self.__is_valid_parameters(parameters)
        self.paramaters = parameters
        self.__is_valid_output(outputs)
        self.outputs = outputs

    def __new__(cls, *args, **kwargs):
        if cls is BasePattern:
            raise TypeError("BasePattern may not be instantiated directly")

    def __is_valid_name(self, name):
        core.correctness.validation.valid_string(
            name, core.correctness.vars.VALID_PATTERN_NAME_CHARS)

    def __is_valid_recipe(self, recipe):
        raise NotImplementedError(
            f"Pattern '{self.__class__.__name__}' has not implemented "
            "'__is_valid_recipe(self, recipe)' function.")

    def __is_valid_parameters(self, parameters):
        raise NotImplementedError(
            f"Pattern '{self.__class__.__name__}' has not implemented "
            "'__is_valid_parameters(self, parameters)' function.")

    def __is_valid_output(self, outputs):
        raise NotImplementedError(
            f"Pattern '{self.__class__.__name__}' has not implemented "
            "'__is_valid_output(self, outputs)' function.")

class BaseRule:
    name: str
    patterns: list[BasePattern]
    def __init__(self, name:str, patterns:list[BasePattern]):
        self.__is_valid_name(name)
        self.name = name
        self.__is_valid_patterns(patterns)
        self.patterns = patterns

    def __new__(cls, *args, **kwargs):
        if cls is BaseRule:
            raise TypeError("BaseRule may not be instantiated directly")

    def __is_valid_name(self, name):
        core.correctness.validation.valid_string(
            name, core.correctness.vars.VALID_RULE_NAME_CHARS)

    def __is_valid_patterns(self, patterns):
        raise NotImplementedError(
            f"Rule '{self.__class__.__name__}' has not implemented "
            "'__is_valid_patterns(self, patterns)' function.")
