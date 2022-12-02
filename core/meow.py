
import core.correctness.vars
import core.correctness.validation

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

    def __init_subclass__(cls, **kwargs) -> None:
        if cls._is_valid_recipe == BaseRecipe._is_valid_recipe:
            raise NotImplementedError(
                f"Recipe '{cls.__name__}' has not implemented "
                "'_is_valid_recipe(self, recipe)' function.")
        if cls._is_valid_parameters == BaseRecipe._is_valid_parameters:
            raise NotImplementedError(
                f"Recipe '{cls.__name__}' has not implemented "
                "'_is_valid_parameters(self, parameters)' function.")
        if cls._is_valid_requirements == BaseRecipe._is_valid_requirements:
            raise NotImplementedError(
                f"Recipe '{cls.__name__}' has not implemented "
                "'_is_valid_requirements(self, requirements)' function.")
        super().__init_subclass__(**kwargs)

    def __new__(cls, *args, **kwargs):
        if cls is BaseRecipe:
            raise TypeError("BaseRecipe may not be instantiated directly")
        return object.__new__(cls)

    def _is_valid_name(self, name:str)->None:
        core.correctness.validation.valid_string(
            name, core.correctness.vars.VALID_RECIPE_NAME_CHARS)

    def _is_valid_recipe(self, recipe:Any)->None:
        pass

    def _is_valid_parameters(self, parameters:Any)->None:
        pass

    def _is_valid_requirements(self, requirements:Any)->None:
        pass


class BasePattern:
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

    def __init_subclass__(cls, **kwargs) -> None:
        if cls._is_valid_recipe == BasePattern._is_valid_recipe:
            raise NotImplementedError(
                f"Pattern '{cls.__name__}' has not implemented "
                "'_is_valid_recipe(self, recipe)' function.")
        if cls._is_valid_parameters == BasePattern._is_valid_parameters:
            raise NotImplementedError(
                f"Pattern '{cls.__name__}' has not implemented "
                "'_is_valid_parameters(self, parameters)' function.")
        if cls._is_valid_output == BasePattern._is_valid_output:
            raise NotImplementedError(
                f"Pattern '{cls.__name__}' has not implemented "
                "'_is_valid_output(self, outputs)' function.")
        super().__init_subclass__(**kwargs)

    def __new__(cls, *args, **kwargs):
        if cls is BasePattern:
            raise TypeError("BasePattern may not be instantiated directly")
        return object.__new__(cls)

    def _is_valid_name(self, name:str)->None:
        core.correctness.validation.valid_string(
            name, core.correctness.vars.VALID_PATTERN_NAME_CHARS)

    def _is_valid_recipe(self, recipe:Any)->None:
        pass

    def _is_valid_parameters(self, parameters:Any)->None:
        pass

    def _is_valid_output(self, outputs:Any)->None:
        pass


class BaseRule:
    name:str
    pattern:BasePattern
    recipe:BaseRecipe
    def __init__(self, name:str, pattern:BasePattern, recipe:BaseRecipe):
        self._is_valid_name(name)
        self.name = name
        self._is_valid_pattern(pattern)
        self.pattern = pattern
        self._is_valid_recipe(recipe)
        self.recipe = recipe

    def __new__(cls, *args, **kwargs):
        if cls is BaseRule:
            raise TypeError("BaseRule may not be instantiated directly")
        return object.__new__(cls)

    def __init_subclass__(cls, **kwargs) -> None:
        if cls._is_valid_pattern == BaseRule._is_valid_pattern:
            raise NotImplementedError(
                f"Rule '{cls.__name__}' has not implemented "
                "'_is_valid_pattern(self, pattern)' function.")
        if cls._is_valid_recipe == BaseRule._is_valid_recipe:
            raise NotImplementedError(
                f"Pattern '{cls.__name__}' has not implemented "
                "'_is_valid_recipe(self, recipe)' function.")
        super().__init_subclass__(**kwargs)

    def _is_valid_name(self, name:str)->None:
        core.correctness.validation.valid_string(
            name, core.correctness.vars.VALID_RULE_NAME_CHARS)

    def _is_valid_pattern(self, pattern:Any)->None:
        pass

    def _is_valid_recipe(self, recipe:Any)->None:
        pass
