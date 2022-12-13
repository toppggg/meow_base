
from multiprocessing.connection import Connection
from typing import Any

from core.correctness.vars import VALID_RECIPE_NAME_CHARS, \
    VALID_PATTERN_NAME_CHARS, VALID_RULE_NAME_CHARS, \
    get_not_imp_msg, get_drt_imp_msg
from core.correctness.validation import valid_string


class BaseRecipe:
    name:str
    recipe:Any
    parameters:dict[str, Any]
    requirements:dict[str, Any]
    def __init__(self, name:str, recipe:Any, parameters:dict[str,Any]={}, 
            requirements:dict[str,Any]={}):
        if (type(self)._is_valid_recipe == BaseRecipe._is_valid_recipe):
            msg = get_not_imp_msg(BaseRecipe, BaseRecipe._is_valid_recipe)
            raise NotImplementedError(msg)
        if (type(self)._is_valid_parameters == BaseRecipe._is_valid_parameters):
            msg = get_not_imp_msg(BaseRecipe, BaseRecipe._is_valid_parameters)
            raise NotImplementedError(msg)
        if (type(self)._is_valid_requirements == BaseRecipe._is_valid_requirements):
            msg = get_not_imp_msg(BaseRecipe, BaseRecipe._is_valid_requirements)
            raise NotImplementedError(msg)
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
            msg = get_drt_imp_msg(BaseRecipe)
            raise TypeError(msg)
        return object.__new__(cls)

    def _is_valid_name(self, name:str)->None:
        valid_string(name, VALID_RECIPE_NAME_CHARS)

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
        if (type(self)._is_valid_recipe == BasePattern._is_valid_recipe):
            msg = get_not_imp_msg(BasePattern, BasePattern._is_valid_recipe)
            raise NotImplementedError(msg)
        if (type(self)._is_valid_parameters == BasePattern._is_valid_parameters):
            msg = get_not_imp_msg(BasePattern, BasePattern._is_valid_parameters)
            raise NotImplementedError(msg)
        if (type(self)._is_valid_output == BasePattern._is_valid_output):
            msg = get_not_imp_msg(BasePattern, BasePattern._is_valid_output)
            raise NotImplementedError(msg)
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
            msg = get_drt_imp_msg(BasePattern)
            raise TypeError(msg)
        return object.__new__(cls)

    def _is_valid_name(self, name:str)->None:
        valid_string(name, VALID_PATTERN_NAME_CHARS)

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
    pattern_type:str=""
    recipe_type:str=""
    def __init__(self, name:str, pattern:BasePattern, recipe:BaseRecipe):
        if (type(self)._is_valid_pattern == BaseRule._is_valid_pattern):
            msg = get_not_imp_msg(BaseRule, BaseRule._is_valid_pattern)
            raise NotImplementedError(msg)
        if (type(self)._is_valid_recipe == BaseRule._is_valid_recipe):
            msg = get_not_imp_msg(BaseRule, BaseRule._is_valid_recipe)
            raise NotImplementedError(msg)

        self._is_valid_name(name)
        self.name = name
        self._is_valid_pattern(pattern)
        self.pattern = pattern
        self._is_valid_recipe(recipe)
        self.recipe = recipe
        self.__check_types_set()

    def __new__(cls, *args, **kwargs):
        if cls is BaseRule:
            msg = get_drt_imp_msg(BaseRule)
            raise TypeError(msg)
        return object.__new__(cls)

    def _is_valid_name(self, name:str)->None:
        valid_string(name, VALID_RULE_NAME_CHARS)

    def _is_valid_pattern(self, pattern:Any)->None:
        pass

    def _is_valid_recipe(self, recipe:Any)->None:
        pass

    def __check_types_set(self)->None:
        if self.pattern_type == "":
            raise AttributeError(f"Rule Class '{self.__class__.__name__}' "
                "does not set a pattern_type.")
        if self.recipe_type == "":
            raise AttributeError(f"Rule Class '{self.__class__.__name__}' "
                "does not set a recipe_type.")


class BaseMonitor:
    rules: dict[str, BaseRule]
    report: Connection
    listen: Connection
    def __init__(self, rules:dict[str, BaseRule], report:Connection, 
            listen:Connection) -> None:
        if (type(self).start == BaseMonitor.start):
            msg = get_not_imp_msg(BaseMonitor, BaseMonitor.start)
            raise NotImplementedError(msg)
        if (type(self).stop == BaseMonitor.stop):
            msg = get_not_imp_msg(BaseMonitor, BaseMonitor.stop)
            raise NotImplementedError(msg)
        if (type(self)._is_valid_report == BaseMonitor._is_valid_report):
            msg = get_not_imp_msg(BaseMonitor, BaseMonitor._is_valid_report)
            raise NotImplementedError(msg)
        self._is_valid_report(report)
        self.report = report
        if (type(self)._is_valid_listen == BaseMonitor._is_valid_listen):
            msg = get_not_imp_msg(BaseMonitor, BaseMonitor._is_valid_listen)
            raise NotImplementedError(msg)
        self._is_valid_listen(listen)
        self.listen = listen
        if (type(self)._is_valid_rules == BaseMonitor._is_valid_rules):
            msg = get_not_imp_msg(BaseMonitor, BaseMonitor._is_valid_rules)
            raise NotImplementedError(msg)
        self._is_valid_rules(rules)
        self.rules = rules
        
    def __new__(cls, *args, **kwargs):
        if cls is BaseMonitor:
            msg = get_drt_imp_msg(BaseMonitor)
            raise TypeError(msg)
        return object.__new__(cls)

    def _is_valid_report(self, report:Connection)->None:
        pass

    def _is_valid_listen(self, listen:Connection)->None:
        pass

    def _is_valid_rules(self, rules:dict[str, BaseRule])->None:
        pass

    def start(self)->None:
        pass

    def stop(self)->None:
        pass


class BaseHandler:
    inputs:Any
    def __init__(self, inputs:Any) -> None:
        if (type(self).handle == BaseHandler.handle):
            msg = get_not_imp_msg(BaseHandler, BaseHandler.handle)
            raise NotImplementedError(msg)
        if (type(self)._is_valid_inputs == BaseHandler._is_valid_inputs):
            msg = get_not_imp_msg(BaseHandler, BaseHandler._is_valid_inputs)
            raise NotImplementedError(msg)
        self._is_valid_inputs(inputs)
        self.inputs = inputs

    def __new__(cls, *args, **kwargs):
        if cls is BaseHandler:
            msg = get_drt_imp_msg(BaseHandler)
            raise TypeError(msg)
        return object.__new__(cls)

    def _is_valid_inputs(self, inputs:Any)->None:
        pass

    def handle()->None:
        pass
