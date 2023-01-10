
from typing import Any

from core.correctness.vars import VALID_RECIPE_NAME_CHARS, \
    VALID_PATTERN_NAME_CHARS, VALID_RULE_NAME_CHARS, VALID_CHANNELS, \
    get_drt_imp_msg
from core.correctness.validation import valid_string, check_type, \
    check_implementation


class BaseRecipe:
    name:str
    recipe:Any
    parameters:dict[str, Any]
    requirements:dict[str, Any]
    def __init__(self, name:str, recipe:Any, parameters:dict[str,Any]={}, 
            requirements:dict[str,Any]={}):
        check_implementation(type(self)._is_valid_recipe, BaseRecipe)
        check_implementation(type(self)._is_valid_parameters, BaseRecipe)
        check_implementation(type(self)._is_valid_requirements, BaseRecipe)
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
    parameters:dict[str,Any]
    outputs:dict[str,Any]
    def __init__(self, name:str, recipe:str, parameters:dict[str,Any]={}, 
            outputs:dict[str,Any]={}):
        check_implementation(type(self)._is_valid_recipe, BasePattern)
        check_implementation(type(self)._is_valid_parameters, BasePattern)
        check_implementation(type(self)._is_valid_output, BasePattern)
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
        check_implementation(type(self)._is_valid_pattern, BaseRule)
        check_implementation(type(self)._is_valid_recipe, BaseRule)
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
    report: VALID_CHANNELS
    def __init__(self, rules:dict[str,BaseRule], 
            report:VALID_CHANNELS)->None:
        check_implementation(type(self).start, BaseMonitor)
        check_implementation(type(self).stop, BaseMonitor)
        check_implementation(type(self)._is_valid_report, BaseMonitor)
        check_implementation(type(self)._is_valid_rules, BaseMonitor)
        self._is_valid_report(report)
        self.report = report
        self._is_valid_rules(rules)
        self.rules = rules
        
    def __new__(cls, *args, **kwargs):
        if cls is BaseMonitor:
            msg = get_drt_imp_msg(BaseMonitor)
            raise TypeError(msg)
        return object.__new__(cls)

    def _is_valid_report(self, report:VALID_CHANNELS)->None:
        pass

    def _is_valid_rules(self, rules:dict[str,BaseRule])->None:
        pass

    def start(self)->None:
        pass

    def stop(self)->None:
        pass


class BaseHandler:
    inputs:Any
    def __init__(self, inputs:list[VALID_CHANNELS]) -> None:
        check_implementation(type(self).start, BaseHandler)
        check_implementation(type(self).stop, BaseHandler)
        check_implementation(type(self).handle, BaseHandler)
        check_implementation(type(self)._is_valid_inputs, BaseHandler)
        self._is_valid_inputs(inputs)
        self.inputs = inputs

    def __new__(cls, *args, **kwargs):
        if cls is BaseHandler:
            msg = get_drt_imp_msg(BaseHandler)
            raise TypeError(msg)
        return object.__new__(cls)

    def _is_valid_inputs(self, inputs:Any)->None:
        pass

    def handle(self, event:Any, rule:BaseRule)->None:
        pass

    def start(self)->None:
        pass

    def stop(self)->None:
        pass


class MeowRunner:
    monitor:BaseMonitor
    handler:BaseHandler
    def __init__(self, monitor:BaseMonitor, handler:BaseHandler) -> None:
        self._is_valid_monitor(monitor)
        self.monitor = monitor
        self._is_valid_handler(handler)
        self.handler = handler

    def start(self)->None:
        self.monitor.start()
        if hasattr(self.handler, "start"):
            self.handler.start()

    def stop(self)->None:
        self.monitor.stop()
        if hasattr(self.handler, "stop"):
            self.handler.stop()

    def _is_valid_monitor(self, monitor:BaseMonitor)->None:
        check_type(monitor, BaseMonitor)

    def _is_valid_handler(self, handler:BaseHandler)->None:
        check_type(handler, BaseHandler)
