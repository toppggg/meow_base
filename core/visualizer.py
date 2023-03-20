import time
import sys
import os

from typing import Any, Union, Dict

from meow_base.core.base_recipe import BaseRecipe
from meow_base.core.base_rule import BaseRule
from meow_base.core.correctness.vars import get_drt_imp_msg, VALID_CHANNELS
from meow_base.core.base_pattern import BasePattern

sys.path.append("C:\\Users\\Johan\OneDrive\\Universitet\\Datalogi\\6. semester\\Bachelor\meow")

class Visualizer:
    # A collection of patterns
    _patterns: Dict[str, BasePattern]
    # A collection of recipes
    _recipes: Dict[str, BaseRecipe] # Might not be needed?
    # A collection of rules derived from _patterns and _recipes
    _rules: Dict[str, BaseRule] # Might not be needed?
    
    _data_to_show: Dict[str, Any] # use rule.rule to match, and increase the int by 1

    _visualized_seconds_array: Dict[BaseRule, list]

    def __init__(self)->None:
        pass
        # self.update()
    
    def update(self):
        while True:
            # self.from_runner()
            print(self._data_to_show)
            time.sleep(0.1)

    def from_runner(self, event:Dict[str,Any])->None:
        
        if event["rule"] not in self._rules: #Check if the rule already exists, otherwise add it to the rules that should be visualized
            self.new_rule(event["rule"])

        visualizer_dir = "visualizer_print"
        with open(os.path.join(visualizer_dir, "print"), "a") as f:
            f.write(str(event))
            f.write(event["rule"].__str__() + "\n")
        


    #Johans leg


    def new_rule(self, rule: BaseRule)->None:
        self._visualized_seconds_array[rule] = [0] * 60
        pass