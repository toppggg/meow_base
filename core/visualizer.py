import time
import sys
import os

from typing import Any, Union, Dict

from core.base_pattern import BasePattern
from core.base_recipe import BaseRecipe
from core.base_rule import BaseRule
from core.correctness.vars import get_drt_imp_msg, VALID_CHANNELS

sys.path.append("C:\\Users\\Johan\OneDrive\\Universitet\\Datalogi\\6. semester\\Bachelor\\meow_base")

class Visualizer:
    # A collection of patterns
    _patterns: Dict[str, BasePattern]
    # A collection of recipes
    _recipes: Dict[str, BaseRecipe] # Might not be needed?
    # A collection of rules derived from _patterns and _recipes
    _rules: Dict[str, BaseRule] # Might not be needed?

    _data_to_show: Dict[str, Any] # use rule.rule to match, and increase the int by 1

    def __init__(self)->None:
        pass
        # self.update()
    
    def update(self):
        while True:
            # self.from_runner()
            print(self._data_to_show)
            time.sleep(0.1)

    def from_runner(self, event:Dict[str,Any])->None:
        
        visualizer_dir = "visualizer_print"
        with open(os.path.join(visualizer_dir, "print"), "w") as f:
            f.write(str(event))
            f.write(event["rule"].__str__())