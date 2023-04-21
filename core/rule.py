
"""
This file contains the MEOW rule defintion.

Author(s): David Marchant
"""

from sys import modules
from typing import Any

if "BasePattern" not in modules:
    from meow_base.core.base_pattern import BasePattern
if "BaseRecipe" not in modules:
    from meow_base.core.base_recipe import BaseRecipe
from meow_base.core.vars import VALID_RULE_NAME_CHARS, \
    get_drt_imp_msg
from meow_base.functionality.validation import valid_string, check_type, \
    check_implementation
from meow_base.functionality.naming import generate_rule_id


class Rule:
    # A unique identifier for the rule
    name:str
    # A pattern to be used in rule triggering
    pattern:BasePattern
    # A recipe to be used in rule execution
    recipe:BaseRecipe
    def __init__(self, pattern:BasePattern, recipe:BaseRecipe, name:str=""):
        """Rule Constructor. This will check that any class inheriting 
        from it implements its validation functions. It will then call these on
        the input parameters."""
        if not name:
            name = generate_rule_id()
        self._is_valid_name(name)
        self.name = name
        check_type(pattern, BasePattern, hint="Rule.pattern")
        self.pattern = pattern
        check_type(recipe, BaseRecipe, hint="Rule.recipe")
        self.recipe = recipe
        if pattern.recipe != recipe.name:
            raise ValueError(f"Cannot create Rule {name}. Pattern "
                f"{pattern.name} does not identify Recipe {recipe.name}. It "
                f"uses {pattern.recipe}")

    def _is_valid_name(self, name:str)->None:
        """Validation check for 'name' variable from main constructor. Is 
        automatically called during initialisation."""
        valid_string(name, VALID_RULE_NAME_CHARS)
