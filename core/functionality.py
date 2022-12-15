
import sys
import inspect

from multiprocessing.connection import Connection, wait as multi_wait
from multiprocessing.queues import Queue
from typing import Union
from random import SystemRandom

from core.meow import BasePattern, BaseRecipe, BaseRule
from core.correctness.validation import check_type, valid_dict, valid_list
from core.correctness.vars import CHAR_LOWERCASE, CHAR_UPPERCASE, \
    VALID_CHANNELS

def check_pattern_dict(patterns, min_length=1):
    valid_dict(patterns, str, BasePattern, strict=False, min_length=min_length)
    for k, v in patterns.items():
        if k != v.name:
            raise KeyError(f"Key '{k}' indexes unexpected Pattern '{v.name}' "
                "Pattern dictionaries must be keyed with the name of the "
                "Pattern.")

def check_recipe_dict(recipes, min_length=1):
    valid_dict(recipes, str, BaseRecipe, strict=False, min_length=min_length)
    for k, v in recipes.items():
        if k != v.name:
            raise KeyError(f"Key '{k}' indexes unexpected Recipe '{v.name}' "
                "Recipe dictionaries must be keyed with the name of the "
                "Recipe.")

def generate_id(prefix:str="", length:int=16, existing_ids:list[str]=[], 
        charset:str=CHAR_UPPERCASE+CHAR_LOWERCASE, attempts:int=24):
    random_length = max(length - len(prefix), 0)
    for _ in range(attempts):
        id = prefix + ''.join(SystemRandom().choice(charset) 
            for _ in range(random_length))
        if id not in existing_ids:
            return id
    raise ValueError(f"Could not generate ID unique from '{existing_ids}' "
        f"using values '{charset}' and length of '{length}'.")

def create_rules(patterns:Union[dict[str,BasePattern],list[BasePattern]], 
        recipes:Union[dict[str,BaseRecipe],list[BaseRecipe]],
        new_rules:list[BaseRule]=[])->dict[str,BaseRule]:
    check_type(patterns, dict, alt_types=[list])
    check_type(recipes, dict, alt_types=[list])
    valid_list(new_rules, BaseRule, min_length=0)

    if isinstance(patterns, list):
        valid_list(patterns, BasePattern, min_length=0)
        patterns = {pattern.name:pattern for pattern in patterns}
    else:
        check_pattern_dict(patterns, min_length=0)

    if isinstance(recipes, list):
        valid_list(recipes, BaseRecipe, min_length=0)
        recipes = {recipe.name:recipe for recipe in recipes}
    else:
        check_recipe_dict(recipes, min_length=0)

    # Imported here to avoid circular imports at top of file
    import rules
    rules = {}
    all_rules ={(r.pattern_type, r.recipe_type):r for r in [r[1] \
        for r in inspect.getmembers(sys.modules["rules"], inspect.isclass) \
        if (issubclass(r[1], BaseRule))]}

    for pattern in patterns.values():
        if pattern.recipe in recipes:
            key = (type(pattern).__name__, 
                type(recipes[pattern.recipe]).__name__)
            if (key) in all_rules:
                rule = all_rules[key](
                    generate_id(prefix="Rule_"), 
                    pattern, 
                    recipes[pattern.recipe]
                )
                rules[rule.name] = rule
    return rules

def wait(inputs:list[VALID_CHANNELS])->list[VALID_CHANNELS]:
    all_connections = [i for i in inputs if type(i) is Connection] \
        + [i._reader for i in inputs if type(i) is Queue]

    ready = multi_wait(all_connections)
    ready_inputs = [i for i in inputs if \
        (type(i) is Connection and i in ready) \
        or (type(i) is Queue and i._reader in ready)]
    return ready_inputs
