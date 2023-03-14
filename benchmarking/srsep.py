
from meow_base.patterns import FileEventPattern
from meow_base.recipes import get_recipe_from_notebook

from meow_base.functionality.meow import create_parameter_sweep
from shared import run_test, SRSEP

def single_rule_single_event_parallel(job_count:int, REPEATS:int, 
        job_counter:int, requested_jobs:int, runtime_start:float):
    patterns = {}
    pattern = FileEventPattern(
        f"pattern_one",
        f"testing/*",
        "recipe_one",
        "input",
        sweep=create_parameter_sweep("var", 1, job_count, 1)
    )
    patterns[pattern.name] = pattern

    recipe = get_recipe_from_notebook("recipe_one", "test.ipynb")
    
    recipes = {
        recipe.name: recipe
    }

    run_test(
        patterns, 
        recipes, 
        1, 
        job_count,
        REPEATS, 
        job_counter,
        requested_jobs,
        runtime_start,
        signature=SRSEP
    )