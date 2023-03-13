
from meow_base.patterns import FileEventPattern
from meow_base.recipes import getRecipeFromNotebook

from meow_base.functionality.meow import create_parameter_sweep
from shared import run_test, SRSEP

def single_rule_single_event_parallel(job_count:int, REPEATS, job_counter, requested_jobs, runtime_start):
    patterns = {}
    pattern = FileEventPattern(
        f"pattern_one",
        f"testing/*",
        "recipe_one",
        "input",
        sweep=create_parameter_sweep("var", 1, job_count, 1)
    )
    patterns[pattern.name] = pattern

    recipe = getRecipeFromNotebook("recipe_one", "test.ipynb")
    
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