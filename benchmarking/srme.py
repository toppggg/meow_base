
from meow_base.patterns import FileEventPattern
from meow_base.recipes import getRecipeFromNotebook

from shared import run_test, SRME

def single_rule_multiple_events(job_count:int, REPEATS, job_counter, requested_jobs, runtime_start):
    patterns = {}
    pattern = FileEventPattern(
        f"pattern_one",
        f"testing/*",
        "recipe_one",
        "input"
    )
    patterns[pattern.name] = pattern

    recipe = getRecipeFromNotebook("recipe_one", "test.ipynb")
    
    recipes = {
        recipe.name: recipe
    }

    run_test(
        patterns, 
        recipes, 
        job_count, 
        job_count,
        REPEATS, 
        job_counter,
        requested_jobs,
        runtime_start,
        signature=SRME
    )
