
from meow_base.patterns import FileEventPattern
from meow_base.recipes import getRecipeFromNotebook

from shared import run_test, MRSE

def multiple_rules_single_event(job_count:int, REPEATS, job_counter, requested_jobs, runtime_start):
    patterns = {}
    for i in range(job_count):
        pattern = FileEventPattern(
            f"pattern_{i}",
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
        1, 
        job_count,
        REPEATS, 
        job_counter,
        requested_jobs,
        runtime_start,
        signature=MRSE
    )
