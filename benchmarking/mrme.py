
from meow_base.patterns import FileEventPattern
from meow_base.recipes import get_recipe_from_notebook

from shared import run_test, MRME

def multiple_rules_multiple_events(job_count:int, REPEATS, job_counter, requested_jobs, runtime_start):
    patterns = {}
    for i in range(job_count):
        pattern = FileEventPattern(
            f"pattern_{i}",
            f"testing/file_{i}.txt",
            "recipe_one",
            "input"
        )
        patterns[pattern.name] = pattern

    recipe = get_recipe_from_notebook("recipe_one", "test.ipynb")
    
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
        signature=MRME
    )
