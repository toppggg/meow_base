
from meow_base.patterns import FileEventPattern
from meow_base.recipes import get_recipe_from_notebook

from shared import run_test, SRSES

def single_rule_single_event_sequential(job_count:int, REPEATS, job_counter, requested_jobs, runtime_start):
    patterns = {}
    pattern = FileEventPattern(
        f"pattern_one",
        f"testing/*",
        "recipe_two",
        "INPUT_FILE",
        parameters={
            "MAX_COUNT":job_count
        }
    )
    patterns[pattern.name] = pattern

    recipe = get_recipe_from_notebook("recipe_two", "sequential.ipynb")
    
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
        signature=SRSES,
        execution=True,
        print_logging=False
    )
