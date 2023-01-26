
from core.correctness.validation import check_type
from core.meow import BaseRule
from patterns.file_event_pattern import FileEventPattern
from recipes.jupyter_notebook_recipe import JupyterNotebookRecipe

class FileEventJupyterNotebookRule(BaseRule):
    pattern_type = "FileEventPattern"
    recipe_type = "JupyterNotebookRecipe" 
    def __init__(self, name: str, pattern:FileEventPattern, 
            recipe:JupyterNotebookRecipe):
        super().__init__(name, pattern, recipe)
        if pattern.recipe != recipe.name:
            raise ValueError(f"Cannot create Rule {name}. Pattern "
                f"{pattern.name} does not identify Recipe {recipe.name}. It "
                f"uses {pattern.recipe}")

    def _is_valid_pattern(self, pattern:FileEventPattern)->None:
        check_type(pattern, FileEventPattern)

    def _is_valid_recipe(self, recipe:JupyterNotebookRecipe)->None:
        check_type(recipe, JupyterNotebookRecipe)
