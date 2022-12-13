
import unittest

from core.correctness.vars import BAREBONES_NOTEBOOK
from patterns.file_event_pattern import FileEventPattern
from recipes.jupyter_notebook_recipe import JupyterNotebookRecipe
from rules.file_event_jupyter_notebook_rule import FileEventJupyterNotebookRule

class CorrectnessTests(unittest.TestCase):
    def setUp(self) -> None:
        return super().setUp()

    def tearDown(self) -> None:
        return super().tearDown()

    def testFileEventJupyterNotebookRuleCreationMinimum(self)->None:
        fep = FileEventPattern("name", "path", "recipe", "file")
        jnr = JupyterNotebookRecipe("recipe", BAREBONES_NOTEBOOK)

        FileEventJupyterNotebookRule("name", fep, jnr)

    def testFileEventJupyterNotebookRuleCreationNoName(self)->None:
        fep = FileEventPattern("name", "path", "recipe", "file")
        jnr = JupyterNotebookRecipe("recipe", BAREBONES_NOTEBOOK)

        with self.assertRaises(ValueError):
            FileEventJupyterNotebookRule("", fep, jnr)

    def testFileEventJupyterNotebookRuleCreationInvalidName(self)->None:
        fep = FileEventPattern("name", "path", "recipe", "file")
        jnr = JupyterNotebookRecipe("recipe", BAREBONES_NOTEBOOK)

        with self.assertRaises(TypeError):
            FileEventJupyterNotebookRule(1, fep, jnr)

    def testFileEventJupyterNotebookRuleCreationInvalidPattern(self)->None:
        jnr = JupyterNotebookRecipe("recipe", BAREBONES_NOTEBOOK)

        with self.assertRaises(TypeError):
            FileEventJupyterNotebookRule("name", "pattern", jnr)

    def testFileEventJupyterNotebookRuleCreationInvalidRecipe(self)->None:
        fep = FileEventPattern("name", "path", "recipe", "file")

        with self.assertRaises(TypeError):
            FileEventJupyterNotebookRule("name", fep, "recipe")

    def testFileEventJupyterNotebookRuleCreationMissmatchedRecipe(self)->None:
        fep = FileEventPattern("name", "path", "recipe", "file")
        jnr = JupyterNotebookRecipe("test_recipe", BAREBONES_NOTEBOOK)

        with self.assertRaises(ValueError):
            FileEventJupyterNotebookRule("name", fep, jnr)

    def testFileEventJupyterNotebookRuleSetupName(self)->None:
        name = "name"
        fep = FileEventPattern("name", "path", "recipe", "file")
        jnr = JupyterNotebookRecipe("recipe", BAREBONES_NOTEBOOK)

        fejnr = FileEventJupyterNotebookRule(name, fep, jnr)

        self.assertEqual(fejnr.name, name)

    def testFileEventJupyterNotebookRuleSetupPattern(self)->None:
        fep = FileEventPattern("name", "path", "recipe", "file")
        jnr = JupyterNotebookRecipe("recipe", BAREBONES_NOTEBOOK)

        fejnr = FileEventJupyterNotebookRule("name", fep, jnr)

        self.assertEqual(fejnr.pattern, fep)

    def testFileEventJupyterNotebookRuleSetupRecipe(self)->None:
        fep = FileEventPattern("name", "path", "recipe", "file")
        jnr = JupyterNotebookRecipe("recipe", BAREBONES_NOTEBOOK)

        fejnr = FileEventJupyterNotebookRule("name", fep, jnr)

        self.assertEqual(fejnr.recipe, jnr)

