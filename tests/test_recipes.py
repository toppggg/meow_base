
import jsonschema
import unittest

from multiprocessing import Pipe

from recipes.jupyter_notebook_recipe import JupyterNotebookRecipe, \
    PapermillHandler
from core.correctness.vars import BAREBONES_NOTEBOOK

class CorrectnessTests(unittest.TestCase):
    def setUp(self)->None:
        return super().setUp()

    def tearDown(self)->None:
        return super().tearDown()

    def testJupyterNotebookRecipeCreationMinimum(self)->None:
        JupyterNotebookRecipe("test_recipe", BAREBONES_NOTEBOOK)

    def testJupyterNotebookRecipeCreationSource(self)->None:
        JupyterNotebookRecipe(
            "test_recipe", BAREBONES_NOTEBOOK, source="notebook.ipynb")

    def testJupyterNotebookRecipeCreationNoName(self)->None:
        with self.assertRaises(ValueError):
            JupyterNotebookRecipe("", BAREBONES_NOTEBOOK)

    def testJupyterNotebookRecipeCreationInvalidName(self)->None:
        with self.assertRaises(ValueError):
            JupyterNotebookRecipe("@test_recipe", BAREBONES_NOTEBOOK)

    def testJupyterNotebookRecipeCreationInvalidRecipe(self)->None:
        with self.assertRaises(jsonschema.exceptions.ValidationError):
            JupyterNotebookRecipe("test_recipe", {})

    def testJupyterNotebookRecipeCreationInvalidSourceExtension(self)->None:
        with self.assertRaises(ValueError):
            JupyterNotebookRecipe(
                "test_recipe", BAREBONES_NOTEBOOK, source="notebook")

    def testJupyterNotebookRecipeCreationInvalidSoureChar(self)->None:
        with self.assertRaises(ValueError):
            JupyterNotebookRecipe(
                "test_recipe", BAREBONES_NOTEBOOK, source="@notebook.ipynb")

    def testJupyterNotebookRecipeSetupName(self)->None:
        name = "name"
        jnr = JupyterNotebookRecipe(name, BAREBONES_NOTEBOOK)
        self.assertEqual(jnr.name, name)

    def testJupyterNotebookRecipeSetupRecipe(self)->None:
        jnr = JupyterNotebookRecipe("name", BAREBONES_NOTEBOOK)
        self.assertEqual(jnr.recipe, BAREBONES_NOTEBOOK)

    def testJupyterNotebookRecipeSetupParameters(self)->None:
        parameters = {
            "a": 1,
            "b": True
        }
        jnr = JupyterNotebookRecipe(
            "name", BAREBONES_NOTEBOOK, parameters=parameters)
        self.assertEqual(jnr.parameters, parameters)

    def testJupyterNotebookRecipeSetupRequirements(self)->None:
        requirements = {
            "a": 1,
            "b": True
        }
        jnr = JupyterNotebookRecipe(
            "name", BAREBONES_NOTEBOOK, requirements=requirements)
        self.assertEqual(jnr.requirements, requirements)

    def testJupyterNotebookRecipeSetupSource(self)->None:
        source = "source.ipynb"
        jnr = JupyterNotebookRecipe(
            "name", BAREBONES_NOTEBOOK, source=source)
        self.assertEqual(jnr.source, source)

    def testPapermillHanderMinimum(self)->None:
        monitor_to_handler_reader, _ = Pipe()

        PapermillHandler([monitor_to_handler_reader])

    def testPapermillHanderStartStop(self)->None:
        monitor_to_handler_reader, _ = Pipe()

        ph = PapermillHandler([monitor_to_handler_reader])
 
        ph.start()
        ph.stop()

    def testPapermillHanderRepeatedStarts(self)->None:
        monitor_to_handler_reader, _ = Pipe()

        ph = PapermillHandler([monitor_to_handler_reader])

        ph.start()
        with self.assertRaises(RuntimeWarning):
            ph.start()
        ph.stop()

    def testPapermillHanderStopBeforeStart(self)->None:
        monitor_to_handler_reader, _ = Pipe()

        ph = PapermillHandler([monitor_to_handler_reader])

        with self.assertRaises(RuntimeWarning):
            ph.stop()


