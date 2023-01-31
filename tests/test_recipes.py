
import jsonschema
import os
import unittest

from multiprocessing import Pipe

from core.correctness.vars import BAREBONES_NOTEBOOK, TEST_HANDLER_BASE, \
    TEST_JOB_OUTPUT, TEST_MONITOR_BASE, COMPLETE_NOTEBOOK, EVENT_TYPE, \
    WATCHDOG_BASE, WATCHDOG_RULE, WATCHDOG_TYPE, EVENT_PATH, SHA256, \
    WATCHDOG_HASH, JOB_ID, PYTHON_TYPE, JOB_PARAMETERS, JOB_HASH, \
    PYTHON_FUNC, PYTHON_OUTPUT_DIR, PYTHON_EXECUTION_BASE, \
    APPENDING_NOTEBOOK, META_FILE, BASE_FILE, PARAMS_FILE, JOB_FILE, \
    RESULT_FILE
from core.correctness.validation import valid_job
from core.functionality import get_file_hash, create_job, create_event
from core.meow import create_rules, create_rule
from patterns.file_event_pattern import FileEventPattern
from recipes.jupyter_notebook_recipe import JupyterNotebookRecipe, \
    PapermillHandler, job_func
from rules.file_event_jupyter_notebook_rule import FileEventJupyterNotebookRule
from shared import setup, teardown

class CorrectnessTests(unittest.TestCase):
    def setUp(self)->None:
        super().setUp()
        setup()

    def tearDown(self)->None:
        super().tearDown()
        teardown()

    # Test JupyterNotebookRecipe can be created
    def testJupyterNotebookRecipeCreationMinimum(self)->None:
        JupyterNotebookRecipe("test_recipe", BAREBONES_NOTEBOOK)

    # Test JupyterNotebookRecipe can be created with source
    def testJupyterNotebookRecipeCreationSource(self)->None:
        JupyterNotebookRecipe(
            "test_recipe", BAREBONES_NOTEBOOK, source="notebook.ipynb")

    # Test JupyterNotebookRecipe cannot be created without name
    def testJupyterNotebookRecipeCreationNoName(self)->None:
        with self.assertRaises(ValueError):
            JupyterNotebookRecipe("", BAREBONES_NOTEBOOK)

    # Test JupyterNotebookRecipe cannot be created with invalid name
    def testJupyterNotebookRecipeCreationInvalidName(self)->None:
        with self.assertRaises(ValueError):
            JupyterNotebookRecipe("@test_recipe", BAREBONES_NOTEBOOK)

    # Test JupyterNotebookRecipe cannot be created with invalid recipe
    def testJupyterNotebookRecipeCreationInvalidRecipe(self)->None:
        with self.assertRaises(jsonschema.exceptions.ValidationError):
            JupyterNotebookRecipe("test_recipe", {})

    # Test JupyterNotebookRecipe cannot be created with invalid source
    def testJupyterNotebookRecipeCreationInvalidSourceExtension(self)->None:
        with self.assertRaises(ValueError):
            JupyterNotebookRecipe(
                "test_recipe", BAREBONES_NOTEBOOK, source="notebook")

    # Test JupyterNotebookRecipe cannot be created with invalid source name
    def testJupyterNotebookRecipeCreationInvalidSoureChar(self)->None:
        with self.assertRaises(ValueError):
            JupyterNotebookRecipe(
                "test_recipe", BAREBONES_NOTEBOOK, source="@notebook.ipynb")

    # Test JupyterNotebookRecipe name setup correctly
    def testJupyterNotebookRecipeSetupName(self)->None:
        name = "name"
        jnr = JupyterNotebookRecipe(name, BAREBONES_NOTEBOOK)
        self.assertEqual(jnr.name, name)

    # Test JupyterNotebookRecipe recipe setup correctly
    def testJupyterNotebookRecipeSetupRecipe(self)->None:
        jnr = JupyterNotebookRecipe("name", BAREBONES_NOTEBOOK)
        self.assertEqual(jnr.recipe, BAREBONES_NOTEBOOK)

    # Test JupyterNotebookRecipe parameters setup correctly
    def testJupyterNotebookRecipeSetupParameters(self)->None:
        parameters = {
            "a": 1,
            "b": True
        }
        jnr = JupyterNotebookRecipe(
            "name", BAREBONES_NOTEBOOK, parameters=parameters)
        self.assertEqual(jnr.parameters, parameters)

    # Test JupyterNotebookRecipe requirements setup correctly
    def testJupyterNotebookRecipeSetupRequirements(self)->None:
        requirements = {
            "a": 1,
            "b": True
        }
        jnr = JupyterNotebookRecipe(
            "name", BAREBONES_NOTEBOOK, requirements=requirements)
        self.assertEqual(jnr.requirements, requirements)

    # Test JupyterNotebookRecipe source setup correctly
    def testJupyterNotebookRecipeSetupSource(self)->None:
        source = "source.ipynb"
        jnr = JupyterNotebookRecipe(
            "name", BAREBONES_NOTEBOOK, source=source)
        self.assertEqual(jnr.source, source)

    # Test PapermillHandler can be created
    def testPapermillHanderMinimum(self)->None:
        PapermillHandler(
            TEST_HANDLER_BASE, 
            TEST_JOB_OUTPUT
        )

    # Test PapermillHandler will handle given events
    def testPapermillHandlerHandling(self)->None:
        from_handler_reader, from_handler_writer = Pipe()
        ph = PapermillHandler(
            TEST_HANDLER_BASE,
            TEST_JOB_OUTPUT
        )
        ph.to_runner = from_handler_writer
        
        with open(os.path.join(TEST_MONITOR_BASE, "A"), "w") as f:
            f.write("Data")

        pattern_one = FileEventPattern(
            "pattern_one", "A", "recipe_one", "file_one")
        recipe = JupyterNotebookRecipe(
            "recipe_one", COMPLETE_NOTEBOOK)

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe.name: recipe,
        }

        rules = create_rules(patterns, recipes)
        self.assertEqual(len(rules), 1)
        _, rule = rules.popitem()
        self.assertIsInstance(rule, FileEventJupyterNotebookRule)

        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 0)

        event = {
            EVENT_TYPE: WATCHDOG_TYPE,
            EVENT_PATH: os.path.join(TEST_MONITOR_BASE, "A"),
            WATCHDOG_BASE: TEST_MONITOR_BASE,
            WATCHDOG_RULE: rule,
            WATCHDOG_HASH: get_file_hash(
                os.path.join(TEST_MONITOR_BASE, "A"), SHA256
            )
        }

        ph.handle(event)

        if from_handler_reader.poll(3):
            job = from_handler_reader.recv()

        self.assertIsNotNone(job[JOB_ID])

        valid_job(job)

    # Test jobFunc performs as expected
    def testJobFunc(self)->None:
        file_path = os.path.join(TEST_MONITOR_BASE, "test")
        result_path = os.path.join(TEST_MONITOR_BASE, "output", "test")

        with open(file_path, "w") as f:
            f.write("Data")

        file_hash = get_file_hash(file_path, SHA256)

        pattern = FileEventPattern(
            "pattern", 
            file_path, 
            "recipe_one", 
            "infile", 
            parameters={
                "extra":"A line from a test Pattern",
                "outfile":result_path
            })
        recipe = JupyterNotebookRecipe(
            "recipe_one", APPENDING_NOTEBOOK)

        rule = create_rule(pattern, recipe)

        job_dict = create_job(
            PYTHON_TYPE,
            create_event(
                WATCHDOG_TYPE,
                file_path,
                {
                    WATCHDOG_BASE: TEST_MONITOR_BASE,
                    WATCHDOG_RULE: rule,
                    WATCHDOG_HASH: file_hash
                }
            ),
            {
                JOB_PARAMETERS:{
                    "extra":"extra",
                    "infile":file_path,
                    "outfile":result_path
                },
                JOB_HASH: file_hash,
                PYTHON_FUNC:job_func,
                PYTHON_OUTPUT_DIR:TEST_JOB_OUTPUT,
                PYTHON_EXECUTION_BASE:TEST_HANDLER_BASE
            }
        )

        job_func(job_dict)

        job_dir = os.path.join(TEST_HANDLER_BASE, job_dict[JOB_ID])
        self.assertFalse(os.path.exists(job_dir))
        
        output_dir = os.path.join(TEST_JOB_OUTPUT, job_dict[JOB_ID])
        self.assertTrue(os.path.exists(output_dir))
        self.assertTrue(os.path.exists(os.path.join(output_dir, META_FILE)))
        self.assertTrue(os.path.exists(os.path.join(output_dir, BASE_FILE)))
        self.assertTrue(os.path.exists(os.path.join(output_dir, PARAMS_FILE)))
        self.assertTrue(os.path.exists(os.path.join(output_dir, JOB_FILE)))
        self.assertTrue(os.path.exists(os.path.join(output_dir, RESULT_FILE)))

        self.assertTrue(os.path.exists(result_path))

    # Test jobFunc doesn't execute with no args
    def testJobFuncBadArgs(self)->None:
        try:
            job_func({})
        except Exception:
            pass

        self.assertEqual(len(os.listdir(TEST_HANDLER_BASE)), 0)
        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 0)
