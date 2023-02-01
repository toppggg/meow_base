
import jsonschema
import os
import unittest

from multiprocessing import Pipe

from core.correctness.vars import EVENT_TYPE, WATCHDOG_BASE, WATCHDOG_RULE, \
    WATCHDOG_TYPE, EVENT_PATH, SHA256, WATCHDOG_HASH, JOB_ID, PYTHON_TYPE, \
    JOB_PARAMETERS, JOB_HASH, PYTHON_FUNC, PYTHON_OUTPUT_DIR, \
    PYTHON_EXECUTION_BASE, META_FILE, BASE_FILE, PARAMS_FILE, JOB_FILE, \
    RESULT_FILE
from core.correctness.validation import valid_job
from core.functionality import get_file_hash, create_job, create_event, \
    make_dir, write_yaml, write_notebook, read_yaml
from core.meow import create_rules, create_rule
from patterns.file_event_pattern import FileEventPattern, SWEEP_START, \
    SWEEP_STOP, SWEEP_JUMP
from recipes.jupyter_notebook_recipe import JupyterNotebookRecipe, \
    PapermillHandler, job_func
from rules.file_event_jupyter_notebook_rule import FileEventJupyterNotebookRule
from shared import setup, teardown, TEST_HANDLER_BASE, TEST_MONITOR_BASE, \
    TEST_JOB_OUTPUT, BAREBONES_NOTEBOOK, APPENDING_NOTEBOOK, COMPLETE_NOTEBOOK

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
            job_dir = from_handler_reader.recv()

        self.assertIsInstance(job_dir, str)
        self.assertTrue(os.path.exists(job_dir))

        job = read_yaml(os.path.join(job_dir, META_FILE))
        valid_job(job)

    # Test PapermillHandler will create enough jobs from single sweep
    def testPapermillHandlerHandlingSingleSweep(self)->None:
        from_handler_reader, from_handler_writer = Pipe()
        ph = PapermillHandler(
            TEST_HANDLER_BASE,
            TEST_JOB_OUTPUT
        )
        ph.to_runner = from_handler_writer
        
        with open(os.path.join(TEST_MONITOR_BASE, "A"), "w") as f:
            f.write("Data")

        pattern_one = FileEventPattern(
            "pattern_one", "A", "recipe_one", "file_one", sweep={"s":{
                SWEEP_START: 0, SWEEP_STOP: 2, SWEEP_JUMP:1
            }})
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

        jobs = []
        recieving = True
        while recieving:
            if from_handler_reader.poll(3):
                jobs.append(from_handler_reader.recv())
            else:
                recieving = False

        values = [0, 1, 2]
        self.assertEqual(len(jobs), 3)
        for job_dir in jobs:
            self.assertIsInstance(job_dir, str)
            self.assertTrue(os.path.exists(job_dir))

            job = read_yaml(os.path.join(job_dir, META_FILE))
            valid_job(job)

            self.assertIn(JOB_PARAMETERS, job)
            self.assertIn("s", job[JOB_PARAMETERS])
            if job[JOB_PARAMETERS]["s"] in values:
                values.remove(job[JOB_PARAMETERS]["s"])
        self.assertEqual(len(values), 0)

    # Test PapermillHandler will create enough jobs from multiple sweeps
    def testPapermillHandlerHandlingMultipleSweep(self)->None:
        from_handler_reader, from_handler_writer = Pipe()
        ph = PapermillHandler(
            TEST_HANDLER_BASE,
            TEST_JOB_OUTPUT
        )
        ph.to_runner = from_handler_writer
        
        with open(os.path.join(TEST_MONITOR_BASE, "A"), "w") as f:
            f.write("Data")

        pattern_one = FileEventPattern(
            "pattern_one", "A", "recipe_one", "file_one", sweep={
                "s1":{
                    SWEEP_START: 0, SWEEP_STOP: 2, SWEEP_JUMP:1
                },
                "s2":{
                    SWEEP_START: 20, SWEEP_STOP: 80, SWEEP_JUMP:15
                }
            })
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

        jobs = []
        recieving = True
        while recieving:
            if from_handler_reader.poll(3):
                jobs.append(from_handler_reader.recv())
            else:
                recieving = False

        values = [
            "s1-0/s2-20", "s1-1/s2-20", "s1-2/s2-20", 
            "s1-0/s2-35", "s1-1/s2-35", "s1-2/s2-35", 
            "s1-0/s2-50", "s1-1/s2-50", "s1-2/s2-50", 
            "s1-0/s2-65", "s1-1/s2-65", "s1-2/s2-65", 
            "s1-0/s2-80", "s1-1/s2-80", "s1-2/s2-80", 
        ]
        self.assertEqual(len(jobs), 15)
        for job_dir in jobs:
            self.assertIsInstance(job_dir, str)
            self.assertTrue(os.path.exists(job_dir))

            job = read_yaml(os.path.join(job_dir, META_FILE))
            valid_job(job)

            self.assertIn(JOB_PARAMETERS, job)
            val1 = None
            val2 = None
            if "s1" in job[JOB_PARAMETERS]:
                val1 = f"s1-{job[JOB_PARAMETERS]['s1']}"
            if "s2" in job[JOB_PARAMETERS]:
                val2 = f"s2-{job[JOB_PARAMETERS]['s2']}"
            val = None
            if val1 and val2:
                val = f"{val1}/{val2}"
            if val and val in values:
                values.remove(val)
        self.assertEqual(len(values), 0)

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

        params_dict = {
            "extra":"extra",
            "infile":file_path,
            "outfile":result_path
        }

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
                JOB_PARAMETERS:params_dict,
                JOB_HASH: file_hash,
                PYTHON_FUNC:job_func,
                PYTHON_OUTPUT_DIR:TEST_JOB_OUTPUT,
                PYTHON_EXECUTION_BASE:TEST_HANDLER_BASE
            }
        )

        job_dir = os.path.join(
            job_dict[PYTHON_EXECUTION_BASE], job_dict[JOB_ID])
        make_dir(job_dir)

        meta_file = os.path.join(job_dir, META_FILE)
        write_yaml(job_dict, meta_file)

        param_file = os.path.join(job_dir, PARAMS_FILE)
        write_yaml(params_dict, param_file)

        base_file = os.path.join(job_dir, BASE_FILE)
        write_notebook(APPENDING_NOTEBOOK, base_file)

        job_func(job_dict)

        job_dir = os.path.join(TEST_HANDLER_BASE, job_dict[JOB_ID])
        self.assertTrue(os.path.exists(job_dir))
        self.assertTrue(os.path.exists(os.path.join(job_dir, META_FILE)))
        self.assertTrue(os.path.exists(os.path.join(job_dir, BASE_FILE)))
        self.assertTrue(os.path.exists(os.path.join(job_dir, PARAMS_FILE)))
        self.assertTrue(os.path.exists(os.path.join(job_dir, JOB_FILE)))
        self.assertTrue(os.path.exists(os.path.join(job_dir, RESULT_FILE)))

        self.assertTrue(os.path.exists(result_path))

    # Test jobFunc doesn't execute with no args
    def testJobFuncBadArgs(self)->None:
        try:
            job_func({})
        except Exception:
            pass

        self.assertEqual(len(os.listdir(TEST_HANDLER_BASE)), 0)
        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 0)
