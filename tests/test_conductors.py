
import os
import unittest

from core.correctness.vars import JOB_TYPE_PYTHON, SHA256, EVENT_TYPE_WATCHDOG, \
    WATCHDOG_BASE, EVENT_RULE, WATCHDOG_HASH, JOB_PARAMETERS, JOB_HASH, \
    PYTHON_FUNC, PYTHON_OUTPUT_DIR, PYTHON_EXECUTION_BASE, JOB_ID, META_FILE, \
    BASE_FILE, PARAMS_FILE, JOB_FILE, RESULT_FILE
from core.functionality import get_file_hash, create_event, create_job, \
    make_dir, write_yaml, write_notebook
from core.meow import create_rule
from conductors import LocalPythonConductor
from patterns import FileEventPattern
from recipes.jupyter_notebook_recipe import JupyterNotebookRecipe, job_func
from shared import setup, teardown, TEST_MONITOR_BASE, APPENDING_NOTEBOOK, \
    TEST_JOB_OUTPUT, TEST_HANDLER_BASE


def failing_func():
    raise Exception("bad function")
 

class MeowTests(unittest.TestCase):
    def setUp(self)->None:
        super().setUp()
        setup()

    def tearDown(self)->None:
        super().tearDown()
        teardown()
        
    # Test LocalPythonConductor creation and job types
    def testLocalPythonConductorCreation(self)->None:
        LocalPythonConductor()

    #TODO Test LocalPythonConductor execution criteria

    # Test LocalPythonConductor executes valid jobs
    def testLocalPythonConductorValidJob(self)->None:
        lpc = LocalPythonConductor()

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
            JOB_TYPE_PYTHON,
            create_event(
                EVENT_TYPE_WATCHDOG,
                file_path,
                rule,
                {
                    WATCHDOG_BASE: TEST_MONITOR_BASE,
                    EVENT_RULE: rule,
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

        job_dir = os.path.join(TEST_HANDLER_BASE, job_dict[JOB_ID])
        make_dir(job_dir)

        param_file = os.path.join(job_dir, PARAMS_FILE)
        write_yaml(params_dict, param_file)

        base_file = os.path.join(job_dir, BASE_FILE)
        write_notebook(APPENDING_NOTEBOOK, base_file)

        lpc.execute(job_dict)

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

    # Test LocalPythonConductor does not execute jobs with bad arguments
    def testLocalPythonConductorBadArgs(self)->None:
        lpc = LocalPythonConductor()

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

        bad_job_dict = create_job(
            JOB_TYPE_PYTHON,
            create_event(
                EVENT_TYPE_WATCHDOG,
                file_path,
                rule,
                {
                    WATCHDOG_BASE: TEST_MONITOR_BASE,
                    EVENT_RULE: rule,
                    WATCHDOG_HASH: file_hash
                }
            ),
            {
                JOB_PARAMETERS:params_dict,
                JOB_HASH: file_hash,
                PYTHON_FUNC:job_func,
            }
        )

        job_dir = os.path.join(TEST_HANDLER_BASE, bad_job_dict[JOB_ID])
        make_dir(job_dir)

        param_file = os.path.join(job_dir, PARAMS_FILE)
        write_yaml(params_dict, param_file)

        base_file = os.path.join(job_dir, BASE_FILE)
        write_notebook(APPENDING_NOTEBOOK, base_file)

        with self.assertRaises(KeyError):
            lpc.execute(bad_job_dict)

        # Ensure execution can continue after one failed job
        good_job_dict = create_job(
            JOB_TYPE_PYTHON,
            create_event(
                EVENT_TYPE_WATCHDOG,
                file_path,
                rule,
                {
                    WATCHDOG_BASE: TEST_MONITOR_BASE,
                    EVENT_RULE: rule,
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

        job_dir = os.path.join(TEST_HANDLER_BASE, good_job_dict[JOB_ID])
        make_dir(job_dir)

        param_file = os.path.join(job_dir, PARAMS_FILE)
        write_yaml(params_dict, param_file)

        base_file = os.path.join(job_dir, BASE_FILE)
        write_notebook(APPENDING_NOTEBOOK, base_file)

        lpc.execute(good_job_dict)

        job_dir = os.path.join(TEST_HANDLER_BASE, good_job_dict[JOB_ID])
        self.assertFalse(os.path.exists(job_dir))
        
        output_dir = os.path.join(TEST_JOB_OUTPUT, good_job_dict[JOB_ID])
        self.assertTrue(os.path.exists(output_dir))
        self.assertTrue(os.path.exists(os.path.join(output_dir, META_FILE)))
        self.assertTrue(os.path.exists(os.path.join(output_dir, BASE_FILE)))
        self.assertTrue(os.path.exists(os.path.join(output_dir, PARAMS_FILE)))
        self.assertTrue(os.path.exists(os.path.join(output_dir, JOB_FILE)))
        self.assertTrue(os.path.exists(os.path.join(output_dir, RESULT_FILE)))

        self.assertTrue(os.path.exists(result_path))

    # Test LocalPythonConductor does not execute jobs with bad functions
    def testLocalPythonConductorBadFunc(self)->None:
        lpc = LocalPythonConductor()

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
            JOB_TYPE_PYTHON,
            create_event(
                EVENT_TYPE_WATCHDOG,
                file_path,
                rule,
                {
                    WATCHDOG_BASE: TEST_MONITOR_BASE,
                    EVENT_RULE: rule,
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
                PYTHON_FUNC:failing_func,
            }
        )

        with self.assertRaises(Exception):
            lpc.execute(job_dict)

    # TODO test job status funcs
    # TODO test mangled status file reads
    # TODO test missing input files
