import io
import os
# import unittest
import sys

sys.path.append("C:\\Users\\Johan\OneDrive\\Universitet\\Datalogi\\6. semester\\Bachelor\\meow_base")

# set PYTHONPATH=%PYTHONPATH%;C:\path\to\project\

from time import sleep
from core.base_conductor import BaseConductor
from core.base_handler import BaseHandler
from core.base_monitor import BaseMonitor
from conductors import LocalPythonConductor
from core.correctness.vars import get_result_file, \
    JOB_TYPE_PAPERMILL, JOB_ERROR, META_FILE, JOB_TYPE_PYTHON, JOB_CREATE_TIME
from core.runner import MeowRunner
from functionality.file_io import make_dir, read_file, read_notebook, read_yaml
from patterns.file_event_pattern import WatchdogMonitor, FileEventPattern
from recipes.jupyter_notebook_recipe import PapermillHandler, \
    JupyterNotebookRecipe
from recipes.python_recipe import PythonHandler, PythonRecipe
from tests.shared import setup, teardown, \
    TEST_JOB_QUEUE, TEST_JOB_OUTPUT, TEST_MONITOR_BASE, \
    APPENDING_NOTEBOOK, COMPLETE_PYTHON_SCRIPT, TEST_DIR


def main():
    # runner = meowRunnerSetup()
    teardown()
    setup()
    testMeowRunnerPythonExecution()

    # createJobs()



# def meowRunnerSetup()->MeowRunner:
#     #Create a simple monitor
#     monitor_one = WatchdogMonitor(TEST_MONITOR_BASE, {}, {})

#     handler_one = PapermillHandler()

#     conductor_one = LocalPythonConductor()

#     runner = MeowRunner(monitor_one, handler_one, conductor_one)
#     return runner

# def createJobs():
#     pass

def testMeowRunnerPythonExecution()->None:
    pattern_one = FileEventPattern(
        "pattern_one", os.path.join("start", "A.txt"), "recipe_two", "infile", 
        parameters={
            "num":10000,
            "outfile":os.path.join("{VGRID}", "output", "{FILENAME}")
        })
        
    recipe = PythonRecipe(
        "recipe_one", COMPLETE_PYTHON_SCRIPT
    )
    recipe2 = PythonRecipe(
        "recipe_two", COMPLETE_PYTHON_SCRIPT
    )
    patterns = {
        pattern_one.name: pattern_one,
    }
    recipes = {
        recipe.name: recipe,
    }

    runner_debug_stream = io.StringIO("")

    runner = MeowRunner(
        WatchdogMonitor(
            TEST_MONITOR_BASE,
            patterns,
            recipes,
            settletime=1
        ), 
        PythonHandler(
            job_queue_dir=TEST_JOB_QUEUE
        ),
        LocalPythonConductor(),
        job_queue_dir=TEST_JOB_QUEUE,
        job_output_dir=TEST_JOB_OUTPUT,
        print=runner_debug_stream,
        logging=3 ,
        visualizer_active=True              
    )        

    runner.start()

    runner.monitors[0].add_recipe(recipe2)

    start_dir = os.path.join(TEST_MONITOR_BASE, "start")
    make_dir(start_dir)
    # self.assertTrue(start_dir)
    with open(os.path.join(start_dir, "A.txt"), "w") as f:
        f.write("25000")


    # self.assertTrue(os.path.exists(os.path.join(start_dir, "A.txt")))

    loops = 0
    job_id = None

    
    while loops < 15:
        sleep(1)
        runner_debug_stream.seek(0)
        messages = runner_debug_stream.readlines()

        for msg in messages:
            # self.assertNotIn("ERROR", msg)
        
            if "INFO: Completed execution for job: '" in msg:
                job_id = msg.replace(
                    "INFO: Completed execution for job: '", "")
                job_id = job_id[:-2]
                loops = 15
        loops += 1

    # self.assertIsNotNone(job_id)
    # self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 1)
    # self.assertIn(job_id, os.listdir(TEST_JOB_OUTPUT))
    

    runner.stop()
    # job_dir = os.path.join(TEST_JOB_OUTPUT, job_id)

    # metafile = os.path.join(job_dir, META_FILE)
    # status = read_yaml(metafile)

    # self.assertNotIn(JOB_ERROR, status)

    # result_path = os.path.join(job_dir, get_result_file(JOB_TYPE_PYTHON))
    # self.assertTrue(os.path.exists(result_path))
    # result = read_file(os.path.join(result_path))
    # self.assertEqual(
        # result, "--STDOUT--\n12505000.0\ndone\n\n\n--STDERR--\n\n")

    # output_path = os.path.join(TEST_MONITOR_BASE, "output", "A.txt")
    # self.assertTrue(os.path.exists(output_path))
    # output = read_file(os.path.join(output_path))
    # self.assertEqual(output, "12505000.0")


if __name__ == "__main__":
    main()