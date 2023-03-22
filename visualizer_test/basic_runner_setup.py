import io
import os
# import unittest
import sys
import random as rand

sys.path.append("C:\\Users\\Johan\OneDrive\\Universitet\\Datalogi\\6. semester\\Bachelor\\meow")

# set PYTHONPATH=%PYTHONPATH%;C:\path\to\project\

from time import sleep
from meow_base.core.base_conductor import BaseConductor
from meow_base.core.base_handler import BaseHandler
from meow_base.core.base_monitor import BaseMonitor
from meow_base.conductors import LocalPythonConductor
from meow_base.core.correctness.vars import get_result_file, \
    JOB_TYPE_PAPERMILL, JOB_ERROR, META_FILE, JOB_TYPE_PYTHON, JOB_CREATE_TIME
from meow_base.core.runner import MeowRunner
from meow_base.functionality.file_io import make_dir, read_file, read_notebook, read_yaml
from meow_base.patterns.file_event_pattern import WatchdogMonitor, FileEventPattern
from meow_base.recipes.jupyter_notebook_recipe import PapermillHandler, \
    JupyterNotebookRecipe
from meow_base.recipes.python_recipe import PythonHandler, PythonRecipe
from meow_base.tests.shared import setup, teardown, \
    TEST_JOB_QUEUE, TEST_JOB_OUTPUT, TEST_MONITOR_BASE, \
    APPENDING_NOTEBOOK, COMPLETE_PYTHON_SCRIPT, TEST_DIR


def main():
    teardown()
    setup()
    testMeowRunnerPythonExecution()


def testMeowRunnerPythonExecution()->None:
    pattern_one = FileEventPattern(
        "pattern_one", os.path.join("start", "*.txt"), "recipe_one", "infile", 
        parameters={
            "num":10000,
            "outfile":os.path.join("{BASE}", "output", "{FILENAME}")
        })

    pattern_two = FileEventPattern(
        "pattern_two", os.path.join("start", "*.txt"), "recipe_two", "infile", 
        parameters={
            "num":10000,
            "outfile":os.path.join("{BASE}", "output", "{FILENAME}")
        })
        
    recipe = PythonRecipe(
        "recipe_one", COMPLETE_PYTHON_SCRIPT
    )
    recipe2 = PythonRecipe(
        "recipe_two", COMPLETE_PYTHON_SCRIPT
    )
    patterns = {
        pattern_one.name: pattern_one,
        pattern_two.name: pattern_two
    }
    recipes = {
        recipe.name: recipe,
        recipe2.name: recipe2
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

    # runner.monitors[0].add_recipe(recipe)
    # runner.monitors[0].add_recipe(recipe2)

    start_dir = os.path.join(TEST_MONITOR_BASE, "start")
    make_dir(start_dir)
    # with open(os.path.join(start_dir, "A.txt"), "w") as f:
    #     f.write("25000")
    # with open(os.path.join(start_dir, "B.txt"), "w") as f:
    #     f.write("25000") 

    # job_id = None
    

    
    for i in range(0, 15):
        for j in range (1,rand.randint(30,30)) :
            with open(os.path.join(start_dir, f"{i}_{j}.txt"), "w") as f: 
                f.write(str(25000/(i*j + 1) + 25000 / (j+1))) 
        loops = 0
        
        while loops < 15:
            sleep(3)
            runner_debug_stream.seek(0)
            messages = runner_debug_stream.readlines()
            
            for msg in messages:
                # self.assertNotIn("ERROR", msg)
                with open(os.path.join("visualizer_print", "messages.txt"), "a") as f:
                    f.write(msg)  
                    
                if "INFO: Completed execution for job: '" in msg:
                    # From Davids test
                    # job_id = msg.replace(
                    #     "INFO: Completed execution for job: '", "")
                    # job_id = job_id[:-2]
                    # # loops = 30
                    loops = 15
            loops += 1

            runner.visualizer.update()
            

    runner.stop()


if __name__ == "__main__":
    main()