
import io
import os
import unittest
 
from time import sleep

from core.correctness.vars import TEST_HANDLER_BASE, TEST_JOB_OUTPUT, \
    TEST_MONITOR_BASE, APPENDING_NOTEBOOK
from core.functionality import make_dir, rmtree, read_notebook
from core.meow import create_rules
from core.runner import MeowRunner
from patterns import WatchdogMonitor, FileEventPattern
from recipes.jupyter_notebook_recipe import PapermillHandler, \
    JupyterNotebookRecipe, RESULT_FILE


class MeowTests(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        make_dir(TEST_MONITOR_BASE)
        make_dir(TEST_HANDLER_BASE)
        make_dir(TEST_JOB_OUTPUT)

    def tearDown(self) -> None:
        super().tearDown()
        rmtree(TEST_MONITOR_BASE)
        rmtree(TEST_HANDLER_BASE)
        rmtree(TEST_JOB_OUTPUT)

    def testMeowRunner(self)->None:
        pattern_one = FileEventPattern(
            "pattern_one", "start/A.txt", "recipe_one", "infile", 
            parameters={
                "extra":"A line from a test Pattern",
                "outfile":"{VGRID}/output/{FILENAME}"
            })
        recipe = JupyterNotebookRecipe(
            "recipe_one", APPENDING_NOTEBOOK)

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe.name: recipe,
        }
        rules = create_rules(patterns, recipes)

        monitor_debug_stream = io.StringIO("")
        handler_debug_stream = io.StringIO("")

        runner = MeowRunner(
            WatchdogMonitor(
                TEST_MONITOR_BASE,
                rules,
                print=monitor_debug_stream,
                logging=3, 
                settletime=1
            ), 
            PapermillHandler(
                TEST_HANDLER_BASE,
                TEST_JOB_OUTPUT,
                print=handler_debug_stream,
                logging=3                
            )
        )        
   
        runner.start()

        start_dir = os.path.join(TEST_MONITOR_BASE, "start")
        make_dir(start_dir)
        self.assertTrue(start_dir)
        with open(os.path.join(start_dir, "A.txt"), "w") as f:
            f.write("Initial Data")

        self.assertTrue(os.path.exists(os.path.join(start_dir, "A.txt")))

        loops = 0
        job_id = None
        while loops < 15:
            sleep(1)
            handler_debug_stream.seek(0)
            messages = handler_debug_stream.readlines()

            for msg in messages:
                self.assertNotIn("ERROR", msg)
            
                if "INFO: Completed job " in msg:
                    job_id = msg.replace("INFO: Completed job ", "")
                    job_id = job_id[:job_id.index(" with output")]
                    loops = 15
            loops += 1

        self.assertIsNotNone(job_id)
        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 1)
        self.assertIn(job_id, os.listdir(TEST_JOB_OUTPUT))

        runner.stop()

        job_dir = os.path.join(TEST_JOB_OUTPUT, job_id)
        self.assertEqual(len(os.listdir(job_dir)), 5)

        result = read_notebook(os.path.join(job_dir, RESULT_FILE))
        self.assertIsNotNone(result)

        output_path = os.path.join(TEST_MONITOR_BASE, "output", "A.txt")
        self.assertTrue(os.path.exists(output_path))
        
        with open(output_path, "r") as f:
            data = f.read()
        
        self.assertEqual(data, "Initial Data\nA line from a test Pattern")

    def testMeowRunnerLinkeExecution(self)->None:
        pattern_one = FileEventPattern(
            "pattern_one", "start/A.txt", "recipe_one", "infile", 
            parameters={
                "extra":"A line from Pattern 1",
                "outfile":"{VGRID}/middle/{FILENAME}"
            })
        pattern_two = FileEventPattern(
            "pattern_two", "middle/A.txt", "recipe_one", "infile", 
            parameters={
                "extra":"A line from Pattern 2",
                "outfile":"{VGRID}/output/{FILENAME}"
            })
        recipe = JupyterNotebookRecipe(
            "recipe_one", APPENDING_NOTEBOOK)

        patterns = {
            pattern_one.name: pattern_one,
            pattern_two.name: pattern_two,
        }
        recipes = {
            recipe.name: recipe,
        }
        rules = create_rules(patterns, recipes)

        monitor_debug_stream = io.StringIO("")
        handler_debug_stream = io.StringIO("")

        runner = MeowRunner(
            WatchdogMonitor(
                TEST_MONITOR_BASE,
                rules,
                print=monitor_debug_stream,
                logging=3, 
                settletime=1
            ), 
            PapermillHandler(
                TEST_HANDLER_BASE,
                TEST_JOB_OUTPUT,
                print=handler_debug_stream,
                logging=3                
            )
        )        
   
        runner.start()

        start_dir = os.path.join(TEST_MONITOR_BASE, "start")
        make_dir(start_dir)
        self.assertTrue(start_dir)
        with open(os.path.join(start_dir, "A.txt"), "w") as f:
            f.write("Initial Data")

        self.assertTrue(os.path.exists(os.path.join(start_dir, "A.txt")))

        loops = 0
        job_ids = []
        while len(job_ids) < 2 and loops < 15:
            sleep(1)
            handler_debug_stream.seek(0)
            messages = handler_debug_stream.readlines()

            for msg in messages:
                self.assertNotIn("ERROR", msg)
            
                if "INFO: Completed job " in msg:
                    job_id = msg.replace("INFO: Completed job ", "")
                    job_id = job_id[:job_id.index(" with output")]
                    if job_id not in job_ids:
                        job_ids.append(job_id)
            loops += 1
        
        print(job_ids)

        self.assertEqual(len(job_ids), 2)
        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 2)
        self.assertIn(job_ids[0], os.listdir(TEST_JOB_OUTPUT))
        self.assertIn(job_ids[1], os.listdir(TEST_JOB_OUTPUT))

        runner.stop()

        mid_job_dir = os.path.join(TEST_JOB_OUTPUT, job_id)
        self.assertEqual(len(os.listdir(mid_job_dir)), 5)

        result = read_notebook(os.path.join(mid_job_dir, RESULT_FILE))
        self.assertIsNotNone(result)

        mid_output_path = os.path.join(TEST_MONITOR_BASE, "middle", "A.txt")
        self.assertTrue(os.path.exists(mid_output_path))
        
        with open(mid_output_path, "r") as f:
            data = f.read()
        
        self.assertEqual(data, "Initial Data\nA line from Pattern 1")

        final_job_dir = os.path.join(TEST_JOB_OUTPUT, job_id)
        self.assertEqual(len(os.listdir(final_job_dir)), 5)

        result = read_notebook(os.path.join(final_job_dir, RESULT_FILE))
        self.assertIsNotNone(result)

        final_output_path = os.path.join(TEST_MONITOR_BASE, "output", "A.txt")
        self.assertTrue(os.path.exists(final_output_path))
        
        with open(final_output_path, "r") as f:
            data = f.read()
        
        self.assertEqual(data, 
            "Initial Data\nA line from Pattern 1\nA line from Pattern 2")
