
import io
import os
import unittest
 
from multiprocessing import Pipe
from typing import Any, Union

from core.correctness.vars import TEST_HANDLER_BASE, TEST_JOB_OUTPUT, \
    TEST_MONITOR_BASE, BAREBONES_NOTEBOOK, WATCHDOG_BASE, WATCHDOG_RULE, \
    EVENT_PATH, WATCHDOG_TYPE, EVENT_TYPE
from core.functionality import make_dir, rmtree
from core.meow import BasePattern, BaseRecipe, BaseRule, BaseMonitor, \
    BaseHandler, create_rules
from patterns import FileEventPattern, WatchdogMonitor
from recipes.jupyter_notebook_recipe import JupyterNotebookRecipe

valid_pattern_one = FileEventPattern(
    "pattern_one", "path_one", "recipe_one", "file_one")
valid_pattern_two = FileEventPattern(
    "pattern_two", "path_two", "recipe_two", "file_two")

valid_recipe_one = JupyterNotebookRecipe(
    "recipe_one", BAREBONES_NOTEBOOK)
valid_recipe_two = JupyterNotebookRecipe(
    "recipe_two", BAREBONES_NOTEBOOK)


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

    def testBaseRecipe(self)->None:
        with self.assertRaises(TypeError):
            BaseRecipe("name", "")
        
        class NewRecipe(BaseRecipe):
            pass
        with self.assertRaises(NotImplementedError):
            NewRecipe("name", "")

        class FullRecipe(BaseRecipe):
            def _is_valid_recipe(self, recipe:Any)->None:
                pass
            def _is_valid_parameters(self, parameters:Any)->None:
                pass
            def _is_valid_requirements(self, requirements:Any)->None:
                pass
        FullRecipe("name", "")

    def testBasePattern(self)->None:
        with self.assertRaises(TypeError):
            BasePattern("name", "", "", "")

        class NewPattern(BasePattern):
            pass
        with self.assertRaises(NotImplementedError):
            NewPattern("name", "", "", "")

        class FullPattern(BasePattern):
            def _is_valid_recipe(self, recipe:Any)->None:
                pass
            def _is_valid_parameters(self, parameters:Any)->None:
                pass
            def _is_valid_output(self, outputs:Any)->None:
                pass
        FullPattern("name", "", "", "")

    def testBaseRule(self)->None:
        with self.assertRaises(TypeError):
            BaseRule("name", "", "")

        class NewRule(BaseRule):
            pass
        with self.assertRaises(NotImplementedError):
            NewRule("name", "", "")

        class FullRule(BaseRule):
            pattern_type = "pattern"
            recipe_type = "recipe"
            def _is_valid_recipe(self, recipe:Any)->None:
                pass
            def _is_valid_pattern(self, pattern:Any)->None:
                pass
        FullRule("name", "", "")

    def testCreateRulesMinimum(self)->None:
        create_rules({}, {})

    def testCreateRulesPatternsAndRecipesDicts(self)->None:
        patterns = {
            valid_pattern_one.name: valid_pattern_one,
            valid_pattern_two.name: valid_pattern_two
        }
        recipes = {
            valid_recipe_one.name: valid_recipe_one,
            valid_recipe_two.name: valid_recipe_two
        }
        rules = create_rules(patterns, recipes)
        self.assertIsInstance(rules, dict)
        self.assertEqual(len(rules), 2)
        for k, rule in rules.items():
            self.assertIsInstance(k, str)
            self.assertIsInstance(rule, BaseRule)
            self.assertEqual(k, rule.name)

    def testCreateRulesMisindexedPatterns(self)->None:
        patterns = {
            valid_pattern_two.name: valid_pattern_one,
            valid_pattern_one.name: valid_pattern_two
        }
        with self.assertRaises(KeyError):
            create_rules(patterns, {})

    def testCreateRulesMisindexedRecipes(self)->None:
        recipes = {
            valid_recipe_two.name: valid_recipe_one,
            valid_recipe_one.name: valid_recipe_two
        }
        with self.assertRaises(KeyError):
            create_rules({}, recipes)

    def testBaseMonitor(self)->None:
        with self.assertRaises(TypeError):
            BaseMonitor({}, {})

        class TestMonitor(BaseMonitor):
            pass

        with self.assertRaises(NotImplementedError):
            TestMonitor({}, {})

        class FullTestMonitor(BaseMonitor):
            def start(self):
                pass
            def stop(self):
                pass
            def _is_valid_patterns(self, patterns:dict[str,BasePattern])->None:
                pass
            def _is_valid_recipes(self, recipes:dict[str,BaseRecipe])->None:
                pass
            def add_pattern(self, pattern:BasePattern)->None:
                pass
            def update_pattern(self, pattern:BasePattern)->None:
                pass
            def remove_pattern(self, pattern:Union[str,BasePattern])->None:
                pass
            def get_patterns(self)->None:
                pass
            def add_recipe(self, recipe:BaseRecipe)->None:
                pass
            def update_recipe(self, recipe:BaseRecipe)->None:
                pass
            def remove_recipe(self, recipe:Union[str,BaseRecipe])->None:
                pass
            def get_recipes(self)->None:
                pass
            def get_rules(self)->None:
                pass
        FullTestMonitor({}, {})

    def testMonitoring(self)->None:
        pattern_one = FileEventPattern(
            "pattern_one", "start/A.txt", "recipe_one", "infile", 
            parameters={})
        recipe = JupyterNotebookRecipe(
            "recipe_one", BAREBONES_NOTEBOOK)

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe.name: recipe,
        }

        monitor_debug_stream = io.StringIO("")

        wm = WatchdogMonitor(
            TEST_MONITOR_BASE,
            patterns,
            recipes,
            print=monitor_debug_stream,
            logging=3, 
            settletime=1
        )

        rules = wm.get_rules()
        rule = rules[list(rules.keys())[0]]

        from_monitor_reader, from_monitor_writer = Pipe()
        wm.to_runner = from_monitor_writer
   
        wm.start()

        start_dir = os.path.join(TEST_MONITOR_BASE, "start")
        make_dir(start_dir)
        self.assertTrue(start_dir)
        with open(os.path.join(start_dir, "A.txt"), "w") as f:
            f.write("Initial Data")

        self.assertTrue(os.path.exists(os.path.join(start_dir, "A.txt")))

        messages = []
        while True:
            if from_monitor_reader.poll(3):
                messages.append(from_monitor_reader.recv())
            else:
                break
        self.assertTrue(len(messages), 1)
        message = messages[0]

        self.assertEqual(type(message), dict)
        self.assertIn(EVENT_TYPE, message)
        self.assertEqual(message[EVENT_TYPE], WATCHDOG_TYPE)
        self.assertIn(WATCHDOG_BASE, message)
        self.assertEqual(message[WATCHDOG_BASE], TEST_MONITOR_BASE)
        self.assertIn(EVENT_PATH, message)
        self.assertEqual(message[EVENT_PATH], 
            os.path.join(start_dir, "A.txt"))
        self.assertIn(WATCHDOG_RULE, message)
        self.assertEqual(message[WATCHDOG_RULE].name, rule.name)

        wm.stop()

    def testMonitoringRetroActive(self)->None:
        pattern_one = FileEventPattern(
            "pattern_one", "start/A.txt", "recipe_one", "infile", 
            parameters={})
        recipe = JupyterNotebookRecipe(
            "recipe_one", BAREBONES_NOTEBOOK)

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe.name: recipe,
        }

        start_dir = os.path.join(TEST_MONITOR_BASE, "start")
        make_dir(start_dir)
        self.assertTrue(start_dir)
        with open(os.path.join(start_dir, "A.txt"), "w") as f:
            f.write("Initial Data")

        self.assertTrue(os.path.exists(os.path.join(start_dir, "A.txt")))

        monitor_debug_stream = io.StringIO("")

        wm = WatchdogMonitor(
            TEST_MONITOR_BASE,
            patterns,
            recipes,
            print=monitor_debug_stream,
            logging=3, 
            settletime=1
        )

        rules = wm.get_rules()
        rule = rules[list(rules.keys())[0]]

        from_monitor_reader, from_monitor_writer = Pipe()
        wm.to_runner = from_monitor_writer
   
        wm.start()

        messages = []
        while True:
            if from_monitor_reader.poll(3):
                messages.append(from_monitor_reader.recv())
            else:
                break
        self.assertTrue(len(messages), 1)
        message = messages[0]

        self.assertEqual(type(message), dict)
        self.assertIn(EVENT_TYPE, message)
        self.assertEqual(message[EVENT_TYPE], WATCHDOG_TYPE)
        self.assertIn(WATCHDOG_BASE, message)
        self.assertEqual(message[WATCHDOG_BASE], TEST_MONITOR_BASE)
        self.assertIn(EVENT_PATH, message)
        self.assertEqual(message[EVENT_PATH], 
            os.path.join(start_dir, "A.txt"))
        self.assertIn(WATCHDOG_RULE, message)
        self.assertEqual(message[WATCHDOG_RULE].name, rule.name)

        wm.stop()


    def testBaseHandler(self)->None:
        with self.assertRaises(TypeError):
            BaseHandler()

        class TestHandler(BaseHandler):
            pass

        with self.assertRaises(NotImplementedError):
            TestHandler()

        class FullTestHandler(BaseHandler):
            def handle(self, event):
                pass
            def start(self):
                pass
            def stop(self):
                pass
            def _is_valid_inputs(self, inputs:Any)->None:
                pass
            def valid_event_types(self)->list[str]:
                pass
        FullTestHandler()

