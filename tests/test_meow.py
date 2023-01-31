
import unittest
 
from typing import Any, Union

from core.correctness.vars import BAREBONES_NOTEBOOK
from core.meow import BasePattern, BaseRecipe, BaseRule, BaseMonitor, \
    BaseHandler, BaseConductor, create_rules, create_rule
from patterns import FileEventPattern
from recipes.jupyter_notebook_recipe import JupyterNotebookRecipe
from shared import setup, teardown

valid_pattern_one = FileEventPattern(
    "pattern_one", "path_one", "recipe_one", "file_one")
valid_pattern_two = FileEventPattern(
    "pattern_two", "path_two", "recipe_two", "file_two")

valid_recipe_one = JupyterNotebookRecipe(
    "recipe_one", BAREBONES_NOTEBOOK)
valid_recipe_two = JupyterNotebookRecipe(
    "recipe_two", BAREBONES_NOTEBOOK)


class MeowTests(unittest.TestCase):
    def setUp(self)->None:
        super().setUp()
        setup()

    def tearDown(self)->None:
        super().tearDown()
        teardown()

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

    def testCreateRule(self)->None:
        rule = create_rule(valid_pattern_one, valid_recipe_one)

        self.assertIsInstance(rule, BaseRule)

        with self.assertRaises(ValueError):
            rule = create_rule(valid_pattern_one, valid_recipe_two)
    
    def testCreateRulesMinimum(self)->None:
        rules = create_rules({}, {})

        self.assertEqual(len(rules), 0)

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

    def testBaseConductor(self)->None:
        with self.assertRaises(TypeError):
            BaseConductor()

        class TestConductor(BaseConductor):
            pass

        with self.assertRaises(NotImplementedError):
            TestConductor()

        class FullTestConductor(BaseConductor):
            def execute(self, job:dict[str,Any])->None:
                pass

            def valid_job_types(self)->list[str]:
                pass

        FullTestConductor()
