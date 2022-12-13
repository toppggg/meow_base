
import unittest

from typing import Any

from core.correctness.vars import BAREBONES_NOTEBOOK
from core.meow import BasePattern, BaseRecipe, BaseRule, BaseMonitor, \
    BaseHandler


class MeowTests(unittest.TestCase):
    def setUp(self)->None:
        return super().setUp()
    
    def tearDown(self)->None:
        return super().tearDown()

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

    def testBaseMonitor(self)->None:
        with self.assertRaises(TypeError):
            BaseMonitor("", "", "")

        class TestMonitor(BaseMonitor):
            pass

        with self.assertRaises(NotImplementedError):
            TestMonitor("", "", "")

        class FullTestMonitor(BaseMonitor):
            def start(self):
                pass
            def stop(self):
                pass
            def _is_valid_report(self, report:Any)->None:
                pass
            def _is_valid_listen(self, listen:Any)->None:
                pass
            def _is_valid_rules(self, rules:Any)->None:
                pass
        FullTestMonitor("", "", "")

    def testBaseHandler(self)->None:
        with self.assertRaises(TypeError):
            BaseHandler("")

        class TestHandler(BaseHandler):
            pass

        with self.assertRaises(NotImplementedError):
            TestHandler("")

        class FullTestHandler(BaseHandler):
            def handle(self):
                pass
            def _is_valid_inputs(self, inputs:Any)->None:
                pass
        FullTestHandler("")



