
import unittest

from core.correctness.vars import CHAR_LOWERCASE, CHAR_UPPERCASE, \
    BAREBONES_NOTEBOOK
from core.functionality import create_rules, generate_id
from core.meow import BaseRule
from patterns.file_event_pattern import FileEventPattern
from recipes.jupyter_notebook_recipe import JupyterNotebookRecipe

valid_pattern_one = FileEventPattern(
    "pattern_one", "path_one", "recipe_one", "file_one")
valid_pattern_two = FileEventPattern(
    "pattern_two", "path_two", "recipe_two", "file_two")

valid_recipe_one = JupyterNotebookRecipe(
    "recipe_one", BAREBONES_NOTEBOOK)
valid_recipe_two = JupyterNotebookRecipe(
    "recipe_two", BAREBONES_NOTEBOOK)

class CorrectnessTests(unittest.TestCase):
    def setUp(self) -> None:
        return super().setUp()

    def tearDown(self) -> None:
        return super().tearDown()

    def testCreateRulesMinimum(self)->None:
        create_rules({}, {})

    def testGenerateIDWorking(self)->None:
        id = generate_id()
        self.assertEqual(len(id), 16)
        for i in range(len(id)):
            self.assertIn(id[i], CHAR_UPPERCASE+CHAR_LOWERCASE)

        # In extrememly rare cases this may fail due to randomness in algorithm
        new_id = generate_id(existing_ids=[id])
        self.assertNotEqual(id, new_id)

        another_id = generate_id(length=32)
        self.assertEqual(len(another_id), 32)

        again_id = generate_id(charset="a")
        for i in range(len(again_id)):
            self.assertIn(again_id[i], "a")

        with self.assertRaises(ValueError):
            generate_id(length=2, charset="a", existing_ids=["aa"])

        prefix_id = generate_id(length=4, prefix="Test")
        self.assertEqual(prefix_id, "Test")

        prefix_id = generate_id(prefix="Test")
        self.assertEqual(len(prefix_id), 16)
        self.assertTrue(prefix_id.startswith("Test"))

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
