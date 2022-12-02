
import unittest

from core.meow import BasePattern, BaseRecipe, BaseRule


class MeowTests(unittest.TestCase):
    def setUp(self)->None:
        return super().setUp()
    
    def tearDown(self)->None:
        return super().tearDown()

    def testBaseRecipe(self)->None:
        with self.assertRaises(TypeError):
            BaseRecipe("", "")

    def testBasePattern(self)->None:
        with self.assertRaises(TypeError):
            BasePattern("", "")

    def testBaseRule(self)->None:
        with self.assertRaises(TypeError):
            BaseRule("", "")
