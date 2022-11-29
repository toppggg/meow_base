
import unittest

from core.correctness.validation import check_input, valid_string
from core.correctness.vars import VALID_NAME_CHARS
from core.meow import BasePattern, BaseRecipe, BaseRule


class CorrectnessTests(unittest.TestCase):
    def setUp(self) -> None:
        return super().setUp()

    def tearDown(self) -> None:
        return super().tearDown()

    def testCheckInput(self):
        # Valid input
        check_input(1, int)
        check_input(0, int)
        check_input(False, bool)
        check_input(True, bool)
        
        # Misstyped input
        with self.assertRaises(TypeError):
            check_input(1, str)

        # Or none
        check_input(None, int, or_none=True)
        with self.assertRaises(TypeError):
            check_input(None, int, or_none=False)

    def testValidString(self):
        # Valid input
        valid_string("", "")
        valid_string("David_Marchant", VALID_NAME_CHARS)

        # Misstyped input
        with self.assertRaises(TypeError):
            valid_string(1, VALID_NAME_CHARS)
        with self.assertRaises(TypeError):
            valid_string("David_Marchant", 1)

        # Missing chars
        with self.assertRaises(ValueError):
            valid_string("David Marchant", VALID_NAME_CHARS)

class MeowTests(unittest.TestCase):
    def setUp(self) -> None:
        return super().setUp()
    
    def tearDown(self) -> None:
        return super().tearDown()

    def testBaseRecipe(self):
        # Should not be implementable on its own
        with self.assertRaises(TypeError):
            BaseRecipe("", "")

    def testBasePattern(self):
        # Should not be implementable on its own
        with self.assertRaises(TypeError):
            BasePattern("", "")

    def testBaseRule(self):
        # Should not be implementable on its own
        with self.assertRaises(TypeError):
            BaseRule("", "")
