
import unittest

from typing import Any

from core.correctness.validation import check_input, valid_string, valid_dict
from core.correctness.vars import VALID_NAME_CHARS


class CorrectnessTests(unittest.TestCase):
    def setUp(self)->None:
        return super().setUp()

    def tearDown(self)->None:
        return super().tearDown()

    def testCheckInputValid(self)->None:
        check_input(1, int)
        check_input(0, int)
        check_input(False, bool)
        check_input(True, bool)
        check_input(1, Any)
        
    def testCheckInputMistyped(self)->None:
        with self.assertRaises(TypeError):
            check_input(1, str)

    def testCheckInputOrNone(self)->None:
        check_input(None, int, or_none=True)
        with self.assertRaises(TypeError):
            check_input(None, int, or_none=False)

    def testValidStringValid(self)->None:
        valid_string("David_Marchant", VALID_NAME_CHARS)

    def testValidStringEmptyString(self)->None:
        valid_string("", VALID_NAME_CHARS, min_length=0)

    def testValidStringNoValidChars(self)->None:
        with self.assertRaises(ValueError):
            valid_string("David_Marchant", "")

    def testValidStringMistypedInput(self)->None:
        with self.assertRaises(TypeError):
            valid_string(1, VALID_NAME_CHARS)
        with self.assertRaises(TypeError):
            valid_string("David_Marchant", 1)

    def testValidStringMissingChars(self)->None:
        with self.assertRaises(ValueError):
            valid_string("David Marchant", VALID_NAME_CHARS)

    def testValidStringInsufficientLength(self)->None:
        with self.assertRaises(ValueError):
            valid_string("", VALID_NAME_CHARS)
            valid_string("David Marchant", VALID_NAME_CHARS, min_length=50)

    def testValidDictMinimum(self)->None:
        valid_dict({"a": 0, "b": 1}, str, int, strict=False)

    def testValidDictAnyKeyType(self)->None:
        valid_dict({"a": 0, "b": 1}, Any, int, strict=False)

    def testValidDictAnyValueType(self)->None:
        valid_dict({"a": 0, "b": 1}, str, Any, strict=False)

    def testValidDictAllRequiredKeys(self)->None:
        valid_dict({"a": 0, "b": 1}, str, int, required_keys=["a", "b"])

    def testValidDictAllRequiredOrOptionalKeys(self)->None:
        valid_dict(
            {"a": 0, "b": 1}, str, int, required_keys=["a"], 
            optional_keys=["b"])

    def testValidDictExtraKeys(self)->None:
        valid_dict(
            {"a": 0, "b": 1, "c": 2}, str, int, required_keys=["a"], 
            optional_keys=["b"], strict=False)

    def testValidDictMissingRequiredKeys(self)->None:
        with self.assertRaises(KeyError):
            valid_dict(
                {"a": 0, "b": 1}, str, int, required_keys=["a", "b", "c"])

    def testValidDictOverlyStrict(self)->None:
        with self.assertRaises(ValueError):
            valid_dict({"a": 0, "b": 1}, str, int)
