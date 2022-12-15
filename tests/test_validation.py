
import unittest

from typing import Any, Union

from core.correctness.validation import check_type, check_implementation, \
    valid_string, valid_dict, valid_list
from core.correctness.vars import VALID_NAME_CHARS


class CorrectnessTests(unittest.TestCase):
    def setUp(self)->None:
        return super().setUp()

    def tearDown(self)->None:
        return super().tearDown()

    def testCheckTypeValid(self)->None:
        check_type(1, int)
        check_type(0, int)
        check_type(False, bool)
        check_type(True, bool)

    def testCheckTypeValidAny(self)->None:
        check_type(1, Any)

    def testCheckTypeValidUnion(self)->None:
        check_type(1, Union[int,str])
        with self.assertRaises(TypeError):
            check_type(Union[int, str], Union[int,str])
        
    def testCheckTypeMistyped(self)->None:
        with self.assertRaises(TypeError):
            check_type(1, str)

    def testCheckTypeOrNone(self)->None:
        check_type(None, int, or_none=True)
        with self.assertRaises(TypeError):
            check_type(None, int, or_none=False)

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

    def testValidListMinimum(self)->None:
        valid_list([1, 2, 3], int)
        valid_list(["1", "2", "3"], str)
        valid_list([1], int)

    def testValidListAltTypes(self)->None:
        valid_list([1, "2", 3], int, alt_types=[str])

    def testValidListMismatchedNotList(self)->None:
        with self.assertRaises(TypeError):
            valid_list((1, 2, 3), int)

    def testValidListMismatchedType(self)->None:
        with self.assertRaises(TypeError):
            valid_list([1, 2, 3], str)

    def testValidListMinLength(self)->None:
        with self.assertRaises(ValueError):
            valid_list([1, 2, 3], str, min_length=10)

    def testCheckImplementationMinimum(self)->None:
        class Parent:
            def func():
                pass

        class Child(Parent):
            def func():
                pass

        check_implementation(Child.func, Parent)

    def testCheckImplementationUnaltered(self)->None:
        class Parent:
            def func():
                pass

        class Child(Parent):
            pass
        
        with self.assertRaises(NotImplementedError):
            check_implementation(Child.func, Parent)

    def testCheckImplementationDifferingSig(self)->None:
        class Parent:
            def func():
                pass

        class Child(Parent):
            def func(var):
                pass
        
        with self.assertRaises(NotImplementedError):
            check_implementation(Child.func, Parent)

    def testCheckImplementationAnyType(self)->None:
        class Parent:
            def func(var:Any):
                pass

        class Child(Parent):
            def func(var:str):
                pass
        
        check_implementation(Child.func, Parent)
