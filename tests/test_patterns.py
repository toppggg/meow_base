
import shutil
import os
import unittest

from multiprocessing import Pipe

from core.correctness.vars import FILE_EVENTS, FILE_CREATE_EVENT, PIPE_READ, \
    PIPE_WRITE, BAREBONES_NOTEBOOK
from core.functionality import create_rules
from patterns.file_event_pattern import FileEventPattern, WatchdogMonitor
from recipes import JupyterNotebookRecipe

TEST_BASE = "test_base"

class CorrectnessTests(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        if not os.path.exists(TEST_BASE):
            os.mkdir(TEST_BASE)

    def tearDown(self) -> None:
        super().tearDown()
        if os.path.exists(TEST_BASE):
            shutil.rmtree(TEST_BASE)

    def testFileEventPatternCreationMinimum(self)->None:
        FileEventPattern("name", "path", "recipe", "file")

    def testFileEventPatternCreationEmptyName(self)->None:
        with self.assertRaises(ValueError):
            FileEventPattern("", "path", "recipe", "file")

    def testFileEventPatternCreationEmptyPath(self)->None:
        with self.assertRaises(ValueError):
            FileEventPattern("name", "", "recipe", "file")

    def testFileEventPatternCreationEmptyRecipe(self)->None:
        with self.assertRaises(ValueError):
            FileEventPattern("name", "path", "", "file")

    def testFileEventPatternCreationEmptyFile(self)->None:
        with self.assertRaises(ValueError):
            FileEventPattern("name", "path", "recipe", "")

    def testFileEventPatternCreationInvalidName(self)->None:
        with self.assertRaises(ValueError):
            FileEventPattern("@name", "path", "recipe", "file")

    def testFileEventPatternCreationInvalidRecipe(self)->None:
        with self.assertRaises(ValueError):
            FileEventPattern("name", "path", "@recipe", "file")

    def testFileEventPatternCreationInvalidFile(self)->None:
        with self.assertRaises(ValueError):
            FileEventPattern("name", "path", "recipe", "@file")

    def testFileEventPatternSetupName(self)->None:
        name = "name"
        fep = FileEventPattern(name, "path", "recipe", "file")
        self.assertEqual(fep.name, name)

    def testFileEventPatternSetupPath(self)->None:
        path = "path"
        fep = FileEventPattern("name", path, "recipe", "file")
        self.assertEqual(fep.triggering_path, path)

    def testFileEventPatternSetupRecipe(self)->None:
        recipe = "recipe"
        fep = FileEventPattern("name", "path", recipe, "file")
        self.assertEqual(fep.recipe, recipe)

    def testFileEventPatternSetupFile(self)->None:
        file = "file"
        fep = FileEventPattern("name", "path", "recipe", file)
        self.assertEqual(fep.triggering_file, file)

    def testFileEventPatternSetupParementers(self)->None:
        parameters = {
            "a": 1,
            "b": True
        }
        fep = FileEventPattern(
            "name", "path", "recipe", "file", parameters=parameters)
        self.assertEqual(fep.parameters, parameters)

    def testFileEventPatternSetupOutputs(self)->None:
        outputs = {
            "a": "a",
            "b": "b"
        }
        fep = FileEventPattern(
            "name", "path", "recipe", "file", outputs=outputs)
        self.assertEqual(fep.outputs, outputs)

    def testFileEventPatternEventMask(self)->None:
        fep = FileEventPattern("name", "path", "recipe", "file")
        self.assertEqual(fep.event_mask, FILE_EVENTS)

        with self.assertRaises(TypeError):
            fep = FileEventPattern("name", "path", "recipe", "file", 
                event_mask=FILE_CREATE_EVENT)

        with self.assertRaises(ValueError):
            fep = FileEventPattern("name", "path", "recipe", "file", 
                event_mask=["nope"])

        with self.assertRaises(ValueError):
            fep = FileEventPattern("name", "path", "recipe", "file", 
                event_mask=[FILE_CREATE_EVENT, "nope"])

        self.assertEqual(fep.event_mask, FILE_EVENTS)

    def testWatchdogMonitorMinimum(self)->None:
        to_monitor = Pipe()
        from_monitor = Pipe()
        WatchdogMonitor(TEST_BASE, {}, from_monitor[PIPE_WRITE], 
            to_monitor[PIPE_READ])

    def testWatchdogMonitorEventIdentificaion(self)->None:
        to_monitor = Pipe()
        from_monitor = Pipe()

        pattern_one = FileEventPattern(
            "pattern_one", "A", "recipe_one", "file_one")
        recipe = JupyterNotebookRecipe(
            "recipe_one", BAREBONES_NOTEBOOK)

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe.name: recipe,
        }
        rules = create_rules(patterns, recipes)

        wm = WatchdogMonitor(TEST_BASE, rules, from_monitor[PIPE_WRITE], 
            to_monitor[PIPE_READ])

        wm.start()

        open(os.path.join(TEST_BASE, "A"), "w")
        if from_monitor[PIPE_READ].poll(3):
            message = from_monitor[PIPE_READ].recv()

        self.assertIsNotNone(message)
        event, rule = message
        self.assertIsNotNone(event)
        self.assertIsNotNone(rule)
        self.assertEqual(event.src_path, os.path.join(TEST_BASE, "A"))

        open(os.path.join(TEST_BASE, "B"), "w")
        if from_monitor[PIPE_READ].poll(3):
            new_message = from_monitor[PIPE_READ].recv()
        else:
            new_message = None
        self.assertIsNone(new_message)

        wm.stop()
