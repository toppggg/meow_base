
import unittest
import os

from multiprocessing import Pipe, Queue
from time import sleep

from core.correctness.vars import CHAR_LOWERCASE, CHAR_UPPERCASE, \
    BAREBONES_NOTEBOOK, SHA256, TEST_MONITOR_BASE, COMPLETE_NOTEBOOK
from core.functionality import create_rules, generate_id, wait, \
    check_pattern_dict, check_recipe_dict, get_file_hash, rmtree, make_dir, \
    parameterize_jupyter_notebook
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
        super().setUp()
        make_dir(TEST_MONITOR_BASE, ensure_clean=True)

    def tearDown(self) -> None:
        super().tearDown()
        rmtree(TEST_MONITOR_BASE)

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

    def testCheckPatternDictValid(self)->None:
        fep1 = FileEventPattern("name_one", "path", "recipe", "file")
        fep2 = FileEventPattern("name_two", "path", "recipe", "file")

        patterns = {
            fep1.name: fep1,
            fep2.name: fep2
        }

        check_pattern_dict(patterns=patterns)

    def testCheckPatternDictNoEntries(self)->None:
        with self.assertRaises(ValueError):
            check_pattern_dict(patterns={})

        check_pattern_dict(patterns={}, min_length=0)

    def testCheckPatternDictMissmatchedName(self)->None:
        fep1 = FileEventPattern("name_one", "path", "recipe", "file")
        fep2 = FileEventPattern("name_two", "path", "recipe", "file")

        patterns = {
            fep2.name: fep1,
            fep1.name: fep2
        }
        
        with self.assertRaises(KeyError):
            check_pattern_dict(patterns=patterns)

    def testCheckRecipeDictValid(self)->None:
        jnr1 = JupyterNotebookRecipe("recipe_one", BAREBONES_NOTEBOOK)
        jnr2 = JupyterNotebookRecipe("recipe_two", BAREBONES_NOTEBOOK)

        recipes = {
            jnr1.name: jnr1,
            jnr2.name: jnr2
        }

        check_recipe_dict(recipes=recipes)

    def testCheckRecipeDictNoEntires(self)->None:
        with self.assertRaises(ValueError):
            check_recipe_dict(recipes={})

        check_recipe_dict(recipes={}, min_length=0)

    def testCheckRecipeDictMismatchedName(self)->None:
        jnr1 = JupyterNotebookRecipe("recipe_one", BAREBONES_NOTEBOOK)
        jnr2 = JupyterNotebookRecipe("recipe_two", BAREBONES_NOTEBOOK)

        recipes = {
            jnr2.name: jnr1,
            jnr1.name: jnr2
        }

        with self.assertRaises(KeyError):
            check_recipe_dict(recipes=recipes)
    
    def testWaitPipes(self)->None:
        pipe_one_reader, pipe_one_writer = Pipe()
        pipe_two_reader, pipe_two_writer = Pipe()
        
        inputs = [
            pipe_one_reader, pipe_two_reader
        ]

        pipe_one_writer.send(1)
        readables = wait(inputs)

        self.assertIn(pipe_one_reader, readables)
        self.assertEqual(len(readables), 1)
        msg = readables[0].recv()
        self.assertEqual(msg, 1)

        pipe_one_writer.send(1)
        pipe_two_writer.send(2)
        readables = wait(inputs)

        self.assertIn(pipe_one_reader, readables)
        self.assertIn(pipe_two_reader, readables)
        self.assertEqual(len(readables), 2)
        for readable in readables:
            if readable == pipe_one_reader:
                msg = readable.recv()        
                self.assertEqual(msg, 1)
            elif readable == pipe_two_reader:
                msg = readable.recv()        
                self.assertEqual(msg, 2)

    def testWaitQueues(self)->None:
        queue_one = Queue()
        queue_two = Queue()

        inputs = [
            queue_one, queue_two
        ]

        queue_one.put(1)
        readables = wait(inputs)

        self.assertIn(queue_one, readables)
        self.assertEqual(len(readables), 1)
        msg = readables[0].get()
        self.assertEqual(msg, 1)

        queue_one.put(1)
        queue_two.put(2)
        sleep(0.1)
        readables = wait(inputs)

        self.assertIn(queue_one, readables)
        self.assertIn(queue_two, readables)
        self.assertEqual(len(readables), 2)
        for readable in readables:
            if readable == queue_one:
                msg = readable.get()
                self.assertEqual(msg, 1)
            elif readable == queue_two:
                msg = readable.get()
                self.assertEqual(msg, 2)

    def testWaitPipesAndQueues(self)->None:
        pipe_one_reader, pipe_one_writer = Pipe()
        pipe_two_reader, pipe_two_writer = Pipe()
        queue_one = Queue()
        queue_two = Queue()

        inputs = [
            pipe_one_reader, pipe_two_reader, queue_one, queue_two
        ]

        pipe_one_writer.send(1)
        readables = wait(inputs)

        self.assertIn(pipe_one_reader, readables)
        self.assertEqual(len(readables), 1)
        msg = readables[0].recv()
        self.assertEqual(msg, 1)

        pipe_one_writer.send(1)
        pipe_two_writer.send(2)
        readables = wait(inputs)

        self.assertIn(pipe_one_reader, readables)
        self.assertIn(pipe_two_reader, readables)
        self.assertEqual(len(readables), 2)
        for readable in readables:
            if readable == pipe_one_reader:
                msg = readable.recv()        
                self.assertEqual(msg, 1)
            if readable == pipe_two_reader:
                msg = readable.recv()        
                self.assertEqual(msg, 2)

        queue_one.put(1)
        readables = wait(inputs)

        self.assertIn(queue_one, readables)
        self.assertEqual(len(readables), 1)
        msg = readables[0].get()
        self.assertEqual(msg, 1)

        queue_one.put(1)
        queue_two.put(2)
        sleep(0.1)
        readables = wait(inputs)

        self.assertIn(queue_one, readables)
        self.assertIn(queue_two, readables)
        self.assertEqual(len(readables), 2)
        for readable in readables:
            if readable == queue_one:
                msg = readable.get()
                self.assertEqual(msg, 1)
            elif readable == queue_two:
                msg = readable.get()
                self.assertEqual(msg, 2)

        queue_one.put(1)
        pipe_one_writer.send(1)
        sleep(0.1)
        readables = wait(inputs)

        self.assertIn(queue_one, readables)
        self.assertIn(pipe_one_reader, readables)
        self.assertEqual(len(readables), 2)
        for readable in readables:
            if readable == queue_one:
                msg = readable.get()
                self.assertEqual(msg, 1)
            elif readable == pipe_one_reader:
                msg = readable.recv()
                self.assertEqual(msg, 1)

    def testGetFileHashSha256(self)->None:
        file_path = os.path.join(TEST_MONITOR_BASE, "hased_file.txt")
        with open(file_path, 'w') as hashed_file:
            hashed_file.write("Some data\n")
        expected_hash = \
            "8557122088c994ba8aa5540ccbb9a3d2d8ae2887046c2db23d65f40ae63abade"
        
        hash = get_file_hash(file_path, SHA256)
        self.assertEqual(hash, expected_hash)
    
    def testGetFileHashSha256NoFile(self)->None:
        file_path = os.path.join(TEST_MONITOR_BASE, "file.txt")

        with self.assertRaises(FileNotFoundError):        
            get_file_hash(file_path, SHA256)

    def testParameteriseNotebook(self)->None:
        pn = parameterize_jupyter_notebook(
            COMPLETE_NOTEBOOK, {})
        
        self.assertEqual(pn, COMPLETE_NOTEBOOK)

        pn = parameterize_jupyter_notebook(
            COMPLETE_NOTEBOOK, {"a": 4})
        
        self.assertEqual(pn, COMPLETE_NOTEBOOK)

        pn = parameterize_jupyter_notebook(
            COMPLETE_NOTEBOOK, {"s": 4})

        self.assertNotEqual(pn, COMPLETE_NOTEBOOK)
        self.assertEqual(
            pn["cells"][0]["source"], 
            "# The first cell\n\ns = 4\nnum = 1000")
