
import unittest
import os

from multiprocessing import Pipe, Queue
from time import sleep

from core.correctness.vars import CHAR_LOWERCASE, CHAR_UPPERCASE, \
    SHA256, TEST_MONITOR_BASE, COMPLETE_NOTEBOOK, EVENT_TYPE, EVENT_PATH
from core.functionality import generate_id, wait, get_file_hash, rmtree, \
    make_dir, parameterize_jupyter_notebook, create_event
    

class CorrectnessTests(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        make_dir(TEST_MONITOR_BASE, ensure_clean=True)

    def tearDown(self) -> None:
        super().tearDown()
        rmtree(TEST_MONITOR_BASE)

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

    def testCreateEvent(self)->None:
        event = create_event("test", "path")

        self.assertEqual(type(event), dict)
        self.assertTrue(EVENT_TYPE in event.keys())
        self.assertEqual(len(event.keys()), 2)
        self.assertEqual(event[EVENT_TYPE], "test")
        self.assertEqual(event[EVENT_PATH], "path")

        event2 = create_event("test2", "path2", {"a":1})

        self.assertEqual(type(event2), dict)
        self.assertTrue(EVENT_TYPE in event2.keys())
        self.assertEqual(len(event2.keys()), 3)
        self.assertEqual(event2[EVENT_TYPE], "test2")
        self.assertEqual(event2[EVENT_PATH], "path2")
        self.assertEqual(event2["a"], 1)

