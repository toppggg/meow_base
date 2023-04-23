
import unittest

from typing import List

from meow_base.core.rule import Rule
from meow_base.core.visualizer.visualizer_data.event_queue_data import EventQueueData
from meow_base.core.visualizer.visualizer_struct import VISUALIZER_STRUCT
from meow_base.patterns.file_event_pattern import FileEventPattern
from meow_base.recipes.jupyter_notebook_recipe import JupyterNotebookRecipe
from shared import BAREBONES_NOTEBOOK

class EventQueueDataTest(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        
    
    def testHandelTimeDictType(self):
        self.assertTrue(isinstance(EventQueueData.event_queue,List[VISUALIZER_STRUCT]))

    def testHandelTimeDictType(self):
        self.assertTrue(isinstance(EventQueueData.average_queue_time,dict[VISUALIZER_STRUCT.rule_name,float]))
        
    def testRuleMatchedValidData(self):
        vs = VISUALIZER_STRUCT("rule_testrulename", "pattern_testpattern", "recipe_testrecipe","input/test.txt","eventid_123","","")

        event_queue_data = EventQueueData()
        self.assertNotIn(vs, event_queue_data.event_queue)
        event_queue_data.rule_matched(vs)
        self.assertIn(vs, event_queue_data.event_queue)

    def testRuleMatchedInvalidData(self):
        event_queue_data = EventQueueData()
        fep = FileEventPattern("name", "path", "recipe", "file")
        jnr = JupyterNotebookRecipe("recipe", BAREBONES_NOTEBOOK)
        r = Rule(fep, jnr)

        with self.assertRaises(TypeError):
                event_queue_data.rule_matched(r)


    def testRemoveRuleValidData(self):
        vs = VISUALIZER_STRUCT("rule_testrulename", "pattern_testpattern", "recipe_testrecipe","input/test.txt","eventid_123","","")

        event_queue_data = EventQueueData()
        self.assertIn(vs, event_queue_data.event_queue)
        event_queue_data.remove_rule(vs)
        self.assertNotIn(vs, event_queue_data.event_queue)

    def testRemoveRuleInvalidData(self):
        event_queue_data = EventQueueData()
        fep = FileEventPattern("name", "path", "recipe", "file")
        jnr = JupyterNotebookRecipe("recipe", BAREBONES_NOTEBOOK)
        r = Rule(fep, jnr)

        with self.assertRaises(TypeError):
                event_queue_data.remove_rule(r)

    def testGetRulesAwaitingJob(self):
        #todo: create visualizer list, se at den bliver returneret
        vs1 = VISUALIZER_STRUCT("rule_testrulename1", "pattern_testpattern", "recipe_testrecipe","input/test.txt","eventid_1","","")
        vs2 = VISUALIZER_STRUCT("rule_testrulename2", "pattern_testpattern2", "recipe_testrecipe2","input/test.txt","eventid_2","","")
        vs3 = VISUALIZER_STRUCT("rule_testrulename3", "pattern_testpattern3", "recipe_testrecipe2","input/test.txt","eventid_3","","")
        vs4 = VISUALIZER_STRUCT("rule_testrulename4", "pattern_testpattern4", "recipe_testrecipe2","input/test.txt","eventid_4","","")
        vs5 = VISUALIZER_STRUCT("rule_testrulename5", "pattern_testpattern5", "recipe_testrecipe2","input/test.txt","eventid_5","","")
        lst = [vs1,vs2,vs3,vs4,vs5]
        event_queue_data = EventQueueData()
        event_queue_data.event_queue = lst
        self.assertListEqual(lst,event_queue_data.event_queue)
        