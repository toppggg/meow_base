
import nbformat
import threading

from multiprocessing import Pipe
from typing import Any

from core.correctness.validation import check_type, valid_string, \
    valid_dict, valid_path, valid_list
from core.correctness.vars import VALID_VARIABLE_NAME_CHARS, VALID_CHANNELS
from core.functionality import wait
from core.meow import BaseRecipe, BaseHandler

class JupyterNotebookRecipe(BaseRecipe):
    source:str
    def __init__(self, name:str, recipe:Any, parameters:dict[str,Any]={}, 
            requirements:dict[str,Any]={}, source:str=""):
        super().__init__(name, recipe, parameters, requirements)
        self._is_valid_source(source)
        self.source = source

    def _is_valid_source(self, source:str)->None:
        if source:
            valid_path(source, extension=".ipynb", min_length=0)

    def _is_valid_recipe(self, recipe:dict[str,Any])->None:
        check_type(recipe, dict)
        nbformat.validate(recipe)

    def _is_valid_parameters(self, parameters:dict[str,Any])->None:
        valid_dict(parameters, str, Any, strict=False, min_length=0)
        for k in parameters.keys():
            valid_string(k, VALID_VARIABLE_NAME_CHARS)

    def _is_valid_requirements(self, requirements:dict[str,Any])->None:
        valid_dict(requirements, str, Any, strict=False, min_length=0)
        for k in requirements.keys():
            valid_string(k, VALID_VARIABLE_NAME_CHARS)

class PapermillHandler(BaseHandler):
    _worker:threading.Thread
    _stop_pipe:Pipe
    def __init__(self, inputs:list[VALID_CHANNELS])->None:
        super().__init__(inputs)
        self._worker = None
        self._stop_pipe = Pipe()

    def run(self)->None:
        all_inputs = self.inputs + [self._stop_pipe[0]]
        while True:
            ready = wait(all_inputs)

            if self._stop_pipe[0] in ready:
                return
            else:
                for input in self.inputs:
                    if input in ready:
                        message = input.recv()
                        event, rule = message
                        self.handle(event, rule)

    def start(self)->None:
        if self._worker is None:
            self._worker = threading.Thread(
                target=self.run,
                args=[])
            self._worker.daemon = True
            self._worker.start()
        else:
            raise RuntimeWarning("Repeated calls to start have no effect.")

    def stop(self)->None:
        if self._worker is None:
            raise RuntimeWarning("Cannot stop thread that is not started.")
        else:
            self._stop_pipe[1].send(1)
            self._worker.join()

    def handle(self, event, rule)->None:
        # TODO finish implementation and test
        pass

    def _is_valid_inputs(self, inputs:list[VALID_CHANNELS])->None:
        valid_list(inputs, VALID_CHANNELS)
