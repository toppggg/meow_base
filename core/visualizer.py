import time
import sys
import os
import threading
import datetime as dt
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import numpy as np

from typing import Any, Union, Dict

from meow_base.core.base_recipe import BaseRecipe
from meow_base.core.base_rule import BaseRule
from meow_base.core.correctness.vars import get_drt_imp_msg, VALID_CHANNELS
from meow_base.core.base_pattern import BasePattern

sys.path.append("C:\\Users\\Johan\OneDrive\\Universitet\\Datalogi\\6. semester\\Bachelor\meow")

class Visualizer:
    # A collection of patterns
    _patterns: Dict[str, BasePattern]
    # A collection of recipes
    _recipes: Dict[str, BaseRecipe] # Might not be needed?
    # A collection of rules derived from _patterns and _recipes
    _rules: Dict[str, BaseRule] # Might not be needed?
    
    # _data_to_show: Dict[str, Any] # use rule.rule to match, and increase the int by 1

    _visualized_seconds_array: Dict[str, list]

    _last_update: int

    _kill = False

    _figure = False


    def __init__(self)->None:
        self._patterns = {}    
        self._recipes = {}
        self._rules = {}
        self._last_update = int(time.time())
        self._visualized_seconds_array = {}
        
        
        # updateThread = threading.Thread(target=self.update)
        # updateThread.start()
        pass
        # self.update()

    def update(self):
        # while not self._kill
            time_this_round = int(time.time())
            time_dif = time_this_round - self._last_update
            
            if time_dif >= 1:
            
                if (time_this_round//60 - self._last_update//60) >= 1: #If the minute has changed, reset the array remaining fields to 0
                    self._copy_arrays()#copy seconds array into minutes array
                    for i in range (self._last_update % 60, 60):
                        for rule in self._rules:
                            self._visualized_seconds_array[rule][i] = 0
                    
                    for i in range(0,time_this_round%60): #in range (0,0)
                        for rule in self._rules:
                            self._visualized_seconds_array[rule][i] = 0
                else : 
                    for i in range(self._last_update%60, time_this_round%60):
                        for rule in self._rules:
                            self._visualized_seconds_array[rule][i] = 0
                        # self._visualized_seconds_array[i] = 0
                # #Go through "tmp sec" dict, and copy into second array.
                
                # print array to file.
                visualizer_dir = "visualizer_print"
                with open(os.path.join(visualizer_dir, "array"), "w") as f:
                    f.write(str(self._visualized_seconds_array) + "\n")
                
                self._last_update = time_this_round

            # time.sleep(0.5)

    def from_runner(self, event:Dict[str,Any])->None:
        time_this_round = int(time.time())
        time_dif = time_this_round - self._last_update
        
        if time_dif < 1: #update has not been run yet this second
            self.update()

        visualizer_dir = "visualizer_print"
        if event["rule"].__str__() not in self._rules: #Check if the rule already exists, otherwise add it to the rules that should be visualized
            with open(os.path.join(visualizer_dir, "debug"), "a") as f:
                # f.write(str(event))
                f.write(event["rule"].__str__() + "\n")
            self.new_rule(event["rule"])

        with open(os.path.join(visualizer_dir, "print"), "a") as f:
            f.write(str(event))
            f.write(event["rule"].__str__() + "\n")
        
        # timestamp = time.ctime(time.time()) 
        timestamp = int(time.time())
        array_index = (timestamp - 3) % 60
        self._visualized_seconds_array[event["rule"].__str__()][array_index] += 1
        # tmp[array_index] += 1
        # self._visualized_seconds_array.update()
        


    #Johans leg


    def new_rule(self, rule: BaseRule)->None:       # {rule, [60]}
        self._rules[rule.__str__()] = rule
        self._visualized_seconds_array[rule.__str__()] = [0] * 60
        if not self._figure:
            updateThread = threading.Thread(target=self.matplotlib,args=())
            updateThread.start()
            # self.matplotlib(int(time.time()))
            self._figure = True

        

    def _copy_arrays(self):
        pass

    def matplotlib(self):
        figure = plt.figure()
        ax = figure.add_subplot(1, 1, 1)
        time_this_round = int(time.time())
        time_initial = dt.datetime.now()
        xs = [(time_initial - dt.timedelta(seconds=float(i))).strftime('%H:%M:%S') for i in range(60, 0, -1)]
        ys = [i for i in range(time_this_round - 60, time_this_round)]
        for rule in self._rules:
            ys = self._visualized_seconds_array[rule]
            # ys = ruleys[(time_this_round % 60):60].append(ruleys[0:(time_this_round % 60)])

        def animate(i, xs, ys):
            time_this_round = int(time.time())
            xs.append(dt.datetime.now().strftime('%H:%M:%S'))
            # ys = np.random.randint(1,30,60)
            for rule in self._rules:
                ruleys = self._visualized_seconds_array[rule]
                ys = ruleys[(time_this_round % 60):60] + (ruleys[0:(time_this_round % 60)])
            xs = xs[-60:]

            ax.clear()
            ax.plot(xs, ys)

            # Format plot
            plt.xticks(np.arange(0, 60, step=5),rotation=45, ha='right')
            plt.subplots_adjust(bottom=0.30)
            plt.title('Rule over time')
            plt.ylabel('Rule')
        
        ani = animation.FuncAnimation(figure, animate, fargs=(xs, ys), interval=1000)
        plt.show()