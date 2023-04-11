import time
import datetime as dt
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import numpy as np

# from meow_base.core.visualizer import Visualizer

class V_matplotlib:

    def _animate_total(i, xs, ys):
        bottom = np.zeros(60)
        time_this_round = int(time.time())
        xs.append(dt.datetime.now().strftime('%H:%M:%S'))
        # ys = np.random.randint(1,30,60)
        xs = xs[-60:]
        ax.clear()
        width = 1
        for rule in visualizer._rules.keys():
            ruleys = visualizer._visualized_seconds_array[rule]
            ys = ruleys[(time_this_round % 60):60] + (ruleys[0:(time_this_round % 60)])
            ys = np.array(ys)
            p = ax.bar(xs, ys, label=visualizer._rules[rule].__str__(), bottom=bottom, width=width)
            bottom += ys
        # xs = xs[-60:]

        # ax.clear()
        # ax.plot(xs, ys)

        # Format plot
        ax.legend(loc="upper right")
        plt.xticks(np.arange(0, 60, step=5),rotation=45, ha='right')
        # plt.subplots_adjust(bottom=0.30)
        plt.title('Rule over time')
        plt.ylabel('Rule')

    def matplotlib(self, visualizer):
        figure, ax = plt.subplots()

        # figure = plt.figure()
        # ax = figure.add_subplot(1, 1, 1)

        time_this_round = int(time.time())
        time_initial = dt.datetime.now()
        xs = [(time_initial - dt.timedelta(seconds=float(i))).strftime('%H:%M:%S') for i in range(60, 0, -1)]
        ys = [i for i in range(time_this_round - 60, time_this_round)]
        for rule in visualizer._rules:
            ys = visualizer._visualized_seconds_array[rule]
            # ys = ruleys[(time_this_round % 60):60].append(ruleys[0:(time_this_round % 60)])


        
        ani = animation.FuncAnimation(figure, self._animate_total, fargs=(xs, ys), interval=1000)
        plt.show()