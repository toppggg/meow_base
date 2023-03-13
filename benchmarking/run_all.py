
import matplotlib.pyplot as pyplot
import numpy
import sys
import time
import os

from shared import JOBS_COUNTS, REPEATS, TESTS, MRME, MRSE, SRME, SRSEP, SRSES, RESULTS_DIR, BASE, GRAPH_FILENAME
from mrme import multiple_rules_multiple_events
from mrse import multiple_rules_single_event
from srme import single_rule_multiple_events
from srsep import single_rule_single_event_parallel
from srsps import single_rule_single_event_sequential

from meow_base.core.correctness.vars import DEFAULT_JOB_OUTPUT_DIR, DEFAULT_JOB_QUEUE_DIR
from meow_base.functionality.file_io import rmtree

LINE_KEYS = {
    SRSES: ('x','#a1467e'),
    SRME: ('.','#896cff'),
    MRME: ('d','#5983b0'),
    MRSE: ('P','#ff6cbe'),
    SRSEP: ('*','#3faf46'),
}


def run_tests():
    rmtree(RESULTS_DIR)

    requested_jobs=0
    for job_count in JOBS_COUNTS:
        requested_jobs += job_count * REPEATS * len(TESTS)
    print(f"requested_jobs: {requested_jobs}")

    runtime_start=time.time()
  
    job_counter=0
    for job_count in JOBS_COUNTS:
        for test in TESTS:
            if test == MRME:
                multiple_rules_multiple_events(job_count, REPEATS, job_counter, requested_jobs, runtime_start)
                job_counter += job_count * REPEATS

            elif test == MRSE:
                multiple_rules_single_event(job_count, REPEATS, job_counter, requested_jobs, runtime_start)
                job_counter += job_count * REPEATS

            elif test == SRME:
                single_rule_multiple_events(job_count, REPEATS, job_counter, requested_jobs, runtime_start)
                job_counter += job_count * REPEATS

            elif test == SRSEP:
                single_rule_single_event_parallel(job_count, REPEATS, job_counter, requested_jobs, runtime_start)
                job_counter += job_count * REPEATS

            elif test == SRSES:
                single_rule_single_event_sequential(job_count, REPEATS, job_counter, requested_jobs, runtime_start)
                job_counter += job_count * REPEATS

    print(f"All tests completed in: {str(time.time()-runtime_start)}")

def get_meow_graph(results_dir):
    lines = []

    for run_type in os.listdir(results_dir):
        #if run_type == 'single_Pattern_single_file_sequential':
        #        continue

#       lines.append((f'scheduling {run_type}', [], 'solid'))
        lines.append((run_type, [], 'solid'))
        run_type_path = os.path.join(results_dir, run_type)

        for job_count in os.listdir(run_type_path):
            results_path = os.path.join(run_type_path, job_count, 'results.txt')
            with open(results_path, 'r') as f_in:
                data = f_in.readlines()

            scheduling_duration = 0
            for line in data:
                if "Average schedule time: " in line:
                    scheduling_duration = float(line.replace("Average schedule time: ", ''))
    
            lines[-1][1].append((job_count, scheduling_duration))
            lines[-1][1].sort(key=lambda y: float(y[0]))

    return lines

def make_plot(lines, graph_path, title, logged):
    w = 10
    h = 4
    linecount = 0
    columns = 1

    pyplot.figure(figsize=(w, h))
    for l in range(len(lines)):
        x_values = numpy.asarray([float(i[0]) for i in lines[l][1]])
        y_values = numpy.asarray([float(i[1]) for i in lines[l][1]])

        # Remove this check to always display lines
        if lines[l][2] == 'solid':
            pyplot.plot(x_values, y_values, label=lines[l][0], linestyle=lines[l][2], marker=LINE_KEYS[lines[l][0]][0], color=LINE_KEYS[lines[l][0]][1])
            linecount += 1

    columns = int(linecount/3) + 1
                    
    pyplot.xlabel("Number of jobs scheduled")
    pyplot.ylabel("Time taken (seconds)")
    pyplot.title(title)

    handles, labels = pyplot.gca().get_legend_handles_labels()
    #    legend_order = [2, 4, 0, 1, 3]
    #    pyplot.legend([handles[i] for i in legend_order], [labels[i] for i in legend_order])

    pyplot.legend(ncol=columns, prop={'size': 12})
    if logged:
        pyplot.yscale('log')

    x_ticks = []
    for tick in x_values:
        label = int(tick)
        if tick <= 100 and tick % 20 == 0:
            label = f"\n{int(tick)}"
        x_ticks.append(label)

    pyplot.xticks(x_values, x_ticks)

    pyplot.savefig(graph_path, format='pdf', bbox_inches='tight')

def make_both_plots(lines, path, title, log=True):
    make_plot(lines, path, title, False)
    if log:
        logged_path = path[:path.index(".pdf")] + "_logged" + path[path.index(".pdf"):]
        make_plot(lines, logged_path, title, True)


def make_graphs():
    lines = get_meow_graph(RESULTS_DIR)

    make_both_plots(lines, "result.pdf", "MiG scheduling overheads on the Threadripper")

    average_lines = []
    all_delta_lines = []
    no_spsfs_delta_lines = []
    for line_signature, line_values, lines_style in lines:
        if lines_style == 'solid':
            averages = [(i, v/float(i)) for i, v in line_values]
            average_lines.append((line_signature, averages, lines_style))

            if line_signature not in ["total single_Pattern_single_file_sequential", "scheduling single_Pattern_single_file_sequential_jobs", "SPSFS"]:
                deltas = []
                for i in range(len(line_values)-1):
                    deltas.append( (line_values[i+1][0], (averages[i+1][1]-averages[i][1]) / (float(averages[i+1][0])-float(averages[i][0])) ) )
                no_spsfs_delta_lines.append((line_signature, deltas, lines_style))
            deltas = []
            for i in range(len(line_values)-1):
                deltas.append( (line_values[i+1][0], (averages[i+1][1]-averages[i][1]) / (float(averages[i+1][0])-float(averages[i][0])) ) )
            all_delta_lines.append((line_signature, deltas, lines_style))


    make_both_plots(average_lines, "result_averaged.pdf", "Per-job MiG scheduling overheads on the Threadripper")

    make_both_plots(all_delta_lines, "result_deltas.pdf", "Difference in per-job MiG scheduling overheads on the Threadripper", log=False)

if __name__ == '__main__':
    try:
        run_tests()
        make_graphs()
        rmtree(DEFAULT_JOB_QUEUE_DIR)
        rmtree(DEFAULT_JOB_OUTPUT_DIR)
        rmtree(BASE)
    except KeyboardInterrupt as ki:
        try:
            sys.exit(1)
        except SystemExit:
            os._exit(1)