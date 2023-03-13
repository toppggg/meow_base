
import datetime
import io
import os
import pathlib
import time
import yaml

from typing import Any, Dict, Tuple

from meow_base.core.correctness.vars import DEFAULT_JOB_OUTPUT_DIR, DEFAULT_JOB_QUEUE_DIR
from meow_base.core.runner import MeowRunner
from meow_base.patterns.file_event_pattern import WatchdogMonitor
from meow_base.recipes.jupyter_notebook_recipe import PapermillHandler
from meow_base.conductors import LocalPythonConductor
from meow_base.functionality.file_io import rmtree

RESULTS_DIR = "results"
BASE = "benchmark_base"
GRAPH_FILENAME = "graph.pdf"
REPEATS = 10
JOBS_COUNTS = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 125, 150, 175, 200, 250, 300, 400, 500]

SRME = "single_rule_multiple_events"
MRSE = "multiple_rules_single_event"
SRSEP = "single_rule_single_event_parallel"
MRME = "multiple_rules_multiple_events"
SRSES = "single_rule_single_event_sequential"

TESTS = [ 
    SRME, 
    MRSE,
    SRSEP, 
    MRME,
    # This test will take approx 90% of total time
    SRSES 
]


class DummyConductor(LocalPythonConductor):
    def valid_execute_criteria(self, job:Dict[str,Any])->Tuple[bool,str]:
        return False, ">:("


def datetime_to_timestamp(date_time_obj):
    return time.mktime(date_time_obj.timetuple()) + float(date_time_obj.microsecond)/1000000 

def generate(file_count, file_path, file_type='.txt'):
    first_filename = ''
    start = time.time()
    for i in range(int(file_count)):
        filename = file_path + str(i) + file_type
        if not first_filename:
            first_filename = filename
        with open(filename, 'w') as f:
            f.write('0')
    return first_filename, time.time() - start

def cleanup(jobs, file_out, base_time, gen_time, execution=False):
    if not jobs:
        return

    job_timestamps = []
    for job in jobs:
        if execution:
            with open(f"{DEFAULT_JOB_OUTPUT_DIR}/{job}/job.yml", 'r') as f_in:
                data = yaml.load(f_in, Loader=yaml.Loader)
        else:
            with open(f"{DEFAULT_JOB_QUEUE_DIR}/{job}/job.yml", 'r') as f_in:
                data = yaml.load(f_in, Loader=yaml.Loader)
        create_datetime = data['create']
        create_timestamp = datetime_to_timestamp(create_datetime)
        job_timestamps.append((create_timestamp, create_datetime))

    job_timestamps.sort(key=lambda y: int(y[0]))

    first = job_timestamps[0]
    last = job_timestamps[-1]

    #dt = datetime.datetime.fromtimestamp(os.path.getctime(base_time), datetime.timezone(datetime.timedelta(hours=0)))
    dt = datetime.datetime.fromtimestamp(os.path.getctime(base_time))

#    if execution:
#        queue_times = []
#        execution_times = []
#        for j in jobs:
#            mrsl_dict = load(os.path.join(mrsl_dir, j))#
#
#            queue_times.append(time.mktime(mrsl_dict['EXECUTING_TIMESTAMP']) - time.mktime(mrsl_dict['QUEUED_TIMESTAMP']))
#            execution_times.append(time.mktime(mrsl_dict['FINISHED_TIMESTAMP']) - time.mktime(mrsl_dict['EXECUTING_TIMESTAMP']))
    pathlib.Path(os.path.dirname(file_out)).mkdir(parents=True, exist_ok=True)
    with open(file_out, 'w') as f_out:
        f_out.write("Job count: "+ str(len(jobs)) +"\n")
        f_out.write("Generation time: "+ str(round(gen_time, 5)) +"\n")
        f_out.write("First trigger: "+ str(dt) +"\n")
        f_out.write("First scheduling datetime: "+ str(first[1]) +"\n")
        f_out.write("Last scheduling datetime: "+ str(last[1]) +"\n")
        f_out.write("First scheduling unixtime: "+ str(first[0]) +"\n")
        f_out.write("First scheduling unixtime: "+ str(last[0]) +"\n")
        f_out.write("Scheduling difference (seconds): "+ str(round(last[0] - first[0], 3)) +"\n")
        f_out.write("Initial scheduling delay (seconds): "+ str(round(first[0] - os.path.getctime(base_time), 3)) +"\n")
        total_time = round(last[0] - os.path.getctime(base_time), 3)
        f_out.write("Total scheduling delay (seconds): "+ str(total_time) +"\n")

#        if execution:
#            f_out.write("Average execution time (seconds): "+ str(round(mean(execution_times), 3)) +"\n")
#            f_out.write("Max execution time (seconds): "+ str(round(max(execution_times), 3)) +"\n")
#            f_out.write("Min execution time (seconds): "+ str(round(min(execution_times), 3)) +"\n")

#            f_out.write("Average queueing delay (seconds): "+ str(round(mean(queue_times), 3)) +"\n")
#            f_out.write("Max queueing delay (seconds): "+ str(round(max(queue_times), 3)) +"\n")
#            f_out.write("Min queueing delay (seconds): "+ str(round(min(queue_times), 3)) +"\n")

#            queue_times.remove(max(queue_times))
#            f_out.write("Average excluded queueing delay (seconds): "+ str(round(mean(queue_times), 3)) +"\n")

    return total_time

def mean(l):
    return sum(l)/len(l)

def collate_results(base_results_dir):

    scheduling_delays = []

    for run in os.listdir(base_results_dir):
        if run != 'results.txt':
            with open(os.path.join(base_results_dir, run, 'results.txt'), 'r') as f:
                d = f.readlines()

                for l in d:
                    if "Total scheduling delay (seconds): " in l:
                        scheduling_delays.append(float(l.replace("Total scheduling delay (seconds): ", '')))

    with open(os.path.join(base_results_dir, 'results.txt'), 'w') as f:
        f.write(f"Average schedule time: {round(mean(scheduling_delays), 3)}\n")
        f.write(f"Scheduling times: {scheduling_delays}")

def run_test(patterns, recipes, files_count, expected_job_count, repeats, job_counter, requested_jobs, runtime_start, signature='', execution=False, print_logging=False):
    if not os.path.exists(RESULTS_DIR):
        os.mkdir(RESULTS_DIR)

    # Does not work. left here as reminder
    if execution:
        os.system("export LC_ALL=C.UTF-8")
        os.system("export LANG=C.UTF-8")

    for run in range(repeats):
        # Ensure complete cleanup from previous run
        for f in [BASE, DEFAULT_JOB_QUEUE_DIR, DEFAULT_JOB_OUTPUT_DIR]:
            if os.path.exists(f):
                rmtree(f)

        file_base = os.path.join(BASE, 'testing')
        pathlib.Path(file_base).mkdir(parents=True, exist_ok=True)

        runner_debug_stream = io.StringIO("")

        if execution:
            runner = MeowRunner(
                WatchdogMonitor(BASE, patterns, recipes, settletime=1),
                PapermillHandler(),
                LocalPythonConductor(),
                print=runner_debug_stream,
                logging=3
            )
        else:
            runner = MeowRunner(
                WatchdogMonitor(BASE, patterns, recipes, settletime=1),
                PapermillHandler(),
                DummyConductor(),
                print=runner_debug_stream,
                logging=3
            )
        
#        meow.WorkflowRunner(
#            VGRID,
#            num_workers,
#            patterns=patterns,
#            recipes=recipes,
#            daemon=True,
#            start_workers=False,
#            retro_active_jobs=False,
#            print_logging=print_logging,
#            file_logging=False,
#            wait_time=1
#        )

        runner.start()

        # Generate triggering files
        first_filename, generation_duration = generate(files_count, file_base +"/file_")

        idle_loops = 0
        total_loops = 0
        messages = 0
        total_time = expected_job_count * 3
        if execution:
            total_time = expected_job_count * 5
        while idle_loops < 10 and total_loops < total_time:
            time.sleep(1)
            runner_debug_stream.seek(0)
            new_messages = len(runner_debug_stream.readlines())

            if messages == new_messages:               
                idle_loops += 1
            else:
                idle_loops = 0
                messages = new_messages
            total_loops += 1

        runner.stop()

        if execution:
            jobs = os.listdir(DEFAULT_JOB_OUTPUT_DIR)
        else:
            jobs = os.listdir(DEFAULT_JOB_QUEUE_DIR)

        results_path = os.path.join(RESULTS_DIR, signature, str(expected_job_count), str(run), 'results.txt')

        cleanup(jobs, results_path, first_filename, generation_duration, execution=execution)

        print(f"Completed scheduling run {str(run + 1)} of {str(len(jobs))}/{str(expected_job_count)} jobs for '{signature}' {job_counter + expected_job_count*(run+1)}/{requested_jobs} ({str(round(time.time()-runtime_start, 3))}s)")

    collate_results(os.path.join(RESULTS_DIR, signature, str(expected_job_count)))
