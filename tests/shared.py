"""
This file contains shared functions used within multiple tests.

Author(s): David Marchant
"""
from core.functionality import make_dir, rmtree
from core.correctness.vars import TEST_HANDLER_BASE, TEST_JOB_OUTPUT, \
    TEST_MONITOR_BASE

def setup():
    make_dir(TEST_MONITOR_BASE, ensure_clean=True)
    make_dir(TEST_HANDLER_BASE, ensure_clean=True)
    make_dir(TEST_JOB_OUTPUT, ensure_clean=True)

def teardown():
    rmtree(TEST_MONITOR_BASE)
    rmtree(TEST_HANDLER_BASE)
    rmtree(TEST_JOB_OUTPUT)
    rmtree("first")
