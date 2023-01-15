
import os

from multiprocessing import Queue
from multiprocessing.connection import Connection
from inspect import signature

from typing import Union

# validation
CHAR_LOWERCASE = 'abcdefghijklmnopqrstuvwxyz'
CHAR_UPPERCASE = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
CHAR_NUMERIC = '0123456789'

VALID_NAME_CHARS = CHAR_UPPERCASE + CHAR_LOWERCASE + CHAR_NUMERIC + "_-"

VALID_RECIPE_NAME_CHARS = VALID_NAME_CHARS
VALID_PATTERN_NAME_CHARS = VALID_NAME_CHARS
VALID_RULE_NAME_CHARS = VALID_NAME_CHARS
VALID_VARIABLE_NAME_CHARS = CHAR_UPPERCASE + CHAR_LOWERCASE + CHAR_NUMERIC + "_"

VALID_JUPYTER_NOTEBOOK_FILENAME_CHARS = VALID_NAME_CHARS + "." + os.path.sep
VALID_JUPYTER_NOTEBOOK_EXTENSIONS = [".ipynb"]

VALID_PATH_CHARS = VALID_NAME_CHARS + "." + os.path.sep
VALID_TRIGGERING_PATH_CHARS = VALID_NAME_CHARS + ".*" + os.path.sep

VALID_CHANNELS = Union[Connection,Queue]

# hashing
HASH_BUFFER_SIZE = 65536
SHA256 = "sha256"

# testing 
BAREBONES_NOTEBOOK = {
    "cells": [],
    "metadata": {},
    "nbformat": 4,
    "nbformat_minor": 4
}
TEST_MONITOR_BASE = "test_monitor_base"
TEST_HANDLER_BASE = "test_handler_base"
TEST_JOB_OUTPUT = "test_job_output"
COMPLETE_NOTEBOOK = {
 "cells": [
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": "# The first cell\n\ns = 0\nnum = 1000"
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": "for i in range(num):\n    s += i"
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": "div_by = 4"
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": "result = s / div_by"
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": "print(result)"
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.7.3"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}
APPENDING_NOTEBOOK = {
 "cells": [
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Default parameters values\n",
    "# The line to append\n",
    "extra = 'This line comes from a default pattern'\n",
    "# Data input file location\n",
    "infile = 'start/alpha.txt'\n",
    "# Output file location\n",
    "outfile = 'first/alpha.txt'"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "# load in dataset. This should be a text file\n",
    "with open(infile) as input_file:\n",
    "    data = input_file.read()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Append the line\n",
    "appended = data + '\\n' + extra"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "import os\n",
    "\n",
    "# Create output directory if it doesn't exist\n",
    "output_dir_path = os.path.dirname(outfile)\n",
    "\n",
    "if output_dir_path:\n",
    "    os.makedirs(output_dir_path, exist_ok=True)\n",
    "\n",
    "# Save added array as new dataset\n",
    "with open(outfile, 'w') as output_file:\n",
    "   output_file.write(appended)"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.10.6 (main, Nov 14 2022, 16:10:14) [GCC 11.3.0]"
  },
  "vscode": {
   "interpreter": {
    "hash": "916dbcbb3f70747c44a77c7bcd40155683ae19c65e1c03b4aa3499c5328201f1"
   }
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}

# meow events
EVENT_TYPE = "meow_event_type"
WATCHDOG_TYPE = "watchdog"
WATCHDOG_SRC = "src_path"
WATCHDOG_BASE = "monitor_base"
WATCHDOG_RULE = "rule_name"

# inotify events
FILE_CREATE_EVENT = "file_created"
FILE_MODIFY_EVENT = "file_modified"
FILE_MOVED_EVENT = "file_moved"
FILE_CLOSED_EVENT = "file_closed"
FILE_DELETED_EVENT = "file_deleted"
FILE_RETROACTIVE_EVENT = "retroactive_file_event"
FILE_EVENTS = [
    FILE_CREATE_EVENT, 
    FILE_MODIFY_EVENT, 
    FILE_MOVED_EVENT, 
    FILE_CLOSED_EVENT, 
    FILE_DELETED_EVENT,
    FILE_RETROACTIVE_EVENT
]

DIR_CREATE_EVENT = "dir_created"
DIR_MODIFY_EVENT = "dir_modified"
DIR_MOVED_EVENT = "dir_moved"
DIR_DELETED_EVENT = "dir_deleted"
DIR_RETROACTIVE_EVENT = "retroactive_dir_event"
DIR_EVENTS = [
    DIR_CREATE_EVENT,
    DIR_MODIFY_EVENT,
    DIR_MOVED_EVENT,
    DIR_DELETED_EVENT,
    DIR_RETROACTIVE_EVENT
]

# debug printing levels
DEBUG_ERROR = 1
DEBUG_WARNING = 2
DEBUG_INFO = 3

# debug message functions
def get_drt_imp_msg(base_class):
    return f"{base_class.__name__} may not be instantiated directly. " \
        f"Implement a child class."

def get_not_imp_msg(parent_class, class_function):
    return f"Children of the '{parent_class.__name__}' class must implement " \
        f"the '{class_function.__name__}({signature(class_function)})' " \
        "function"
