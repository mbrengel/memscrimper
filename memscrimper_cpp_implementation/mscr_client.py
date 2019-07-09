#! /usr/bin/env python
# Copyright [2019] <Daniel Weber>

import sys

# check running python version and load corresponding mscr_client version
if sys.version_info.major == 3:
    from mscr_client_python3 import *
else:
    from mscr_client_python2 import *

