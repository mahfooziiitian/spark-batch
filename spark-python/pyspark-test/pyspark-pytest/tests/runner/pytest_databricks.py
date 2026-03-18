import pytest
import os
import sys

if __name__ == '__main__':
    # Get the path to the directory for this file in the workspace.
    dir_root = os.path.dirname(os.path.realpath(__file__))
    print(dir_root)

    # Switch to the root directory.
    os.chdir(dir_root)

    # Skip writing .pyc files to the bytecode cache on the cluster.
    sys.dont_write_bytecode = True

    ret_code = pytest.main(sys.argv[1:])
