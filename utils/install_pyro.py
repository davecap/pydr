#!/usr/bin/python
import subprocess
import os
import shutil
import tempfile
import re

try:
    temp_dir = tempfile.mkdtemp(prefix='pyro_install_')
    print 'Installing in %s' % temp_dir
    os.chdir(temp_dir)
    url = 'http://www.xs4all.nl/~irmen/pyro3/download/Pyro-3.10.tar.gz'
    filename = 'Pyro-3.10.tar.gz'
    dir = re.match(r'(.*)\.tar.gz', filename).group(1)

    return_code = subprocess.call(['wget', url])
    if return_code != 0:
        raise Exception('Could not download file from url: %s' % url)
    else:
        print 'Downloaded...'

    return_code = subprocess.call(['tar', 'xzvf', filename])
    if return_code != 0:
        raise Exception('Could not untar file: %s' % filename)
    else:
        print 'Untarred...'

    os.chdir(os.path.abspath(dir))

    return_code = subprocess.call(['python', 'setup.py', 'install'])
    if return_code != 0:
        raise Exception('Could not install Pyro')
finally:
    shutil.rmtree(temp_dir)
