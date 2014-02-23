# -*- coding: utf-8 -*-

"""
PMR setup script.
"""

from distutils.command.install_data import install_data
from distutils.command.sdist import sdist
from distutils.core import setup
import os
import sys

version = "latest"

try:
    fn = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'VERSION')
    version = open(fn).read().strip()
except IOError:
    from subprocess import Popen, PIPE, STDOUT
    import re

    VERSION_MATCH = re.compile(r'\d+\.\d+\.\d+(\w|-)*')

    try:
        p = Popen(['git', 'describe', '--tags', '--always'], stdout=PIPE, stderr=STDOUT)
        out = p.communicate()[0]

        if (not p.returncode) and out:
            v = VERSION_MATCH.search(out)
            if v:
                version = v.group()
    except OSError:
        pass    

scripts = [] # ["bin/pmr-run"]

if sys.hexversion < 0x02070000:
    raise RuntimeError, "PMR requires Python 2.7 or higher"

class our_install_data(install_data):

    def finalize_options(self):
        self.set_undefined_options('install',
            ('install_lib', 'install_dir'),
        )
        install_data.finalize_options(self)

    def run(self):
        install_data.run(self)
        # ensure there's a pmr/VERSION file
        fn = os.path.join(self.install_dir, 'pmr', 'VERSION')
        open(fn, 'w').write(version)
        self.outfiles.append(fn)

class our_sdist(sdist):

    def make_release_tree(self, base_dir, files):
        sdist.make_release_tree(self, base_dir, files)
        # ensure there's a air/VERSION file
        fn = os.path.join(base_dir, 'pmr', 'VERSION')
        open(fn, 'w').write(version)

setup_args = {
    'name': "PilotMapReduce",
    'version': version,
    'description': "SAGA Pilot-Abstractions based MapReduce Implementation",
    'long_description': "SAGA Pilot-Abstractions based MapReduce Implementation",
    'author': "Pradeep Mantha",
    'author_email': "pradeepm66@gmail.com",
    'maintainer': "Pradeep Mantha",
    'maintainer_email': "pmanth2@cct.lsu.edu",
    'url': "https://github.com/saga-project/PilotMapReduce/wiki",
    'license': "MIT",
    'classifiers': [
        'Development Status :: 5 - Production/Stable',
        'Environment :: No Input/Output (Daemon)',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'License :: OSI Approved :: MIT License',
        'Topic :: System :: Distributed Computing',
        'Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Operating System :: POSIX :: AIX',
        'Operating System :: POSIX :: BSD',
        'Operating System :: POSIX :: BSD :: BSD/OS',
        'Operating System :: POSIX :: BSD :: FreeBSD',
        'Operating System :: POSIX :: BSD :: NetBSD',
        'Operating System :: POSIX :: BSD :: OpenBSD',
        'Operating System :: POSIX :: GNU Hurd',
        'Operating System :: POSIX :: HP-UX',
        'Operating System :: POSIX :: IRIX',
        'Operating System :: POSIX :: Linux',
        'Operating System :: POSIX :: Other',
        'Operating System :: POSIX :: SCO',
        'Operating System :: POSIX :: SunOS/Solaris',
        'Operating System :: Unix'
        ],

    'packages': [ "pmr", "pmr.util"],
    'include_package_data':True,
    'scripts': scripts,
    # mention data_files, even if empty, so install_data is called and
    # VERSION gets copied
    'data_files': [("", ["README","README"])],
    'cmdclass': {
        'install_data': our_install_data,
        'sdist': our_sdist
        }
    }


try:
    # If setuptools is installed, then we'll add setuptools-specific arguments
    # to the setup args.
    import setuptools #@UnusedImport
except ImportError:
    pass
else:
    setup_args['install_requires'] = [
        'bigjob'
    ]
    
setup(**setup_args)
