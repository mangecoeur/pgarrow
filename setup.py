import os

from setuptools import setup
from Cython.Build import cythonize

import numpy as np

import pyarrow as pa

requirements = [
    # package requirements go here
]


os.environ['CFLAGS'] = '-std=c++11 -stdlib=libc++'

ext_modules = cythonize("pgarrow/*.pyx", annotate=True)

for ext in ext_modules:
    # The Numpy C headers are currently required
    ext.include_dirs.append(np.get_include())
    ext.include_dirs.append(pa.get_include())
    ext.libraries.extend(pa.get_libraries())
    ext.library_dirs.extend(pa.get_library_dirs())

setup(
    name='pgarrow',
    version='0.1.0',
    description="postgres to arrow",
    author="jonathan chambers",
    author_email='jon.chambers3001@gmail.com',
    url='https://github.com/mangecoeur/pgarrow',
    packages=['pgarrow'],
    # TODO probably don't need this
    entry_points={
        'console_scripts': [
            'pgarrow=pgarrow.cli:cli'
        ]
    },
    install_requires=requirements,
    keywords='pgarrow',
    classifiers=[
        'Programming Language :: Python :: 3.6'],

    ext_modules = ext_modules
)
