import codecs
import os
import re

from setuptools import setup

here = os.path.abspath(os.path.dirname(__file__))


def read(*parts):
    with codecs.open(os.path.join(here, *parts), 'r') as fp:
        return fp.read()


def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


setup(
    author='Learning Equality',
    author_email='aron@learningequality.org',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7'
    ],
    description='',
    install_requires=['SQLAlchemy>=1.1.10', 'futures>=3.1.1'],
    keywords=('queue', 'async'),
    license='MIT',
    long_description='',
    name='iceqube',
    package_data={},
    package_dir={'': "src"},
    packages=[
        'iceqube',
    ],
    url='https://github.com/learningequality/iceqube',
    version=find_version('src', 'iceqube', '__init__.py'),
    zip_safe=True
)
