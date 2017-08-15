target(
  name="src",
  dependencies=[
    "//src:lib"
  ]
)

target(
  name="tests",
  dependencies=[
    "//tests:tests"
  ]
)

# Generate an sdist
# has to be separate from the actual target that collects source files (in this case :src)
# because otherwise it'll error out with a cryptic exception
python_library(
  name="sdist",
  dependencies=[
    ":src",
  ],
  provides=setup_py(
    name="barbequeue",
    version="0.0.1",
    description="",
    long_description="",
    author='Learning Equality',
    author_email='aron+barbequeue@learningequality.org',
    url='https://github.com/learningequality/barbequeue',
    license='MIT',
    zip_safe=True,
    keywords=('queue', 'async'),
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: PyPy',
    ]
  )
)