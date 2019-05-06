.PHONY: help clean clean-pyc release dist

help:
	@echo "dist - package"
	@echo "test - run the entire test suite"

clean: clean-build clean-pyc

clean-build:
	rm -fr build/
	rm -fr dist/
	rm -fr dist-packages-cache/
	rm -fr dist-packages-temp/
	rm -fr *.egg-info
	rm -fr .eggs
	rm -fr .cache

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +

dist:
	rm -fr dist/*
	python setup.py sdist --format=gztar > /dev/null # silence the sdist output! Too noisy!
	python setup.py bdist_wheel --universal

release: dist
	twine upload -s dist/*

test:
	py.test

test-all:
	tox
