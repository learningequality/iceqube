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
	rm -r dist/*
	./pants setup-py src:lib
	./pants setup-py --setup-py-run="bdist_wheel --universal -d $(PWD)/dist" src:lib
	# Copy over the generated setup.py to the root dir, to allow github installation
	cp dist/iceqube-*/setup.py ./setup.py

release: dist
	twine upload dist/iceqube-*.{whl,tar.gz}

test:
	py.test

test-all:
	tox
