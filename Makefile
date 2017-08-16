.PHONY: help clean clean-pyc release dist

help:
	@echo "dist - package"
	@echo "test - run the entire test suite"

dist:
	./pants setup-py :sdist

test:
	./pants test :tests
