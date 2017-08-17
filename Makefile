.PHONY: help clean clean-pyc release dist

help:
	@echo "dist - package"
	@echo "test - run the entire test suite"

dist:
	./pants setup-py src:lib

test:
	./pants test :tests
