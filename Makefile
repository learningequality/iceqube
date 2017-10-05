.PHONY: help clean clean-pyc release dist

help:
	@echo "dist - package"
	@echo "test - run the entire test suite"

dist:
	rm -r dist/*
	./pants setup-py src:lib
	./pants setup-py --setup-py-run="bdist_wheel --universal -d $(PWD)/dist" src:lib
	# Copy over the generated setup.py to the root dir, to allow github installation
	cp dist/iceqube-*/setup.py ./setup.py

release: dist
	twine upload dist/iceqube-.{whl,tar.gz}

test:
	./pants test :tests
