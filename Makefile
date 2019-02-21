SHELL=/bin/bash
.PHONY: test publish build check
.DEFAULT_GOAL := build

build: check test it-test ## Build package

dist-build:
	python setup.py sdist

publish: dist-build ## Publish package to PyPi
	twine upload dist/*

test: ## Run all tests
	python -m unittest

it-test:
	python -m unittest discover . "it_*.py"

check:
	pylint --rcfile=.pylintrc --output-format=colorized \
		setup.py\
		cloudsql \
		tests/*/cloudsql \
#		bigquery \
#		pubsub \
#		storage \
#		tests \
