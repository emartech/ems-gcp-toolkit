SHELL=/bin/bash
.PHONY: test publish build check

build: ## Build package
	python setup.py sdist

publish: build ## Publish package to PyPi
	twine upload dist/*

test: ## Run all tests
	python -m unittest
	python -m unittest discover . "it_*.py"

check:
	pylint --rcfile=.pylintrc --output-format=colorized \
		cloudsql \
		tests/*/cloudsql \
#		bigquery \
#		pubsub \
#		storage \
#		tests \
