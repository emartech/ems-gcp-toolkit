SHELL=/bin/bash
.PHONY: test publish build

build: ## Build package
	python setup.py sdist

publish: build ## Publish package to PyPi
	twine upload dist/*

test: ## Run all tests
	python -m unittest
	python -m unittest discover . "it_*.py"
