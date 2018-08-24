SHELL=/bin/bash
.PHONY: publish build

build: ## Build package
	python setup.py sdist

publish: build ## Publish package to PyPi
	twine upload dist/*
