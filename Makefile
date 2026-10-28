.PHONY: install lint test samples demo
install:
	python -m pip install -e ".[dev]"
lint:
	ruff check . && ruff format --check .
test:
	pytest -q
samples:
	python samples/make_samples.py
demo:
	python -m samples.run_local_pipeline
