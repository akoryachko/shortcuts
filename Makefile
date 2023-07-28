SHELL_PYTHON = $(shell which python3.10)
VENV = .venv
VENV_PYTHON = $(VENV)/bin/python
TEST_PATH = tests

venv:
	$(SHELL_PYTHON) -m venv $(VENV)

install: venv
	(\
	.  $(VENV)/bin/activate;\
	pip install --upgrade pip;\
	pip install -r requirements.txt;\
	)

clean:
	rm -rf $(VENV)

test:
	(\
	.  $(VENV)/bin/activate;\
	python -m pytest $(TEST_PATH) -vv -s;\
	)
