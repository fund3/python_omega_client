test:
	py.test tests

coverage:
	py.test --cov-report html --cov=$$(python -c "import os; import inspect; os.chdir('tests'); import tes_client; print(os.path.dirname(inspect.getsourcefile(tes_client)));") tests

pylint:
	pylint $$(python -c "import os; import inspect; os.chdir('tests'); import tes_client; print(os.path.dirname(inspect.getsourcefile(tes_client)));") --exit-zero | pylint-json2html -f jsonextended -o pylint.html

quality_check: coverage pylint
	cp pylint.html htmlcov/pylint.html
