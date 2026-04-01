fetch:
	python ingestion/fetch_bmkg.py

dbt-run:
	cd dbt_project && dbt run

dbt-test:
	cd dbt_project && dbt test

pipeline: fetch dbt-run dbt-test
