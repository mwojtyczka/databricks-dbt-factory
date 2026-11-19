# One command runs the whole benchmark; `make bench-clean` removes the bundle and arm/raw schemas (not results).
SF ?= 0.001
REPS ?= 1
UV_RUN = uv run --python 3.12 --with-editable ../../ --with databricks-sdk --with PyYAML --with "dbt-core==1.12.5" --with "dbt-adapters==1.24.6" --with "dbt-common==1.38.0" --with "dbt-databricks==1.12.7" --with "dbt-spark==1.10.3" --with "dbt-extractor==0.6.0" --with "dbt-protos==1.0.615" --with "dbt-core-experimental-parser==2.0.11"

bench:
	$(UV_RUN) python bench.py run --sf $(SF) --reps $(REPS)

bench-clean:
	$(UV_RUN) python bench.py clean

.PHONY: bench bench-clean
