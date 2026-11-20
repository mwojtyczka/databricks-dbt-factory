# One command runs the whole benchmark; `make bench-clean` removes the bundle and arm/raw schemas (not results).
SF ?= 0.001
REPS ?= 1
UV_RUN = uv run --python 3.12 --with-editable ../../ --with databricks-sdk --with PyYAML --with "dbt-core==1.12.5" --with "dbt-adapters==1.24.5" --with "dbt-common==1.38.0" --with "dbt-databricks==1.12.7" --with "dbt-spark==1.10.3" --with "dbt-extractor==0.6.0" --with "dbt-protos==1.0.615" --with "dbt-core-experimental-parser==2.0.11"

bench:
	$(UV_RUN) python bench.py run --sf $(SF) --reps $(REPS)

bench-clean:
	$(UV_RUN) python bench.py clean

# One-time (and after changing bundle/dbt_base_environment.yml): prebuild the dbt environment every job uses.
# Needs workspace admin. Kept by bench-clean; delete with
# `databricks environments delete-workspace-base-environment workspace-base-environments/jaffle-bench-dbt`.
BASE_ENV_ID = jaffle-bench-dbt
DBX = databricks --profile fevm-test-jg
ENV_DIR = /Workspace/Users/$(shell $(DBX) current-user me -o json | python3 -c 'import json,sys; print(json.load(sys.stdin)["userName"])')/.jaffle_bench
bench-env:
	$(DBX) workspace mkdirs $(ENV_DIR)
	$(DBX) workspace import $(ENV_DIR)/dbt_base_environment.yml --file bundle/dbt_base_environment.yml --format AUTO --overwrite
	$(DBX) environments get-workspace-base-environment workspace-base-environments/$(BASE_ENV_ID) >/dev/null 2>&1 \
	  && $(DBX) environments refresh-workspace-base-environment workspace-base-environments/$(BASE_ENV_ID) \
	  || $(DBX) environments create-workspace-base-environment "jaffle-bench dbt 1.12" \
	       --workspace-base-environment-id $(BASE_ENV_ID) --filepath $(ENV_DIR)/dbt_base_environment.yml

.PHONY: bench bench-clean bench-env
