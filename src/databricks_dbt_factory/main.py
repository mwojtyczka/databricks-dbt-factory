import argparse
from databricks_dbt_factory.DatabricksDbtFactory import DatabricksDbtFactory
from databricks_dbt_factory.FileHandler import FileHandler


def main():
    parser = argparse.ArgumentParser(description="Generate Databricks job definition from dbt manifest.")
    parser.add_argument("--manifest-path", type=str, help="Path to the manifest file", required=True)
    args = parser.parse_args()

    factory = DatabricksDbtFactory(FileHandler())
    job_definition = factory.generate_job_definition(args.manifest_path)
    print(job_definition)


if __name__ == "__main__":
    main()
