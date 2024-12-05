from databricks_dbt_factory.FileHandler import FileHandler


class DatabricksDbtFactory:
    def __init__(
        self,
        file_handler: FileHandler,
    ):
        self.file_handler = file_handler

    def generate_job_definition(self, manifest_path: str):
        manifest = self.file_handler.read(manifest_path)
        job_definition = self._generate_job_definition(manifest)
        return job_definition

    @staticmethod
    def _generate_job_definition(manifest: dict) -> dict:
        manifest = manifest["metadata"]
        print(manifest)
        job_definition = {
            "name": "example_job",
            "new_cluster": {"spark_version": "7.3.x-scala2.12", "node_type_id": "i3.xlarge", "num_workers": 2},
            "libraries": [
                {"pypi": {"package": "requests"}},
                {"maven": {"coordinates": "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1"}},
            ],
            "spark_conf": {"spark.speculation": "true"},
            "notebook_task": {"notebook_path": "/Users/example@example.com/ExampleNotebook"},
        }
        return job_definition
