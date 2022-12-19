# Databricks notebook source
# MAGIC %md This notebook sets up the companion cluster(s) to run the solution accelerator. It also creates the Workflow to create a Workflow DAG and illustrate the order of execution. Feel free to interactively run notebooks with the cluster or to run the Workflow to see how this solution accelerator executes. Happy exploring!
# MAGIC 
# MAGIC The pipelines, workflows and clusters created in this script are not user-specific, so if another user alters the workflow and cluster via the UI, running this script again after the modification resets them.
# MAGIC 
# MAGIC **Note**: If the job execution fails, please confirm that you have set up other environment dependencies as specified in the accelerator notebooks. Accelerators sometimes require the user to set up additional cloud infra or data access, for instance. 

# COMMAND ----------

# DBTITLE 0,Install util packages
# MAGIC %pip install git+https://github.com/databricks-academy/dbacademy@v1.0.13 git+https://github.com/databricks-industry-solutions/notebook-solution-companion@safe-print-html --quiet --disable-pip-version-check

# COMMAND ----------

from solacc.companion import NotebookSolutionCompanion

# COMMAND ----------

job_json = {
        "timeout_seconds": 14400,
        "max_concurrent_runs": 1,
        "tags": {
                "usage": "solacc_automation",
                "group": "FSI"
            },
        "tasks": [
            {
                "job_cluster_key": "aml_cluster",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": f"00_aml_context" # different from standard API
                },
                "task_key": "aml_00",
                "description": ""
            },
            {
                "job_cluster_key": "aml_cluster",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": f"01_aml_network_analysis" # different from standard API
                },
                "task_key": "aml_01",
                "depends_on": [
                    {
                        "task_key": "aml_00"
                    }
                ],
                "description": ""
            },
            {
                "job_cluster_key": "aml_cluster",
                "notebook_task": {
                    "notebook_path": f"02_aml_address_verification" # different from standard API
                },
                "task_key": "aml_02",
                "depends_on": [
                    {
                        "task_key": "aml_01"
                    }
                ]
            },
            {
                "job_cluster_key": "aml_cluster",
                "notebook_task": {
                    "notebook_path": f"03_aml_entity_resolution" # different from standard API
                },
                "task_key": "aml_03",
                "depends_on": [
                    {
                        "task_key": "aml_02"
                    }
                ]
            }
        ],
        "job_clusters": [
            {
                "job_cluster_key": "aml_cluster",
                "new_cluster": {
                    "spark_version": "11.3.x-cpu-ml-scala2.12",
                    "spark_conf": {
                        "spark.databricks.delta.formatCheck.enabled": "false"
                        },
                    "num_workers": 2,
                    "node_type_id": {"AWS": "i3.xlarge", "MSA": "Standard_DS3_v2", "GCP": "n1-highmem-4"}, # different from standard API
                    "custom_tags": {
                        "usage": "solacc_automation",
                        "group": "FSI"
                    },
                }
            }
        ]
    }

# COMMAND ----------

dbutils.widgets.dropdown("run_job", "False", ["True", "False"])
run_job = dbutils.widgets.get("run_job") == "True"
NotebookSolutionCompanion().deploy_compute(job_json, run_job=run_job)

# COMMAND ----------


