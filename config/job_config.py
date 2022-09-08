from dbacademy.dbgems import get_cloud
    
def get_job_param_json(env, solacc_path, job_name, node_type_id, spark_version, spark):
    cloud = get_cloud()
    # This job is not environment specific, so `env` is not used
    num_workers = 8
    job_json = {
        "timeout_seconds": 14400,
        "name": job_name,
        "max_concurrent_runs": 1,
        "tags": {
            "usage": "solacc_testing",
            "group": "FSI"
        },
        "tasks": [
            {
                "job_cluster_key": "aml_cluster",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": f"{solacc_path}/00_aml_context"
                },
                "task_key": "aml_00",
                "description": ""
            },
            {
                "job_cluster_key": "aml_cluster",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": f"{solacc_path}/01_aml_network_analysis"
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
                    "notebook_path": f"{solacc_path}/02_aml_address_verification"
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
                    "notebook_path": f"{solacc_path}/03_aml_entity_resolution"
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
                    "spark_version": spark_version,
                    "spark_conf": {
                        "spark.databricks.delta.formatCheck.enabled": "false"
                        },
                    "num_workers": num_workers,
                    "node_type_id": node_type_id,
                    "custom_tags": {
                        "usage": "solacc_testing"
                    },
                }
            }
        ]
    }
    if get_cloud() == "AWS": 
      job_json["job_clusters"][0]["new_cluster"]["aws_attributes"] = {
                        "ebs_volume_count": 0,
                        "availability": "ON_DEMAND",
                        "instance_profile_arn": "arn:aws:iam::997819012307:instance-profile/shard-demo-s3-access",
                        "first_on_demand": 1
                    }
    if cloud == "MSA": 
      job_json["job_clusters"][0]["new_cluster"]["azure_attributes"] = {
                        "availability": "ON_DEMAND_AZURE",
                        "first_on_demand": 1
                    }
    if cloud == "GCP": 
      job_json["job_clusters"][0]["new_cluster"]["gcp_attributes"] = {
                        "use_preemptible_executors": False
                    }
    return job_json

    