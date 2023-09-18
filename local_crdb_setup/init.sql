CREATE TABLE cluster_health_check(
   cluster_name STRING,
   deployment_env STRING,
   region STRING,
   aws_account_name STRING,
   workflow_id STRING,
   exec_time TIMESTAMP,
   check_type STRING,
   check_result STRING,
   check_output JSONB,
   PRIMARY KEY(cluster_name, deployment_env, region, aws_account_name, check_type, workflow_id)
);

CREATE TABLE cluster_health_check_workflow_state(
   cluster_name STRING,
   deployment_env STRING,
   region STRING,
   aws_account_name STRING,
   workflow_id STRING,
   exec_time TIMESTAMP,
   check_type STRING,
   status STRING,
   retry_count INTEGER,
   PRIMARY KEY(cluster_name, deployment_env, region, aws_account_name, check_type, workflow_id)
);
