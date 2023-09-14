from sqlalchemy import Column, String, TIMESTAMP, PrimaryKeyConstraint, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from sqlalchemy.orm import mapped_column

Base = declarative_base()


class ClusterHealthCheck(Base):
    __tablename__ = 'cluster_health_check'

    cluster_name = mapped_column(String, primary_key=True, autoincrement=False)
    deployment_env = mapped_column(String, primary_key=True, autoincrement=False)
    region = mapped_column(String, primary_key=True, autoincrement=False)
    aws_account_name = mapped_column(String, primary_key=True, autoincrement=False)
    check_type = mapped_column(String, primary_key=True, autoincrement=False)
    workflow_id = mapped_column(String, primary_key=True, autoincrement=False)
    exec_time = mapped_column(TIMESTAMP, default=func.now())
    check_result = mapped_column(String)
    check_output = mapped_column(JSONB)

    # Setting primary key constraint
    __table_args__ = (
        PrimaryKeyConstraint('cluster_name', 'deployment_env', 'region', 'aws_account_name', 'check_type',
                             'workflow_id'),
    )

    def __repr__(self):
        return (f"<ClusterHealthCheck(cluster_name='{self.cluster_name}', deployment_env='{self.deployment_env}', "
                f"region='{self.region}', aws_account_name='{self.aws_account_name}', workf"
                f"low_id='{self.workflow_id}', exec_time='{self.exec_time}', check_type='{self.check_type}', "
                f"check_result='{self.check_result}', check_output='{self.check_output}')>")

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class ClusterHealthCheckWorkflowState(Base):
    __tablename__ = 'cluster_health_check_workflow_state'

    cluster_name = mapped_column(String, primary_key=True, autoincrement=False)
    deployment_env = mapped_column(String, primary_key=True, autoincrement=False)
    region = mapped_column(String, primary_key=True, autoincrement=False)
    aws_account_name = mapped_column(String, primary_key=True, autoincrement=False)
    check_type = mapped_column(String, primary_key=True, autoincrement=False)
    workflow_id = mapped_column(String, primary_key=True, autoincrement=False)
    exec_time = mapped_column(TIMESTAMP, default=func.now())
    status = Column(String, nullable=False)  # 'Success', 'Failure', 'Failed', 'InProgress'
    retry_count = Column(Integer, default=0)  # Count of retry attempts for the current step

    # Setting primary key constraint
    __table_args__ = (
        PrimaryKeyConstraint('cluster_name', 'deployment_env', 'region', 'aws_account_name', 'check_type',
                             'workflow_id'),
    )

    def __repr__(self):
        return (f"<ClusterHealthCheck(cluster_name='{self.cluster_name}', deployment_env='{self.deployment_env}', "
                f"region='{self.region}', aws_account_name='{self.aws_account_name}', workf"
                f"low_id='{self.workflow_id}', exec_time='{self.exec_time}', check_type='{self.check_type}', "
                f"status='{self.status}', retry_count='{self.retry_count}')>")

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)