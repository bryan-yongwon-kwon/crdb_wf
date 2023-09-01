from sqlalchemy import Column, String, TIMESTAMP, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func

Base = declarative_base()


class ClusterHealthCheck(Base):
    __tablename__ = 'cluster_health_check'

    cluster_name = Column(String, nullable=False)
    deployment_env = Column(String, nullable=False)
    region = Column(String, nullable=False)
    aws_account_name = Column(String, nullable=False)
    workflow_id = Column(String, nullable=False)
    exec_time = Column(TIMESTAMP, default=func.now())
    check_type = Column(String)
    check_result = Column(String)
    check_output = Column(JSONB)

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
