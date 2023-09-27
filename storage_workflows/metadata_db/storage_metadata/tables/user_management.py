from sqlalchemy import create_engine, Column, String, TIMESTAMP, Enum, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from sqlalchemy.orm import mapped_column


# Create a base class using declarative_base
Base = declarative_base()

# Define an enumeration for the 'deployment_env' column
class DeploymentEnv(Enum):
    staging = 'staging'
    prod = 'prod'

# Define the UserManagement class to represent the 'user_management' table
class UserManagement(Base):
    __tablename__ = 'user_management'

    cluster_name = mapped_column(String, primary_key=True)
    deployment_env = mapped_column(String, primary_key=True, autoincrement=False)
    region = mapped_column(String, primary_key=True)
    aws_account = mapped_column(String, primary_key=True)
    database_name = mapped_column(String)
    role_name = mapped_column(String, primary_key=True)
    certificate_path = mapped_column(String)
    created_at = mapped_column(TIMESTAMP, default=func.now())
    updated_at = mapped_column(TIMESTAMP, default=func.now())

    # Define the primary key constraint
    __table_args__ = (
        PrimaryKeyConstraint('cluster_name', 'deployment_env', 'region', 'aws_account', 'role_name'),
    )

    def __repr__(self):
        return (f"<UserManagement(cluster_name='{self.cluster_name}', deployment_env='{self.deployment_env}', "
                f"region='{self.region}', aws_account_name='{self.aws_account_name}', database_name"
                f"='{self.database_name}', role_name='{self.role_name}', certificate_path='{self.certificate_path}', "
                f"created_at='{self.created_at}', updated_at='{self.updated_at}')>")

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
