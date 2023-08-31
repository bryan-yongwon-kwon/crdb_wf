from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy import Column, String
from sqlalchemy.orm import declarative_base, mapped_column, Mapped

Base = declarative_base()

class ClustersInfo(Base):
    """The ClusterInfo class corresponds to the "cluster_info" database table
    https://docs.sqlalchemy.org/en/20/orm/mapping_api.html#sqlalchemy.orm.mapped_column.params
    """
    __tablename__ = "clusters_info"

    cluster_name: Mapped[str] = mapped_column(String, primary_key=True)
    deployment_env: Mapped[str] = mapped_column(String, primary_key=True)
    instance_ids: Mapped[list[str]] = mapped_column(ARRAY(String))

    def __repr__(self) -> str:
        return f"ClustersInfo(cluster_name={self.cluster_name!r},deployment_env={self.deployment_env!r}, \
                instance_ids={self.instance_ids!r})"