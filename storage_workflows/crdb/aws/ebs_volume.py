class EBSVolume:
    def __init__(self, volume_id, size, iops, throughput):
        self.volume_id = volume_id
        self.size = size
        self.iops = iops
        self.throughput = throughput

    @staticmethod
    def from_aws_response(response_item):
        return EBSVolume(
            volume_id=response_item["VolumeId"],
            size=response_item["Size"],
            iops=response_item.get("Iops", None),  # Some volume types don't have IOPS, like gp2
            throughput=response_item.get("Throughput", None)  # Throughput is only available for gp3 volumes
        )

    def __eq__(self, other):
        if not isinstance(other, EBSVolume):
            return NotImplemented
        return self.size == other.size and self.iops == other.iops and self.throughput == other.throughput

    def __repr__(self):
        return f"EBSVolume(volume_id={self.volume_id}, size={self.size}, iops={self.iops}, throughput={self.throughput})"
