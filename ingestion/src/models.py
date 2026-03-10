from dataclasses import asdict, dataclass
from typing import Optional


@dataclass(frozen=True)
class SourceFile:
    source_url: str
    file_name: str
    content_bytes: bytes
    local_path: Optional[str] = None


@dataclass(frozen=True)
class PipelineRun:
    run_id: str
    source_name: str
    entity_name: str
    status: str
    row_count: Optional[int]
    checksum: Optional[str]
    started_at: str
    completed_at: Optional[str]
    error_message: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class FileManifest:
    run_id: str
    bucket_name: str
    object_key: str
    file_name: str
    source_url: str
    checksum: str
    byte_size: int
    row_count: int

    def to_dict(self) -> dict:
        return asdict(self)
