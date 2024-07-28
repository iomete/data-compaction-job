from dataclasses import dataclass
from typing import Any
from pyhocon import ConfigFactory


@dataclass
class ExpireSnapshotConfig:
    retain_last: int = 1


@dataclass
class RemoveOrphanFilesConfig:
    older_than_days: int = None


@dataclass
class RewriteDataFilesConfig:
    strategy: str = None
    sort_order: str = None
    options: dict[str, Any] = None


@dataclass
class RewriteManifestsConfig:
    use_caching: bool = None


@dataclass
class ApplicationConfig:
    expire_snapshot: ExpireSnapshotConfig = ExpireSnapshotConfig()
    remove_orphan_files: RemoveOrphanFilesConfig = RemoveOrphanFilesConfig()
    rewrite_data_files: RewriteDataFilesConfig = RewriteDataFilesConfig()
    rewrite_manifests: RewriteManifestsConfig = RewriteManifestsConfig()
    parallelism: int = 4


def get_config(application_config_path) -> ApplicationConfig:
    config = ConfigFactory.parse_file(filename=application_config_path, required=False)

    app_config = ApplicationConfig()

    if "expire_snapshot" in config:
        app_config.expire_snapshot=ExpireSnapshotConfig(
            retain_last=config["expire_snapshot"].get("retain_last", 1)
        )

    if "rewrite_data_files" in config:
        app_config.rewrite_data_files=RewriteDataFilesConfig(
            options=dict(config["rewrite_data_files"].get("options", {}))
        )

    if "rewrite_manifests" in config:
        app_config.rewrite_manifests=RewriteManifestsConfig(
            use_caching=config["rewrite_manifests"].get("use_caching", None)
        )

    if "parallelism" in config:
        app_config.parallelism = config.get("parallelism", 4)

    return app_config
