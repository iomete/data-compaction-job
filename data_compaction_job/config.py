from dataclasses import dataclass
from typing import List

from pyhocon import ConfigFactory

@dataclass
class ExpireSnapshotConfig:
    enabled: bool = True
    retain_last: int = 1

@dataclass
class RemoveOrphanFilesConfig:
    enabled: bool = True
    older_than_days: int = 3

@dataclass
class RewriteDataFilesConfig:
    options: dict[str, str]
    enabled: bool = True
    strategy: str = "binpack"
    sort_order: str = ""

@dataclass
class RewriteManifestsConfig:
    enabled: bool = True
    use_caching: bool = True

@dataclass
class ApplicationConfig:
    expire_snapshot: ExpireSnapshotConfig
    remove_orphan_files: RemoveOrphanFilesConfig
    rewrite_data_files: RewriteDataFilesConfig
    rewrite_manifests: RewriteManifestsConfig


def get_config(application_config_path) -> ApplicationConfig:
    config = ConfigFactory.parse_file(application_config_path)

    app_conf = ApplicationConfig(
        expire_snapshot=ExpireSnapshotConfig(
            enabled=config["expire_snapshot"].get("enabled", True),
            retain_last=config["expire_snapshot"].get("retain_last", 1)
        ) if config.get("expire_snapshot", None) is not None  else ExpireSnapshotConfig(),

        remove_orphan_files=RemoveOrphanFilesConfig(
            enabled=config["remove_orphan_files"].get("enabled", True),
            older_than_days=config["remove_orphan_files"].get("older_than_days", 3)
        ) if config.get("remove_orphan_files", None) is not None else RemoveOrphanFilesConfig(),

        rewrite_data_files=RewriteDataFilesConfig(
            enabled=config["rewrite_data_files"].get("enabled", True),
            strategy=config["rewrite_data_files"].get("strategy", "binpack"),
            sort_order=config["rewrite_data_files"].get("sort_order", ""),
            options=dict(config["rewrite_data_files"].get("options", {}))
        ) if config.get("rewrite_data_files", None) is not None else RewriteDataFilesConfig(options={}),

        rewrite_manifests=RewriteManifestsConfig(
            enabled=config["rewrite_manifests"].get("enabled", True),
            use_caching=config["rewrite_manifests"].get("use_caching", True)
        ) if config.get("rewrite_manifests", None) is not None else RewriteManifestsConfig()
    )

    print(app_conf)
    return app_conf