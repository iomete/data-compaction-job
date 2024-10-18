from dataclasses import dataclass
from typing import Any
from pyhocon import ConfigFactory


@dataclass
class ExpireSnapshotConfig:
    retain_last: int = 1


@dataclass
class RemoveOrphanFilesConfig:
    older_than_days: int = 1


@dataclass
class RewriteDataFilesConfig:
    strategy: str = None
    sort_order: str = None
    options: dict[str, Any] = None
    where: str = None


@dataclass
class RewriteManifestsConfig:
    use_caching: bool = None

@dataclass
class IncludeExcludeConfig:
    databases: list[str] = None
    table_include: list[str] = None
    table_exclude: list[str] = None

@dataclass
class ApplicationConfig:
    catalog: str = ""
    expire_snapshot: ExpireSnapshotConfig = ExpireSnapshotConfig()
    remove_orphan_files: RemoveOrphanFilesConfig = RemoveOrphanFilesConfig()
    rewrite_data_files: RewriteDataFilesConfig = RewriteDataFilesConfig()
    rewrite_manifests: RewriteManifestsConfig = RewriteManifestsConfig()
    include_exclude: IncludeExcludeConfig = IncludeExcludeConfig()
    parallelism: int = 4
    table_overrides: dict[str, dict] = None


def get_config(application_config_path) -> ApplicationConfig:
    config = ConfigFactory.parse_file(filename=application_config_path, required=False)

    app_config = ApplicationConfig()

    if "catalog" in config:
        app_config.catalog = config["catalog"]
    else:
        raise Exception("Catalog not provided in config. Please provide catalog for which to run optimisation.")

    if "expire_snapshot" in config:
        app_config.expire_snapshot=ExpireSnapshotConfig(
            retain_last=config["expire_snapshot"].get("retain_last", 1)
        )

    if "rewrite_data_files" in config:
        app_config.rewrite_data_files=RewriteDataFilesConfig(
            options=dict(config["rewrite_data_files"].get("options", {})),
            strategy=config["rewrite_data_files"].get("strategy", None),
            sort_order=config["rewrite_data_files"].get("sort_order", None),
            where=config["rewrite_data_files"].get("where", None)
        )

    if "rewrite_manifests" in config:
        app_config.rewrite_manifests=RewriteManifestsConfig(
            use_caching=config["rewrite_manifests"].get("use_caching", None)
        )

    if "parallelism" in config:
        app_config.parallelism = config.get("parallelism", 4)

    if "databases" in config:
        app_config.include_exclude.databases = config.get("databases", [])

    if "table_include" in config:
        app_config.include_exclude.table_include = config.get("table_include")
        app_config.include_exclude.table_exclude = []
    elif "table_exclude" in config:
        app_config.include_exclude.table_exclude = config.get("table_exclude")
        app_config.include_exclude.table_include = []
    else:
        app_config.include_exclude.table_exclude = []
        app_config.include_exclude.table_include = []

    if "table_overrides" in config:
        app_config.table_overrides = config["table_overrides"]

    return app_config
