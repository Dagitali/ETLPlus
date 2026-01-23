# `etlplus.workflow` Subpackage

Documentation for the `etlplus.workflow` subpackage: configuration helpers for connectors,
pipelines, jobs, and profiles.

- Provides classes and utilities for managing ETL pipeline configuration
- Supports YAML/JSON config loading and validation
- Includes helpers for connectors, jobs, pipelines, and profiles
- Exposes type definitions for config schemas

Back to project overview: see the top-level [README](../../README.md).

- [`etlplus.workflow` Subpackage](#etlplusworkflow-subpackage)
  - [Supported Configuration Types](#supported-configuration-types)
  - [Loading and Validating Configs](#loading-and-validating-configs)
  - [Example: Loading a Pipeline Config](#example-loading-a-pipeline-config)
  - [See Also](#see-also)

## Supported Configuration Types

- **Connector**: Connection details for databases, files, or APIs
- **Job**: ETL job definitions and scheduling
- **Pipeline**: End-to-end pipeline configuration
- **Profile**: User or environment-specific settings

## Loading and Validating Configs

Use the provided classes to load and validate configuration files:

```python
from etlplus.workflow import PipelineConfig

cfg = PipelineConfig.from_yaml("pipeline.yml")
```

- Supports YAML and JSON formats
- Validates against expected schema

## Example: Loading a Pipeline Config

```python
from etlplus.workflow import PipelineConfig

pipeline = PipelineConfig.from_yaml("configs/pipeline.yml")
print(pipeline)
```

## See Also

- Top-level CLI and library usage in the main [README](../../README.md)
- Config type definitions in [types.py](types.py)
- Config utilities in [utils.py](utils.py)
