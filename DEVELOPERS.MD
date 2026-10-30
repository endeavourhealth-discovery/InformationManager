# Developer Structural Overview

This document provides a high-level overview of the Information Manager (IM2) codebase structure and module responsibilities.

## Module Architecture

The project is organized into several Gradle subprojects, each handling a specific part of the data ingestion and transformation pipeline.

### Core Modules

- **[./Transforms](./Transforms)**: The central module for data transformation.
  - `org.endeavourhealth.informationmanager.transforms.sources`: Contains specialized importers for various data formats (SNOMED, EMIS, TPP, ICD10, OPCS4, etc.).
  - `org.endeavourhealth.informationmanager.transforms.preload`: Contains the `Preload` entry point for bulk loading data into GraphDB.
  - `org.endeavourhealth.informationmanager.transforms.online`: Contains `ImportApp` for online (transactional) data imports.
- **[./Feeds](./Feeds)**: Handles fetching and updating external data feeds.
  - `org.endeavourhealth.informationmanager.trud`: Specifically handles TRUD (Terminology Repository and Update Distribution) updates from NHS Digital.
- **[./Utils](./Utils)**: Common utility tools.
  - `org.endeavourhealth.informationmanager.utils.opensearch`: Tools for indexing terminology into OpenSearch/Elasticsearch for fast searching.
  - `org.endeavourhealth.informationmanager.utils.codegen`: Utility for generating code based on the ontology.

### Specialized Tools

- **[./QOFExtractor](./QOFExtractor)**: A dedicated tool for extracting Quality and Outcomes Framework (QOF) definitions.
- **[./V1-V2Migration](./V1-V2Migration)**: Tools and scripts for migrating data between IMv1 and IMv2 formats.
- **[./Plugins](./Plugins)**: Custom Gradle plugins used during the build process.
  - `StaticConstGenerator`: Generates Java static constants from the ontology vocabulary.
  - `TypescriptConstEnumToEnum`: Helps in bridging constants to TypeScript.

## Key Components & Patterns

### Importer Pattern
The `Importer` class in `Transforms` acts as a factory/manager for specialized importers. To add a new data source:
1. Implement the `TTImport` interface.
2. Register the new importer in the `Importer.getImporter(ImportType)` switch statement.

### Data Flow
1. **Fetch**: `TrudUpdater` downloads and unzips raw terminology files.
2. **Transform**: Specialized importers (e.g., `SnomedImporter`) read raw files and convert them into TripleTree (`TT`) objects.
3. **File/Load**: `TTBulkFiler` or `TTTransactionFiler` (from `imapi`) handles the actual persistence into the graph database.

## Development Workflow

- **Local Setup**: Requires Java 21 and a running instance of GraphDB for testing transforms.
- **Ontology Updates**: When the core ontology changes, use the `StaticConstGenerator` plugin to refresh the Java vocabulary constants.
- **Indexing**: After a large bulk load, run the `OpenSearchSender` to ensure the search index is in sync with the graph data.

## Build System
The project uses Gradle with Kotlin DSL. Module dependencies and versions are managed in [./gradle/libs.versions.toml](./gradle/libs.versions.toml).
