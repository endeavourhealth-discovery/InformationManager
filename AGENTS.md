# Repository Guidelines

## Project Structure & Module Organization
The Information Manager (IM2) is a multi-module Java/Kotlin project focused on loading and transforming external medical data into an IM2 graph database.
- **Feeds**: Handles data ingestion from external sources like TRUD.
- **Transforms**: Core logic for data transformation and bulk preloading into GraphDB.
- **Utils**: Shared utility modules, including OpenSearch/Elasticsearch term updates.
- **Plugins**: Gradle plugins for generating static constants and handling TypeScript enums.
- **V1-V2Migration**: Tools for migrating data between IM versions.

## Build, Test, and Development Commands
The project uses Gradle with Kotlin DSL.
- **Build**: `./gradlew build`
- **Test all**: `./gradlew test`
- **Test single module**: `./gradlew :<module-name>:test`
- **Run TRUD Updater**: `org.endeavourhealth.informationmanager.trud` (requires API key)
- **Run Preload**: `org.endeavourhealth.informationmanager.transforms.preload` (requires source, preload, temp, and cmd arguments)
- **Sonar Analysis**: `./gradlew safeSonar` (requires `SONAR_LOGIN` environment variable)

## Coding Style & Naming Conventions
- **Language**: Java 21 (Source/Target compatibility).
- **Style**: Enforced via `.editorconfig`.
- **Naming**: Follows standard Java/Kotlin conventions (PascalCase for classes, camelCase for methods/variables).
- **Linter/Quality**: SonarCloud is used for continuous quality inspection.

## Testing Guidelines
- **Framework**: JUnit 5 (Jupiter).
- **Mocking**: Mockito.
- **BDD**: Cucumber for Java.
- **Coverage**: JaCoCo is applied to all subprojects. Report generated at `build/reports/jacoco/test/jacocoTestReport.xml`.
- **Note**: Tests run with `-XX:+EnableDynamicAgentLoading` to support Mockito/ByteBuddy on Java 21.

## Commit & Pull Request Guidelines
- **Commit Messages**: Follow a structured format: `<type>(<scope>): <description>`.
- **Common Types**: `refactor`, `fix`, `feat`.
- **Merge Pattern**: Uses pull requests with standard "Merge pull request #..." messages.
