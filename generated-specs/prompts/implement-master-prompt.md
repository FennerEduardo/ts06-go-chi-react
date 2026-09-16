# 🚀 AI AGENT MASTER IMPLEMENTATION PROMPT
## Feature: Orquestación de Saga Idempotente con Goroutines en Go 1.22 y chi (Spec Hash: cce68ef8)
## Architecture: MONOLITH | Stack: GO (chi)
## Prompt Version / Audit Hash: prt_447ac973
## Author / Developer: Fenner Eduardo González C. <fennereduardo@gmail.com> (source: git)

### 📌 Context Files to Read & Follow:
- @.ghkgovernance.yaml
- @features/go_chi_saga.feature
- @generated-specs/orquestacindesagaidempotentecongoroutinesengo122ychi.contract.go
- @generated-specs/ADR-001-architecture-decisions.md
- @generated-specs/openapi.json
- @generated-specs/docker-compose.yml

### 🛠️ Technical Guardrails & Stack Specifications:
- **Language**: go (chi)
- **Persistence**: go-orm + mysql
- **Validation**: go-validator
- **Testing Framework**: testing

### 🐳 Docker Execution Sandbox & Host Isolation Guardrails:
> **IMPORTANT**: If your host operating system lacks the native runtime SDK (GO), DO NOT install heavy packages directly on the host machine.
> Execute all compilation, migrations, and test runs inside the isolated Docker container:
> 
> ```bash
> # Start database and infrastructure services
> docker compose up -d
> 
> # Execute test suite inside Docker sandbox container:
> docker compose run --rm app go test ./...
> ```

### 🎯 Mandatory Step-by-Step Implementation Flow:

#### Phase 1: Pure Domain Layer
1. Read the feature specification in `features/go_chi_saga.feature` and contract in `generated-specs/orquestacindesagaidempotentecongoroutinesengo122ychi.contract.go`.
2. Implement pure domain Entities, Value Objects, and Domain Events.
3. Ensure zero dependencies on external frameworks or database drivers in the domain core.

#### Phase 2: Application Use Cases & Infrastructure
1. Implement the Repository Port interface using GO-ORM (mysql).
2. Implement Controllers/Handlers to process HTTP requests and return appropriate status codes (e.g. 201 Created, 400 Bad Request).
3. Apply validation using go-validator.

#### Phase 3: Automated Unit & Feature Testing
1. Implement automated test cases in TESTING matching all scenarios in `features/go_chi_saga.feature`.
2. Assert HTTP response status codes, payload structures, and event emissions.
3. If host environment lacks SDK, run verification inside Docker sandbox (`docker compose run --rm app go test ./...`).
4. Ensure 100% scenario pass rate.
