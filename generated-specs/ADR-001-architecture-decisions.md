# ADR 001: Architecture Decisions for Orquestación de Saga Idempotente con Goroutines en Go 1.22 y chi

## Status
Accepted

## Context
Project requiring structured implementation matching Gherkin specification.

## Decisions
- **Architecture Style**: Monolith Architecture (MVC / Monolithic) (monolith)
- **Primary Backend Language**: go
- **Backend Framework**: chi (Go 1.22+)
- **ORM / Persistence**: go-orm (gorm.io/gorm v1.25.7)
- **Validation**: go-validator (github.com/go-playground/validator/v10)
- **Authentication**: jwt-bcrypt (bcrypt cost factor 12, JWT TTL 3600s)
- **Backend Testing Framework**: testing (testing (standard library))
- **Frontend Framework**: react
- **Frontend Language**: javascript
- **Frontend Bundler**: vite
- **Frontend Unit Testing**: vitest
- **Frontend E2E Testing**: cypress

## Prohibited Layer Dependencies
Domain core must NOT import:
- `direct SQL string interpolation`
- `global state mutation`
