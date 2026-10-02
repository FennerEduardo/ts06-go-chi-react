# language: es
Característica: Orquestación de Saga Idempotente con Goroutines en Go 1.22 y chi

  Escenario: Ejecución de Saga con Context Timeout y Publicación de Evento Outbox
    Dado un comando `CreateOrderCommand` enviado a `/api/v1/orders` con `idempotency_key` "IDEM-GO-122"
    Cuando la ruta `chi` procesa la petición dentro de un `context.WithTimeout` de 5 segundos
    Entonces debe abrir una transacción `pgx.Tx` y registrar el agregado y el registro Outbox
    Y debe devolver HTTP 201 Created con el payload JSON ideomático en Go
    Y la suite `go test -race ./...` debe verificar la ausencia de condiciones de carrera (data races)
