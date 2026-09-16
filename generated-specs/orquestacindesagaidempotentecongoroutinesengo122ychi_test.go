// Godog Step Definitions for Orquestación de Saga Idempotente con Goroutines en Go 1.22 y chi
package orquestacindesagaidempotentecongoroutinesengo122ychi

import (
    "context"
    "testing"
    "github.com/cucumber/godog"
)


// Scenario: Ejecución de Saga con Context Timeout y Publicación de Evento Outbox

func uncomandocreateordercommandenviadoaapiv1ordersconidempotencykeyidemgo122(ctx context.Context) error {
    return godog.ErrPending
}

func larutachiprocesalapeticindentrodeuncontextwithtimeoutde5segundos(ctx context.Context) error {
    return godog.ErrPending
}

func debeabrirunatransaccinpgxtxyregistrarelagregadoyelregistrooutbox(ctx context.Context) error {
    return godog.ErrPending
}



func InitializeScenario(ctx *godog.ScenarioContext) {

    // Ejecución de Saga con Context Timeout y Publicación de Evento Outbox
    ctx.Step(`^un comando `CreateOrderCommand` enviado a `/api/v1/orders` con `idempotency_key` \"IDEM-GO-122\"$`, uncomandocreateordercommandenviadoaapiv1ordersconidempotencykeyidemgo122)
    ctx.Step(`^la ruta `chi` procesa la petición dentro de un `context.WithTimeout` de 5 segundos$`, larutachiprocesalapeticindentrodeuncontextwithtimeoutde5segundos)
    ctx.Step(`^debe abrir una transacción `pgx.Tx` y registrar el agregado y el registro Outbox$`, debeabrirunatransaccinpgxtxyregistrarelagregadoyelregistrooutbox)

}

func TestFeatures(t *testing.T) {
    suite := godog.TestSuite{
        ScenarioInitializer: InitializeScenario,
        Options: &godog.Options{
            Format:   "pretty",
            Paths:    []string{"features"},
            TestingT: t,
        },
    }

    if suite.Run() != 0 {
        t.Fatal("non-zero status returned, failed to run feature tests")
    }
}
