package de.otto.capella

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk

object OpenTelemetrySetup:
    def apply(): OpenTelemetry = AutoConfiguredOpenTelemetrySdk.initialize().getOpenTelemetrySdk()
