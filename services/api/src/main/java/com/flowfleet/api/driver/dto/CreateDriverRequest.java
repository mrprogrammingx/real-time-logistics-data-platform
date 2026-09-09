package com.flowfleet.api.driver.dto;

import jakarta.validation.constraints.NotBlank;

public record CreateDriverRequest(
        @NotBlank String name,
        Long vehicleId) {
}
