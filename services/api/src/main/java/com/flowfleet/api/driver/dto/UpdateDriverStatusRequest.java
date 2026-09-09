package com.flowfleet.api.driver.dto;

import com.flowfleet.common.domain.DriverStatus;
import jakarta.validation.constraints.NotNull;

public record UpdateDriverStatusRequest(@NotNull DriverStatus status) {
}
