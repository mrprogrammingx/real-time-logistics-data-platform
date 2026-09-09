package com.flowfleet.api.driver;

import com.flowfleet.api.driver.dto.CreateDriverRequest;
import com.flowfleet.api.driver.dto.DriverResponse;
import com.flowfleet.api.web.NotFoundException;
import com.flowfleet.common.domain.DriverStatus;
import java.time.Clock;
import java.util.List;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
class DriverService {

    private final DriverRepository drivers;
    private final Clock clock;

    DriverService(DriverRepository drivers, Clock clock) {
        this.drivers = drivers;
        this.clock = clock;
    }

    @Transactional
    DriverResponse create(CreateDriverRequest request) {
        DriverRow saved = drivers.save(DriverRow.newDriver(
                request.name(), DriverStatus.OFFLINE.name(), request.vehicleId(), clock.instant()));
        return toResponse(saved);
    }

    @Transactional(readOnly = true)
    DriverResponse get(long id) {
        return drivers.findById(id).map(DriverService::toResponse)
                .orElseThrow(() -> new NotFoundException("driver", id));
    }

    @Transactional(readOnly = true)
    List<DriverResponse> list(DriverStatus status) {
        List<DriverRow> rows = status == null ? drivers.findAll() : drivers.findByStatus(status.name());
        return rows.stream().map(DriverService::toResponse).toList();
    }

    @Transactional
    DriverResponse changeStatus(long id, DriverStatus target) {
        DriverRow row = drivers.findById(id).orElseThrow(() -> new NotFoundException("driver", id));
        if (DriverStatus.valueOf(row.status()) == target) {
            return toResponse(row);
        }
        return toResponse(drivers.save(row.withStatus(target.name(), clock.instant())));
    }

    private static DriverResponse toResponse(DriverRow row) {
        return new DriverResponse(row.id(), row.name(), row.status(), row.vehicleId(),
                row.version(), row.createdAt(), row.updatedAt());
    }
}
