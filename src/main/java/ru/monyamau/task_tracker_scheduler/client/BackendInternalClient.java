package ru.monyamau.task_tracker_scheduler.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import ru.monyamau.task_tracker_scheduler.dto.backend.FullTaskDto;

import java.time.OffsetDateTime;
import java.util.List;

@FeignClient(
        name = "backendInternalClient",
        url = "${backend-service.url}"
)
public interface BackendInternalClient {
    @GetMapping("/period-tasks")
    List<FullTaskDto> findPeriodTasksBy(
            @RequestParam("from")
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
            OffsetDateTime from,
            @RequestParam("to")
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
            OffsetDateTime to);
}