package ru.monyamau.task_tracker_scheduler.scheduler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ru.monyamau.task_tracker_scheduler.service.DailyReportService;

import java.time.Instant;

@Slf4j
@Component
public class DailyReportScheduler {
    private final DailyReportService dailyReportService;

    public DailyReportScheduler(DailyReportService dailyReportService) {
        this.dailyReportService = dailyReportService;
    }

    @Scheduled(
            cron = "${scheduler.cron_value}",
            zone = "UTC")
    public void publishDailyReports() {
        log.info("Запущен процесс публикации отчётов пользователей в {}", Instant.now());
        dailyReportService.sendReportRequests();
        log.info("Процесс публикации отчётов пользователей окончен в {}", Instant.now());
    }
}