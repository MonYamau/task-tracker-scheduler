package ru.monyamau.task_tracker_scheduler.scheduler;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ru.monyamau.task_tracker_scheduler.service.DailyReportService;

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
        dailyReportService.sendReportRequests();
    }
}