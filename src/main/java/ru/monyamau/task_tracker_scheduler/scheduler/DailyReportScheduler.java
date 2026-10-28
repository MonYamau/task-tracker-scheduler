package ru.monyamau.task_tracker_scheduler.scheduler;

import org.springframework.scheduling.annotation.Scheduled;

public class DailyReportScheduler {

    @Scheduled(
            cron = "${scheduler.cron_value}",
            zone = "UTC")
    public void publishDailyReports() {
    }
}