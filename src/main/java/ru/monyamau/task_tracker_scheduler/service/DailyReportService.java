package ru.monyamau.task_tracker_scheduler.service;

import feign.FeignException;
import org.springframework.stereotype.Service;
import ru.monyamau.task_tracker_scheduler.client.BackendInternalClient;
import ru.monyamau.task_tracker_scheduler.dto.backend.FullTaskDto;
import ru.monyamau.task_tracker_scheduler.dto.summarization.PersonalTaskCollectionDto;
import ru.monyamau.task_tracker_scheduler.dto.summarization.TaskDto;
import ru.monyamau.task_tracker_scheduler.exception.InternalClientException;
import ru.monyamau.task_tracker_scheduler.infrastructure.TaskReportRequestProducer;

import java.time.Clock;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

@Service
public class DailyReportService {
    private final BackendInternalClient backendInternalClient;
    private final TaskReportRequestProducer reportRequestProducer;

    public DailyReportService(BackendInternalClient backendInternalClient, TaskReportRequestProducer reportRequestProducer) {
        this.backendInternalClient = backendInternalClient;
        this.reportRequestProducer = reportRequestProducer;
    }

    public void sendReportRequests() {
        List<FullTaskDto> tasks = fetchTasksForPreviousDay();
        if (tasks.isEmpty()) return;
        List<String> ownerEmails = extractDistinctOwnerEmails(tasks);
        for (String ownerEmail : ownerEmails) {
            List<TaskDto> ownerTasks = findTasksByOwnerEmail(tasks, ownerEmail);
            reportRequestProducer.publishReportRequest(new PersonalTaskCollectionDto(ownerEmail, ownerTasks));
        }
    }

    private List<FullTaskDto> fetchTasksForPreviousDay() {
        LocalDate today = LocalDate.now(Clock.systemUTC());
        OffsetDateTime toTime = OffsetDateTime.of(today.getYear(), today.getMonthValue(), today.getDayOfMonth(), 0, 0, 0, 0, ZoneOffset.UTC);
        OffsetDateTime fromTime = toTime.minusDays(1);
        try {
            return backendInternalClient.findPeriodTasksBy(fromTime, toTime);
        } catch (FeignException e) {
            throw new InternalClientException("Возникла ошибка при попытке обращения к внутреннему backend API", e);
        }
    }

    private List<String> extractDistinctOwnerEmails(List<FullTaskDto> taskDtoList) {
        return taskDtoList.stream()
                .map(FullTaskDto::ownerEmail)
                .distinct()
                .toList();
    }

    private List<TaskDto> findTasksByOwnerEmail(List<FullTaskDto> taskDtoList, String ownerEmail) {
        return taskDtoList.stream()
                .filter(taskDto -> taskDto.ownerEmail().equals(ownerEmail))
                .map(this::convert)
                .toList();
    }

    private TaskDto convert(FullTaskDto fullTaskDto) {
        return new TaskDto(fullTaskDto.title(), fullTaskDto.text(), fullTaskDto.isReady());
    }
}