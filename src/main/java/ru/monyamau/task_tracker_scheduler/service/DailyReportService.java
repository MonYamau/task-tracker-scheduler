package ru.monyamau.task_tracker_scheduler.service;

import feign.FeignException;
import feign.RetryableException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.monyamau.task_tracker_scheduler.client.BackendInternalClient;
import ru.monyamau.task_tracker_scheduler.dto.backend.FullTaskDto;
import ru.monyamau.task_tracker_scheduler.dto.summarization.PersonalTaskCollectionDto;
import ru.monyamau.task_tracker_scheduler.dto.summarization.TaskDto;
import ru.monyamau.task_tracker_scheduler.exception.InternalClientException;
import ru.monyamau.task_tracker_scheduler.exception.NonRetryableKafkaException;
import ru.monyamau.task_tracker_scheduler.exception.UnavailableInternalRequestException;
import ru.monyamau.task_tracker_scheduler.infrastructure.TaskReportRequestProducer;

import java.time.Clock;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

@Slf4j
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
        if (tasks.isEmpty()) {
            log.info("Не удалось найти задачи за предыдущий день");
            return;
        }
        List<String> ownerEmails = extractDistinctOwnerEmails(tasks);
        log.info("Успешно получены задачи для {} пользователей в количестве {} штук", ownerEmails.size(), tasks.size());
        for (String ownerEmail : ownerEmails) {
            publishTaskWithRequestForReport(ownerEmail, tasks);
        }
    }

    private List<FullTaskDto> fetchTasksForPreviousDay() {
        LocalDate today = LocalDate.now(Clock.systemUTC());
        OffsetDateTime toTime = OffsetDateTime.of(
                today.getYear(), today.getMonthValue(), today.getDayOfMonth(), 0, 0, 0, 0, ZoneOffset.UTC);
        OffsetDateTime fromTime = toTime.minusDays(10);
        try {
            return backendInternalClient.findPeriodTasksBy(fromTime, toTime);
        } catch (RetryableException e) {
            throw new UnavailableInternalRequestException("LLM недоступна для сетевых запросов", e);
        } catch (FeignException e) {
            throw new InternalClientException("Возникла ошибка при попытке обращения к backend API", e);
        }
    }

    private List<String> extractDistinctOwnerEmails(List<FullTaskDto> taskDtoList) {
        return taskDtoList.stream()
                .map(FullTaskDto::ownerEmail)
                .distinct()
                .toList();
    }

    private void publishTaskWithRequestForReport(String email, List<FullTaskDto> tasks) {
        List<TaskDto> ownerTasks = findAndConvertTasksByOwnerEmail(tasks, email);
        try {
            reportRequestProducer.publishReportRequest(new PersonalTaskCollectionDto(email, ownerTasks));
            log.info("Успешно опубликована задача с запросом на генерацию отчёта для пользователя {}", email);
        } catch (NonRetryableKafkaException e) {
            log.error("Не удалось опубликовать задачу на генерацию отчёта для пользователя {}", email, e);
        }
    }

    private List<TaskDto> findAndConvertTasksByOwnerEmail(List<FullTaskDto> taskDtoList, String ownerEmail) {
        return taskDtoList.stream()
                .filter(taskDto -> taskDto.ownerEmail().equals(ownerEmail))
                .map(this::convert)
                .toList();
    }

    private TaskDto convert(FullTaskDto fullTaskDto) {
        return new TaskDto(fullTaskDto.title(), fullTaskDto.text(), fullTaskDto.isReady());
    }
}