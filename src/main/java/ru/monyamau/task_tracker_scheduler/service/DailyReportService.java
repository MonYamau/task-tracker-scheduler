package ru.monyamau.task_tracker_scheduler.service;

import feign.FeignException;
import feign.RetryableException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.monyamau.task_tracker_scheduler.client.BackendInternalClient;
import ru.monyamau.task_tracker_scheduler.dto.backend.FullTaskDto;
import ru.monyamau.task_tracker_scheduler.dto.summarization.PersonalTaskCollectionDto;
import ru.monyamau.task_tracker_scheduler.dto.summarization.PersonalTaskReportDto;
import ru.monyamau.task_tracker_scheduler.dto.summarization.TaskDto;
import ru.monyamau.task_tracker_scheduler.exception.InternalClientException;
import ru.monyamau.task_tracker_scheduler.exception.NonRetryableKafkaException;
import ru.monyamau.task_tracker_scheduler.exception.RetryableKafkaException;
import ru.monyamau.task_tracker_scheduler.exception.UnavailableInternalRequestException;
import ru.monyamau.task_tracker_scheduler.infrastructure.TaskReportRequestProducer;

import java.time.Clock;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

@Slf4j
@Service
public class DailyReportService {
    private final BackendInternalClient backendInternalClient;
    private final TaskReportRequestProducer reportRequestProducer;
    private final ReportMailingService reportMailingService;
    private final int windowDaysAgo;

    public DailyReportService(BackendInternalClient backendInternalClient,
                              TaskReportRequestProducer reportRequestProducer,
                              ReportMailingService reportMailingService,
                              @Value("${scheduler.window-days-ago-value}") int windowDaysAgo) {
        this.backendInternalClient = backendInternalClient;
        this.reportRequestProducer = reportRequestProducer;
        this.reportMailingService = reportMailingService;
        this.windowDaysAgo = windowDaysAgo;
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
            Optional<PersonalTaskReportDto> requestPersonalReport = requestPersonalReport(ownerEmail, tasks);
            if (requestPersonalReport.isEmpty()) {
                continue;
            }
            log.info("Успешно получен персональный отчёт для пользователя {}", ownerEmail);
            mailingPersonalReport(ownerEmail, requestPersonalReport.get());
            log.info("Успешно произведена рассылка персонального отчёта для пользователя {}", ownerEmail);
        }
    }

    private List<FullTaskDto> fetchTasksForPreviousDay() {
        OffsetDateTime toTime = LocalDate.now(Clock.systemUTC()).atStartOfDay().atOffset(ZoneOffset.UTC);
        OffsetDateTime fromTime = toTime.minusDays(windowDaysAgo);
        try {
            return backendInternalClient.findPeriodTasksBy(fromTime, toTime);
        } catch (RetryableException e) {
            throw new UnavailableInternalRequestException("Backend API недоступен для сетевых запросов", e);
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

    private Optional<PersonalTaskReportDto> requestPersonalReport(String email, List<FullTaskDto> tasks) {
        List<TaskDto> ownerTasks = findAndConvertTasksByOwnerEmail(tasks, email);
        try {
            return Optional.ofNullable(reportRequestProducer.requestReport(new PersonalTaskCollectionDto(ownerTasks)));
        } catch (RetryableKafkaException e) {
            log.error("Возникла временная ошибка при попытке получить персональный отчёт для пользователя {}", email, e);
        } catch (NonRetryableKafkaException e) {
            log.error("Не удалось получить персональный отчёт из-за ошибки для пользователя {}", email, e);
        }
        return Optional.empty();
    }

    private void mailingPersonalReport(String email, PersonalTaskReportDto personalTaskReportDto) {
        try {
            reportMailingService.sendReportToUser(email, personalTaskReportDto.personalReport());
        } catch (RetryableKafkaException e) {
            log.error("Возникла временная ошибка при попытке отправки письма с отчётом для пользователя {}", email, e);
        } catch (NonRetryableKafkaException e) {
            log.error("Не удалось опубликовать задачу на отправку письма с отчётом для пользователя {}", email, e);
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