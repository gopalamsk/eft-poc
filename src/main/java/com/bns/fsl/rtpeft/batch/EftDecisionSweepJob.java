package com.bns.fsl.rtpeft.batch;

import com.bns.fsl.rtpeft.aspect.LoggableMethodExecution;
import com.bns.fsl.rtpeft.service.EftDecisionSweepService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.boot.autoconfigure.batch.BatchDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * The single sweeper job referenced throughout the design review: one
 * Spring Batch tasklet, triggered every 2 seconds, running one query with a
 * single WHERE status = 'PENDING' left-joined against the Fraud Decision
 * Service table and the eft_config cutoff - no separate timeout job, no
 * separate fast/slow cadence. See EftDecisionSweepService for the actual
 * per-row logic; this class is purely the Spring Batch wiring and schedule.
 *
 * Deliberately a Tasklet, not a chunk-oriented step - each run pulls at most
 * SWEEP_BATCH_SIZE rows in one query and processes them procedurally, which
 * fits a repeating poll far better than Spring Batch's read/process/write
 * chunk model built for large, one-off datasets.
 */
@Configuration
@RequiredArgsConstructor
@Slf4j
public class EftDecisionSweepJob {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager batchTransactionManager;
    private final EftDecisionSweepService eftDecisionSweepService;
    private final JobLauncher jobLauncher;

    @Bean
    public Tasklet eftDecisionSweepTasklet() {
        return (contribution, chunkContext) -> {
            int resolved = eftDecisionSweepService.sweepOnce();
            if (resolved > 0) {
                log.info("EftDecisionSweepJob resolved {} correlation ids this cycle", resolved);
            }
            return RepeatStatus.FINISHED;
        };
    }

    @Bean
    public Step eftDecisionSweepStep() {
        return new org.springframework.batch.core.step.builder.StepBuilder("eftDecisionSweepStep", jobRepository)
                .tasklet(eftDecisionSweepTasklet(), batchTransactionManager)
                .build();
    }

    @Bean
    public Job eftDecisionSweepJobDefinition() {
        return new org.springframework.batch.core.job.builder.JobBuilder("EftDecisionSweepJob", jobRepository)
                .start(eftDecisionSweepStep())
                .build();
    }

    /**
     * Fires every 2 seconds. Each run gets a unique JobParameters (timestamp)
     * because Spring Batch refuses to re-run an identical completed job
     * instance - this is a polling job, not a one-shot batch run, so every
     * tick is intentionally treated as a new instance.
     */
    @LoggableMethodExecution("EftDecisionSweepJob.trigger")
    @Scheduled(fixedDelay = 2000)
    public void trigger() {
        try {
            JobParameters params = new JobParametersBuilder()
                    .addLong("triggeredAtMs", System.currentTimeMillis())
                    .toJobParameters();
            jobLauncher.run(eftDecisionSweepJobDefinition(), params);
        } catch (Exception e) {
            log.error("EftDecisionSweepJob run failed - will retry on next scheduled tick", e);
        }
    }
}
