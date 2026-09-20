package com.bns.fsl.eft.consumer;

import com.bns.fsl.eft.EftFraudPaymentRequest;
import com.bns.fsl.eft.OriginalGroupInformationAndStatus;
import com.bns.fsl.eft.Payments;
import com.bns.fsl.eft.aspect.LoggableMethodExecution;
import com.bns.fsl.eft.context.EftTransactionContext;
import com.bns.fsl.eft.exception.RtpEftException;
import com.bns.fsl.eft.processor.EftTransactionProcessor;
import com.bns.fsl.eft.util.JsonUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@RequiredArgsConstructor
@Slf4j
@LoggableMethodExecution
public class EftInboundKafkaConsumer {

    private final EftTransactionProcessor processor;
    private final JsonUtil jsonUtil;

    @KafkaListener(
            topics = "${spring.kafka.consumer.topic-name}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, String> consumerRecord, Acknowledgment acknowledgment) {
        String key = consumerRecord.key();
        String payload = consumerRecord.value();
        log.info("Received Kafka message - topic: {}, partition: {}, offset: {}, key: {}",
                consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), key);
        try {
            EftFraudPaymentRequest request = jsonUtil.fromJson(payload, EftFraudPaymentRequest.class);
            log.debug("Deserialized EftFraudPaymentRequest for key: {}", key);

            String transactionId = Optional.ofNullable(request)
                    .map(EftFraudPaymentRequest::getPayments)
                    .map(Payments::getOriginalGroupInformationAndStatus)
                    .map(OriginalGroupInformationAndStatus::getOriginalMessageIdentification)
                    .orElse(null);

            if (transactionId == null) {
                log.warn("transactionId (originalMessageIdentification) missing/null for key: {} — "
                        + "one of payload/originalGroupInformationAndStatus/originalMessageIdentification was null",
                        key);
            }

            EftTransactionContext context = EftTransactionContext.builder()
                    .transactionId(transactionId)
                    .correlationId(transactionId)
                    .phubPaymentRequest(request)
                    .build();

            context = processor.process(context);
            acknowledgment.acknowledge();
            log.info("Successfully processed and acknowledged message for key: {}, transactionId: {}, status: {}",
                    key, transactionId, context.getProcessingStatus());
        } catch (RtpEftException ex) {
            log.error("Processing error for key: {} - [{}] {}", key, ex.getErrorCode(), ex.getMessage(), ex);
            // Not retryable (see KafkaConsumerConfig) — rethrow so the container's error handler
            // logs/recovers and commits the offset (skip), instead of silently stalling here.
            throw ex;
        } catch (Exception ex) {
            log.error("Unexpected error processing message for key: {}", key, ex);
            // Rethrow so DefaultErrorHandler applies the configured retry/backoff policy;
            // swallowing here would make that configuration a no-op.
            throw ex;
        }
    }
}
