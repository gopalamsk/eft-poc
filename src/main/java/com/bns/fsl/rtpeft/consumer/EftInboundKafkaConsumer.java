package com.bns.fsl.rtpeft.consumer;

import com.bns.fsl.rtpeft.context.EftTransactionContext;
import com.bns.fsl.rtpeft.model.EftTransactionPayload;
import com.bns.fsl.rtpeft.processor.EftTransactionProcessor;
import com.bns.fsl.rtpeft.util.CommonUtil;
import com.bns.fsl.rtpeft.util.JsonUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Listens on the inbound Kafka topic - one message per EFT transaction
 * (Optimus payment hub splits the batch file before publishing). The Kafka
 * offset is only acknowledged after EftTransactionProcessor has durably
 * written the PENDING row and attempted the ACI publish; a failure before
 * that point simply leaves the offset uncommitted and the message is
 * redelivered on restart - see README "Failure scenarios".
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class EftInboundKafkaConsumer {

    private final EftTransactionProcessor eftTransactionProcessor;
    private final JsonUtil jsonUtil;

    @KafkaListener(topics = "${fsl.kafka.topic.eft-inbound}", groupId = "${fsl.kafka.consumer.group-id}")
    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        String rawJson = record.value();
        try {
            EftTransactionPayload payload = jsonUtil.fromJson(rawJson, EftTransactionPayload.class);
            String correlationId = CommonUtil.isBlank(payload.getCorrelationId())
                    ? CommonUtil.generateCorrelationId()
                    : payload.getCorrelationId();

            EftTransactionContext context = EftTransactionContext.of(correlationId, payload.getFileId(), rawJson);
            eftTransactionProcessor.process(context, payload);

            acknowledgment.acknowledge();
        } catch (Exception e) {
            log.error("Failed to process inbound EFT transaction, offset will not be committed, partition={} offset={}",
                    record.partition(), record.offset(), e);
            // Deliberately not acknowledging - message is redelivered. A persistently
            // failing message (bad data ACI can never score) should route to a DLQ
            // topic rather than block the partition; wire that in via a
            // DefaultErrorHandler + DeadLetterPublishingRecoverer per README "Poison messages".
        }
    }
}
