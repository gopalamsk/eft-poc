package com.bns.fsl.emt.incoming.event.consumer;

import com.bns.fsl.emt.incoming.event.aspect.LogExecutionTime;
import com.bns.fsl.emt.incoming.event.exception.MappingException;
import com.bns.fsl.emt.incoming.event.processor.MessageConverter;
import com.bns.fsl.emt.incoming.event.processor.PayhubEventProcessor;
import com.bns.fsl.emt.incoming.event.util.CommonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
@Component
@Slf4j
@RequiredArgsConstructor
public class PayhubEventKafkaListener implements KafkaListenerInterface {

    private static final String MDC_REQUEST_ID = "requestId";
    private final PayhubEventProcessor payhubEventProcessor;
    private final MessageConverter messageConverter;

    @Override
    @LogExecutionTime
    public void consumePayhubEventListener(ConsumerRecord<String, String> consumerRecord, Acknowledgment acknowledgment) {
        var payload = consumerRecord.value();
        if (payload == null || payload.isBlank()) {
            log.warn("Skipping invalid empty message. Topic: {}", consumerRecord.topic());
            acknowledgment.acknowledge();
            return;
        }
        try {
            var fslIncomingPaymentEventsRequest = messageConverter.jsonToFslIncomingPaymentEventsRequestObject(payload);
            try ( ) {
                log.info("Processing Payhub event started.");
                // Invoke Business Logic
                payhubEventProcessor.processPayhubEvent(fslIncomingPaymentEventsRequest);
                log.info("Processing Payhub event completed successfully.");
            acknowledgment.acknowledge();
        } catch (Exception e) {
            // ⚠️ RETRYABLE ERROR (DB down, Network, etc.)
            log.error("Transient error processing Payhub event. Retrying...", e);
            throw new RuntimeException("Retryable error processing Kafka message", e);
        }
    } catch (MappingException e) {
            throw new RuntimeException(e);
        }
    }} m

    }
}
