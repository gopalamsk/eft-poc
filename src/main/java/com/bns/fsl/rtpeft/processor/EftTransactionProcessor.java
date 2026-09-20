package com.bns.fsl.eft.processor;

import com.bns.fsl.eft.EftFraudPaymentResponse;
import com.bns.fsl.eft.aspect.LoggableMethodExecution;
import com.bns.fsl.eft.context.EftTransactionContext;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
@LoggableMethodExecution
public class EftTransactionProcessor {


    public EftTransactionContext process(EftTransactionContext context) {
        // TODO: implement full pipeline orchestration, enriching `context` at each step:
        //   context.setAciRTPaymentRequest(...) / setAciNRTPaymentRequest(...)
        //   context.setPhubPaymentResponse(...)
        //   context.setProcessingStatus(...) / setErrorMessage(...)
        context.setPhubPaymentResponse(new EftFraudPaymentResponse());
        context.setProcessingStatus("PROCESSED");
        return context;
    }
}
