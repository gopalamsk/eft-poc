package com.bns.fsl.rtpeft.mapper;

import com.bns.fsl.rtpeft.model.EftDecisionOutcome;
import org.mapstruct.Mapper;

/** Maps a resolved decision outcome to the NRT audit message body published
 *  to the NRT audit queue - carries the original request plus the decision,
 *  matching the shape the existing EMT flow already sends to NRTMQ. */
@Mapper(componentModel = "spring")
public interface EFTNRTMapper extends BaseMapper<EftDecisionOutcome, String> {

    @Override
    default String map(EftDecisionOutcome source) {
        return "{"
                + "\"correlationId\":\"" + source.getCorrelationId() + "\","
                + "\"decision\":\"" + source.getDecision() + "\","
                + "\"originalRequest\":" + source.getOriginalRequest()
                + "}";
    }
}
