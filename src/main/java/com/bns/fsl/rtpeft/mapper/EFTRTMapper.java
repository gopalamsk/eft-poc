package com.bns.fsl.rtpeft.mapper;

import com.bns.fsl.rtpeft.constants.AppConstants;
import com.bns.fsl.rtpeft.model.AciRequestMessage;
import com.bns.fsl.rtpeft.model.EftTransactionPayload;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

/** Maps an inbound EFT transaction to the ACI request message shape - the
 *  "RT" naming is kept for consistency with the existing EMT RT mapper even
 *  though this flow is not itself real-time. */
@Mapper(componentModel = "spring")
public interface EFTRTMapper extends BaseMapper<EftTransactionPayload, AciRequestMessage> {

    @Override
    @Mapping(target = "flow", constant = AppConstants.FLOW_EFT)
    @Mapping(source = "rawJson", target = "payload")
    AciRequestMessage map(EftTransactionPayload source);
}
