package com.bns.fsl.rtpeft.mapper;

/** Marker interface for MapStruct mappers in this package - keeps a
 *  consistent naming/generation convention across EFTRTMapper and EFTNRTMapper. */
public interface BaseMapper<S, T> {
    T map(S source);
}
