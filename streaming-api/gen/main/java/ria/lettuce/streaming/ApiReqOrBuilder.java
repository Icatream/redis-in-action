// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: ria/lettuce/streaming/streaming.proto

package ria.lettuce.streaming;

import com.google.protobuf.ByteString;
import com.google.protobuf.MessageOrBuilder;

// @@protoc_insertion_point(interface_extends:ria.lettuce.streaming.ApiReq)
public interface ApiReqOrBuilder extends MessageOrBuilder {

    /**
     * <code>string identification = 1;</code>
     */
    String getIdentification();

    /**
     * <code>string identification = 1;</code>
     */
    ByteString getIdentificationBytes();

    /**
     * <code>.ria.lettuce.streaming.ApiReq.Filter filter = 2;</code>
     */
    int getFilterValue();

    /**
     * <code>.ria.lettuce.streaming.ApiReq.Filter filter = 2;</code>
     */
    ApiReq.Filter getFilter();

    /**
     * <code>int64 id = 3;</code>
     */
    long getId();
}