// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: ria/lettuce/streaming/streaming.proto

package ria.lettuce.streaming;

import com.google.protobuf.*;

public final class StreamingServiceProto {
    private StreamingServiceProto() {
    }

    public static void registerAllExtensions(ExtensionRegistryLite registry) {
    }

    public static void registerAllExtensions(ExtensionRegistry registry) {
        registerAllExtensions((ExtensionRegistryLite) registry);
    }

    static final Descriptors.Descriptor internal_static_ria_lettuce_streaming_ApiReq_descriptor;
    static final GeneratedMessageV3.FieldAccessorTable internal_static_ria_lettuce_streaming_ApiReq_fieldAccessorTable;
    static final Descriptors.Descriptor internal_static_ria_lettuce_streaming_Subscription_descriptor;
    static final GeneratedMessageV3.FieldAccessorTable internal_static_ria_lettuce_streaming_Subscription_fieldAccessorTable;

    public static Descriptors.FileDescriptor getDescriptor() {
        return descriptor;
    }

    private static Descriptors.FileDescriptor descriptor;

    static {
        String[] descriptorData = {
          "\n%ria/lettuce/streaming/streaming.proto\022" +
            "\025ria.lettuce.streaming\032\031google/protobuf/" +
            "any.proto\"\235\001\n\006ApiReq\022\026\n\016identification\030\001" +
            " \001(\t\0224\n\006filter\030\002 \001(\0162$.ria.lettuce.strea" +
            "ming.ApiReq.Filter\022\n\n\002id\030\003 \001(\003\"9\n\006Filter" +
            "\022\n\n\006SAMPLE\020\000\022\t\n\005TRACK\020\001\022\n\n\006FOLLOW\020\002\022\014\n\010L" +
            "OCATION\020\003\"0\n\014Subscription\022\017\n\007channel\030\001 \001" +
            "(\t\022\017\n\007message\030\002 \001(\t2\301\001\n\020StreamingService" +
            "\022T\n\014RequestReply\022\035.ria.lettuce.streaming" +
            ".ApiReq\032#.ria.lettuce.streaming.Subscrip" +
            "tion\"\000\022W\n\rRequestStream\022\035.ria.lettuce.st" +
            "reaming.ApiReq\032#.ria.lettuce.streaming.S" +
            "ubscription\"\0000\001B0\n\025ria.lettuce.streaming" +
            "B\025StreamingServiceProtoP\001b\006proto3"
        };
        descriptor = Descriptors.FileDescriptor.internalBuildGeneratedFileFrom(
          descriptorData,
          new Descriptors.FileDescriptor[]{AnyProto.getDescriptor()});
        internal_static_ria_lettuce_streaming_ApiReq_descriptor = getDescriptor()
          .getMessageTypes().get(0);
        internal_static_ria_lettuce_streaming_ApiReq_fieldAccessorTable = new GeneratedMessageV3.FieldAccessorTable(
          internal_static_ria_lettuce_streaming_ApiReq_descriptor,
          new String[]{"Identification", "Filter", "Id"});
        internal_static_ria_lettuce_streaming_Subscription_descriptor = getDescriptor()
          .getMessageTypes().get(1);
        internal_static_ria_lettuce_streaming_Subscription_fieldAccessorTable = new GeneratedMessageV3.FieldAccessorTable(
          internal_static_ria_lettuce_streaming_Subscription_descriptor,
          new String[]{"Channel", "Message"});
        AnyProto.getDescriptor();
    }

    // @@protoc_insertion_point(outer_class_scope)
}
