package org.apache.parquet.hadoop;

import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Timestamp;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.DescriptorProto.Builder;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.gojek.offset.OffsetMetadata;

public class OffsetInfo {
  private final String topic;
  private final int partition;
  private final long offset;
  private final long timestamp;
  private final static List<FieldDescriptor> offsetMetadataFields;

  static {
    offsetMetadataFields = OffsetMetadata.getDescriptor().getFields();
  }

  public OffsetInfo(String topic, int partition, long offset, long timestamp) {
    this.topic = topic;
    this.partition = partition;
    this.offset = offset;
    this.timestamp = timestamp;
  }

  public String getTopic() {
    return this.topic;
  }

  public int getPartition() {
    return this.partition;
  }

  public long getOffset() {
    return this.offset;
  }

  public long getTimestamp() {
    return this.timestamp;
  }

  public static List<FieldDescriptor> getMetadataFieldDescriptors() {
    return offsetMetadataFields;
  }

  public Map<FieldDescriptor, Object> getMetadataFields() {
    Instant time = Instant.now();
    List<FieldDescriptor> fieldDescriptorsToAdd = getMetadataFieldDescriptors();
    Map<FieldDescriptor, Object> fdMap = new HashMap<>();
    fdMap.put(fieldDescriptorsToAdd.get(0), offset);
    fdMap.put(fieldDescriptorsToAdd.get(1), partition);
    fdMap.put(fieldDescriptorsToAdd.get(2), topic);
    fdMap.put(fieldDescriptorsToAdd.get(3),
        Timestamp.newBuilder().setSeconds(timestamp / 1000).setNanos((int) ((timestamp % 1000) * 1000000)).build());
    fdMap.put(fieldDescriptorsToAdd.get(4),
        Timestamp.newBuilder().setSeconds(time.getEpochSecond()).setNanos(time.getNano()).build());

    return fdMap;
  }

  public static boolean isMetadataDescriptor(FieldDescriptor fd) {
    return OffsetMetadata.getDescriptor().getFields().contains(fd);
  }
}
