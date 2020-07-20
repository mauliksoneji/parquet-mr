package org.apache.parquet.proto;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.twitter.elephantbird.util.Protobufs;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.BadConfigurationException;

public class ProtoDescriptorSupport {

  private Descriptors.Descriptor messageDescriptor;
  private Class<? extends Message> protoMessage;

  public ProtoDescriptorSupport(Class<? extends Message> protoMessage) {
    this.protoMessage = protoMessage;
  }

  public ProtoDescriptorSupport(Descriptors.Descriptor messageDescriptor) {
    this.messageDescriptor = messageDescriptor;
  }

  public Descriptors.Descriptor getMessageDescriptor(Configuration configuration) {
    // if no protobuf descriptor was given in constructor, load descriptor from configuration (set with setProtobufClass)
    if (protoMessage == null && messageDescriptor == null) {
      Class<? extends Message> pbClass = configuration.getClass(ProtoWriteSupport.PB_CLASS_WRITE, null, Message.class);
      if (pbClass != null) {
        protoMessage = pbClass;
      } else {
        String msg = "Protocol buffer class not specified.";
        String hint = " Please use method ProtoParquetOutputFormat.setProtobufClass(...) or other similar method.";
        throw new BadConfigurationException(msg + hint);
      }
    }

    if (messageDescriptor == null) {
      messageDescriptor = Protobufs.getMessageDescriptor(protoMessage);
    }
    return messageDescriptor;
  }
}
