package org.apache.parquet.proto;

import com.google.protobuf.Descriptors;
import com.twitter.elephantbird.util.Protobufs;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.proto.test.TestProtobuf;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class ProtoDescriptorSupportTest {

  @Test
  public void testProtoMessageClass() throws Exception {
    Configuration conf =  new Configuration();
    ProtoDescriptorSupport prs = new ProtoDescriptorSupport(TestProtobuf.InnerMessage.class);
    Descriptors.Descriptor msgDesc = prs.getMessageDescriptor(conf);
    Descriptors.Descriptor msgDesc2 = Protobufs.getMessageDescriptor(TestProtobuf.InnerMessage.class);
    assertEquals(msgDesc,msgDesc2 );
  }

}
