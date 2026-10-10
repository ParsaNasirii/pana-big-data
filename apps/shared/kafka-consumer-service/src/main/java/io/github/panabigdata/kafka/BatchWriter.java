package io.github.panabigdata.kafka;

import java.io.IOException;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface BatchWriter {

  WriteResult write(List<ConsumerRecord<byte[], GenericRecord>> records) throws IOException;
}
