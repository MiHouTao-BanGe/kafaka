package org.kafaka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerCallbackPartitions {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // 1. 创建 kafka 生产者的配置对象
        Properties properties = new Properties();

        // 2. 给 kafka 配置对象添加配置信息

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.136.128:9092,192.168.136.129:9092,192.168.136.130:9092");
        // key,value 序列化（必须）：key.serializer，value.serializer
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        // 添加自定义分区器
        //properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"org.kafaka.MyPartitioner");

        // batch.size：批次大小，默认 16K
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        // linger.ms：等待时间，默认 0
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        // RecordAccumulator：缓冲区大小，默认 32M：buffer.memory
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432);
        // compression.type：压缩，默认 none，可配置值 gzip、snappy、lz4 和 zstd
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        // 设置 acks
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        // 重试次数 retries，默认是 int 最大值，2147483647
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);

        //开幂等性  精准一次写入
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true);

        KafkaProducer<String, String> kafkaProducer = new
                KafkaProducer<>(properties);
        for (int i = 0; i < 15; i++) {
            // 指定数据发送到 1 号分区，key 为空（IDEA 中 ctrl + p 查看参数）
//            kafkaProducer.send(new ProducerRecord<>("two",
//                    1,""+i,"tb" + i), new Callback() {
//
//                @Override
//                public void onCompletion(RecordMetadata metadata, Exception e) {
//
//                        if (e == null){
//                            System.out.println(" 主题： " +
//                                    metadata.topic() + "->" + "分区：" + metadata.partition()
//                            );
//                        }else {

//                            e.printStackTrace();
//                        }
//                }
//            });

            kafkaProducer.send(new ProducerRecord<>("two",i+"","t" + i), new Callback() {

                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {

                    if (e == null){
                        System.out.println(" 主题： " +
                                metadata.topic() + "->" + "分区：" + metadata.partition()
                        );
                    }else {
                        e.printStackTrace();
                    }
                }
            });
        }
        kafkaProducer.close();
    }
}
