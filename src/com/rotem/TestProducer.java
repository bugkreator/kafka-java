package com.rotem;

import java.util.Properties;
import kafka.javaapi.producer.Producer;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.UUID;

import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import kafka.producer.Partitioner;

public class TestProducer {

    final static Logger logger = Logger.getLogger(TestProducer.class);

    private String topic = "";
    private Producer<String, String> producer;
    private int messageCounter = 0;
    private void init() {
        Properties props = new Properties();
        props.put("metadata.broker.list","ubuntu:9092, ubuntu:9093,ubuntu:9094,ubuntu:9095,ubuntu:9096,ubuntu:9097");
        props.put("serializer.class","kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        props.put("partitioner.class", "com.rotem.MyPartitioner");
        //props.put("compression.codec", "gzip");
        ProducerConfig config = new ProducerConfig(props);
        producer =  new Producer<String,String>(config);
    }

    public TestProducer(String Topic)
    {
        topic = Topic;
        init();
    }

    public void send(String Text)
    {
        String message = "Message #" + Integer.toString(messageCounter++) + " : " + (new SimpleDateFormat("YYYY-MM-dd hh:mm:ss.SSS")).format(Calendar.getInstance().getTime()) + " : " + Text;
        String key = UUID.randomUUID().toString();
        //key = null; // make sure all goes to one partition
        //logger.info(key);
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, key, message);
        producer.send(data);
    }

    public void close() {
        producer.close();
    }
}
