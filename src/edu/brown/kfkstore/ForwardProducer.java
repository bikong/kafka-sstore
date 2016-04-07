// InitProducer : Data streaming pushed to kafka

package edu.brown.kfkstore;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


public class ForwardProducer extends Thread{

    private final KafkaProducer<Integer, String> producer;
    private final String topic;
    private Scanner scanner;

    public ForwardProducer(String topic, String filePath) {
        this.topic = topic;
        Properties props = new Properties();
        try {
            scanner = new Scanner(new File(filePath));
            //scanner.useDelimiter(",");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        props.put("bootstrap.servers", "localhost:9092");
        props.put("client.id", "d2kProducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }
    
    public void run() {
        int messageNo = 1;
		while(scanner.hasNext()) {
            try{
                String messageStr = scanner.next();
				producer.send(new ProducerRecord<Integer, String>
						  (topic, messageNo, messageStr)).get();
            } catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
            messageNo++;
		}
        scanner.close();
    }
}


