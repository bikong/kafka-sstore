package edu.brown.kfkstore;

public class Forward implements ForwardProperties{
    public static void main(String[] args) {
        ForwardProducer producerThread = new ForwardProducer(ForwardProperties.topic, ForwardProperties.filePath);
        producerThread.start();
        ForwardConsumer consumerThread = new ForwardConsumer(ForwardProperties.topic);
        consumerThread.start();
    }
}
