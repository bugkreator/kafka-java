package com.rotem;

public class Main {

    public static void main(String[] args) {
	    TestProducer tp = new TestProducer("topic7");
        tp.send("Hello World");
        tp.close();
    }
}
