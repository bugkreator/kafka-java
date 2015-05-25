package com.rotem;

import java.util.UUID;

public class Main {

    public static void main(String[] args) {
	    TestProducer tp = new TestProducer("topic8");
        for (int i = 0; i<10000; i++) {
            tp.send("Hello World");
        }
        tp.close();
    }
}
