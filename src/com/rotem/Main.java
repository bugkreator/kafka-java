package com.rotem;



import java.util.UUID;

public class Main {

    public static void main(String[] args) {
	    TestProducer tp = new TestProducer("topic11");
        String filler = "";
        for (int i = 0; i<10; i++)
        {
            filler+= UUID.randomUUID().toString().replace("-","");
        }

        for (int i = 0; i<1000; i++) {
            tp.send(filler);
        }
        tp.close();
    }
}
