package org.zproto;

import static org.junit.Assert.*;
import org.junit.Test;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZFrame;
import org.zeromq.ZContext;

public class TestDafkaProto
{
    @Test
    public void testDafkaProto () throws InterruptedException
    {
        System.out.printf (" * dafka_proto: ");

        //  Simple create/destroy test
        DafkaProto self = new DafkaProto ('A');
        assert (self != null);
        self.destroy ();

        //  Create pair of sockets we can send through
        ZContext ctx = new ZContext ();
        assert (ctx != null);

        Socket output = ctx.createSocket (ZMQ.PUB);
        assert (output != null);
        output.bind ("inproc://selftest");
        Socket input = ctx.createSocket (ZMQ.SUB);
        assert (input != null);
        input.connect ("inproc://selftest");

        //  Encode/send/decode and verify each message type

        self = new DafkaProto (DafkaProto.MSG);
        self.setTopic ("HELLO");
        self.subscribe(input, DafkaProto.MSG, "HELLO");
        Thread.sleep(100);  //  Give time for subscription to become valid
        self.setSubject ("Life is short but Now lasts for ever");
        self.setAddress ("Life is short but Now lasts for ever");
        self.setSequence ((byte) 123);
        self.setContent (new ZFrame ("Captcha Diem"));
        self.send (output);

        self = DafkaProto.recv (input);
        assert (self != null);
        assertEquals(self.topic(), "HELLO");
        assertEquals (self.subject (), "Life is short but Now lasts for ever");
        assertEquals (self.address (), "Life is short but Now lasts for ever");
        assertEquals (self.sequence (), 123);
        assertTrue (self.content ().streq ("Captcha Diem"));
        self.destroy ();

        self = new DafkaProto (DafkaProto.DIRECT_MSG);
        self.setTopic ("HELLO");
        self.subscribe(input, DafkaProto.DIRECT_MSG, "HELLO");
        Thread.sleep(100);  //  Give time for subscription to become valid
        self.setSubject ("Life is short but Now lasts for ever");
        self.setAddress ("Life is short but Now lasts for ever");
        self.setSequence ((byte) 123);
        self.setContent (new ZFrame ("Captcha Diem"));
        self.send (output);

        self = DafkaProto.recv (input);
        assert (self != null);
        assertEquals(self.topic(), "HELLO");
        assertEquals (self.subject (), "Life is short but Now lasts for ever");
        assertEquals (self.address (), "Life is short but Now lasts for ever");
        assertEquals (self.sequence (), 123);
        assertTrue (self.content ().streq ("Captcha Diem"));
        self.destroy ();

        self = new DafkaProto (DafkaProto.FETCH);
        self.setTopic ("HELLO");
        self.subscribe(input, DafkaProto.FETCH, "HELLO");
        Thread.sleep(100);  //  Give time for subscription to become valid
        self.setSubject ("Life is short but Now lasts for ever");
        self.setSequence ((byte) 123);
        self.setCount ((byte) 123);
        self.setAddress ("Life is short but Now lasts for ever");
        self.send (output);

        self = DafkaProto.recv (input);
        assert (self != null);
        assertEquals(self.topic(), "HELLO");
        assertEquals (self.subject (), "Life is short but Now lasts for ever");
        assertEquals (self.sequence (), 123);
        assertEquals (self.count (), 123);
        assertEquals (self.address (), "Life is short but Now lasts for ever");
        self.destroy ();

        self = new DafkaProto (DafkaProto.ACK);
        self.setTopic ("HELLO");
        self.subscribe(input, DafkaProto.ACK, "HELLO");
        Thread.sleep(100);  //  Give time for subscription to become valid
        self.setSubject ("Life is short but Now lasts for ever");
        self.setSequence ((byte) 123);
        self.send (output);

        self = DafkaProto.recv (input);
        assert (self != null);
        assertEquals(self.topic(), "HELLO");
        assertEquals (self.subject (), "Life is short but Now lasts for ever");
        assertEquals (self.sequence (), 123);
        self.destroy ();

        self = new DafkaProto (DafkaProto.HEAD);
        self.setTopic ("HELLO");
        self.subscribe(input, DafkaProto.HEAD, "HELLO");
        Thread.sleep(100);  //  Give time for subscription to become valid
        self.setSubject ("Life is short but Now lasts for ever");
        self.setAddress ("Life is short but Now lasts for ever");
        self.setSequence ((byte) 123);
        self.send (output);

        self = DafkaProto.recv (input);
        assert (self != null);
        assertEquals(self.topic(), "HELLO");
        assertEquals (self.subject (), "Life is short but Now lasts for ever");
        assertEquals (self.address (), "Life is short but Now lasts for ever");
        assertEquals (self.sequence (), 123);
        self.destroy ();

        self = new DafkaProto (DafkaProto.DIRECT_HEAD);
        self.setTopic ("HELLO");
        self.subscribe(input, DafkaProto.DIRECT_HEAD, "HELLO");
        Thread.sleep(100);  //  Give time for subscription to become valid
        self.setSubject ("Life is short but Now lasts for ever");
        self.setAddress ("Life is short but Now lasts for ever");
        self.setSequence ((byte) 123);
        self.send (output);

        self = DafkaProto.recv (input);
        assert (self != null);
        assertEquals(self.topic(), "HELLO");
        assertEquals (self.subject (), "Life is short but Now lasts for ever");
        assertEquals (self.address (), "Life is short but Now lasts for ever");
        assertEquals (self.sequence (), 123);
        self.destroy ();

        self = new DafkaProto (DafkaProto.GET_HEADS);
        self.setTopic ("HELLO");
        self.subscribe(input, DafkaProto.GET_HEADS, "HELLO");
        Thread.sleep(100);  //  Give time for subscription to become valid
        self.setAddress ("Life is short but Now lasts for ever");
        self.send (output);

        self = DafkaProto.recv (input);
        assert (self != null);
        assertEquals(self.topic(), "HELLO");
        assertEquals (self.address (), "Life is short but Now lasts for ever");
        self.destroy ();

        ctx.destroy ();
        System.out.printf ("OK\n");
    }
}
