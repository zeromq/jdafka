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
    public void testDafkaProto ()
    {
        System.out.printf (" * dafka_proto: ");

        //  Simple create/destroy test
        DafkaProto self = new DafkaProto (0);
        assert (self != null);
        self.destroy ();

        //  Create pair of sockets we can send through
        ZContext ctx = new ZContext ();
        assert (ctx != null);

        Socket output = ctx.createSocket (ZMQ.DEALER);
        assert (output != null);
        output.bind ("inproc://selftest");
        Socket input = ctx.createSocket (ZMQ.ROUTER);
        assert (input != null);
        input.connect ("inproc://selftest");

        //  Encode/send/decode and verify each message type

        self = new DafkaProto (DafkaProto.MSG);
        self.setSubject ("Life is short but Now lasts for ever");
        self.setAddress ("Life is short but Now lasts for ever");
        self.setSequence ((byte) 123);
        self.setContent (new ZFrame ("Captcha Diem"));
        self.send (output);

        self = DafkaProto.recv (input);
        assert (self != null);
        assertEquals (self.subject (), "Life is short but Now lasts for ever");
        assertEquals (self.address (), "Life is short but Now lasts for ever");
        assertEquals (self.sequence (), 123);
        assertTrue (self.content ().streq ("Captcha Diem"));
        self.destroy ();

        self = new DafkaProto (DafkaProto.DIRECT_MSG);
        self.setSubject ("Life is short but Now lasts for ever");
        self.setAddress ("Life is short but Now lasts for ever");
        self.setSequence ((byte) 123);
        self.setContent (new ZFrame ("Captcha Diem"));
        self.send (output);

        self = DafkaProto.recv (input);
        assert (self != null);
        assertEquals (self.subject (), "Life is short but Now lasts for ever");
        assertEquals (self.address (), "Life is short but Now lasts for ever");
        assertEquals (self.sequence (), 123);
        assertTrue (self.content ().streq ("Captcha Diem"));
        self.destroy ();

        self = new DafkaProto (DafkaProto.FETCH);
        self.setSubject ("Life is short but Now lasts for ever");
        self.setSequence ((byte) 123);
        self.setCount ((byte) 123);
        self.setAddress ("Life is short but Now lasts for ever");
        self.send (output);

        self = DafkaProto.recv (input);
        assert (self != null);
        assertEquals (self.subject (), "Life is short but Now lasts for ever");
        assertEquals (self.sequence (), 123);
        assertEquals (self.count (), 123);
        assertEquals (self.address (), "Life is short but Now lasts for ever");
        self.destroy ();

        self = new DafkaProto (DafkaProto.ACK);
        self.setSubject ("Life is short but Now lasts for ever");
        self.setSequence ((byte) 123);
        self.send (output);

        self = DafkaProto.recv (input);
        assert (self != null);
        assertEquals (self.subject (), "Life is short but Now lasts for ever");
        assertEquals (self.sequence (), 123);
        self.destroy ();

        self = new DafkaProto (DafkaProto.HEAD);
        self.setSubject ("Life is short but Now lasts for ever");
        self.setAddress ("Life is short but Now lasts for ever");
        self.setSequence ((byte) 123);
        self.send (output);

        self = DafkaProto.recv (input);
        assert (self != null);
        assertEquals (self.subject (), "Life is short but Now lasts for ever");
        assertEquals (self.address (), "Life is short but Now lasts for ever");
        assertEquals (self.sequence (), 123);
        self.destroy ();

        self = new DafkaProto (DafkaProto.DIRECT_HEAD);
        self.setSubject ("Life is short but Now lasts for ever");
        self.setAddress ("Life is short but Now lasts for ever");
        self.setSequence ((byte) 123);
        self.send (output);

        self = DafkaProto.recv (input);
        assert (self != null);
        assertEquals (self.subject (), "Life is short but Now lasts for ever");
        assertEquals (self.address (), "Life is short but Now lasts for ever");
        assertEquals (self.sequence (), 123);
        self.destroy ();

        self = new DafkaProto (DafkaProto.GET_HEADS);
        self.setAddress ("Life is short but Now lasts for ever");
        self.send (output);

        self = DafkaProto.recv (input);
        assert (self != null);
        assertEquals (self.address (), "Life is short but Now lasts for ever");
        self.destroy ();

        ctx.destroy ();
        System.out.printf ("OK\n");
    }
}
