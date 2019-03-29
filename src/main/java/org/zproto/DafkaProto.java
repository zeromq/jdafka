/*  =========================================================================
    DafkaProto - dafka_proto

    ** WARNING *************************************************************
    THIS SOURCE FILE IS 100% GENERATED. If you edit this file, you will lose
    your changes at the next build cycle. This is great for temporary printf
    statements. DO NOT MAKE ANY CHANGES YOU WISH TO KEEP. The correct places
    for commits are:

    * The XML model used for this code generation: dafka_proto.xml
    * The code generation script that built this file: zproto_codec_java
    ************************************************************************
    =========================================================================
*/

/*  These are the DafkaProto messages:

    MSG - Message from producer to consumers.
The topic is either the subject or recipient address.
        subject             string
        address             string
        sequence            number 8
        content             frame

    DIRECT_MSG - Direct message from producer to consumer.
The topic is the recipient address.
        subject             string
        address             string
        sequence            number 8
        content             frame

    FETCH - Consumer publish the message when a message is missing.
Topic is the address of the producer (partition).
Either the producer or a store daemon can answer.
        subject             string
        sequence            number 8
        count               number 4
        address             string

    ACK - Ack from a store daemon to a producer.
Topic is the address of the producer.
        subject             string
        sequence            number 8

    HEAD -
        subject             string
        address             string
        sequence            number 8

    DIRECT_HEAD -
        subject             string
        address             string
        sequence            number 8

    GET_HEADS -
        address             string
*/

package org.zproto;

import java.nio.ByteBuffer;

import org.zeromq.SocketType;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

public class DafkaProto implements java.lang.AutoCloseable
{

    public static final char MSG                  = 'M';
    public static final char DIRECT_MSG           = 'D';
    public static final char FETCH                = 'F';
    public static final char ACK                  = 'K';
    public static final char HEAD                 = 'H';
    public static final char DIRECT_HEAD          = 'E';
    public static final char GET_HEADS            = 'G';

    //  Structure of our class
    private ZFrame routingId;           // Routing_id from ROUTER, if any
    private char id;                    //  DafkaProto message ID
    private String topic;               //  Topic to send and receive over pub/sub
    private boolean isSubscribe;       //  Indicate if it is a subscribe or unsubscribe command
    private ByteBuffer needle;          //  Read/write pointer for serialization

    private String subject;
    private String address;
    private long sequence;
    private ZFrame content;
    private long count;

    public DafkaProto(char id)
    {
        this.id = id;
    }

    public void destroy()
    {
        close();
    }

    @Override
    public void close()
    {
        //  Destroy frame fields
        if (content != null)
            content.destroy();
        content = null;
    }
    //  --------------------------------------------------------------------------
    //  Network data encoding macros


    //  Put a 1-byte number to the frame
    private final void putNumber1 (int value)
    {
        needle.put ((byte) value);
    }

    //  Get a 1-byte number to the frame
    //  then make it unsigned
    private byte getNumber1 ()
    {
        int value = needle.get ();
        if (value < 0)
            value = (0xff) & value;
        return (byte) value;
    }

    //  Put a 2-byte number to the frame
    private final void putNumber2 (int value)
    {
        needle.putShort ((short) value);
    }

    //  Get a 2-byte number to the frame
    private int getNumber2 ()
    {
        int value = needle.getShort ();
        if (value < 0)
            value = (0xffff) & value;
        return value;
    }

    //  Put a 4-byte number to the frame
    private final void putNumber4 (long value)
    {
        needle.putInt ((int) value);
    }

    //  Get a 4-byte number to the frame
    //  then make it unsigned
    private long getNumber4 ()
    {
        long value = needle.getInt ();
        if (value < 0)
            value = (0xffffffff) & value;
        return value;
    }

    //  Put a 8-byte number to the frame
    public void putNumber8 (long value)
    {
        needle.putLong (value);
    }

    //  Get a 8-byte number to the frame
    public long getNumber8 ()
    {
        return needle.getLong ();
    }


    //  Put a block to the frame
    private void putBlock (byte [] value, int size)
    {
        needle.put (value, 0, size);
    }

    private byte [] getBlock (int size)
    {
        byte [] value = new byte [size];
        needle.get (value);

        return value;
    }

    //  Put a string to the frame
    public void putString (String value)
    {
        byte [] bytes = value.getBytes(ZMQ.CHARSET);
        needle.put ((byte) bytes.length);
        needle.put (bytes);
    }

    //  Get a string from the frame
    public String getString ()
    {
        int size = getNumber1 ();
        byte [] value = new byte [size];
        needle.get (value);

        return new String (value, ZMQ.CHARSET);
    }

        //  Put a string to the frame
    public void putLongString (String value)
    {
        byte [] bytes = value.getBytes(ZMQ.CHARSET);
        needle.putInt (bytes.length);
        needle.put (bytes);
    }

    //  Get a string from the frame
    public String getLongString ()
    {
        long size = getNumber4 ();
        byte [] value = new byte [(int) size];
        needle.get (value);

        return new String (value, ZMQ.CHARSET);
    }
    //  --------------------------------------------------------------------------
    //  Receive and parse a DafkaProto from the socket. Returns new object or
    //  null if error. Will block if there's no message waiting.

    public static DafkaProto recv (Socket input)
    {
        assert (input != null);
        DafkaProto self = new DafkaProto ('A');
        ZFrame frame = null;

        try {
            //  Read valid message frame from socket; we loop over any
            //  garbage data we might receive from badly-connected peers
            while (true) {
                //  If we're reading from a ROUTER socket, get routingId
                if (input.getType () == ZMQ.ROUTER) {
                    self.routingId = ZFrame.recvFrame (input);
                    if (self.routingId == null)
                        return null;         //  Interrupted
                    if (!self.routingId.hasData())
                        return null;         //  Empty Frame (eg recv-timeout)
                    if (!input.hasReceiveMore ())
                        throw new IllegalArgumentException ();
                }
                //  Read and parse command in frame
                frame = ZFrame.recvFrame (input);
                if (frame == null)
                    return null;             //  Interrupted

                //  Get and check protocol signature
                self.needle = ByteBuffer.wrap (frame.getData ());
                //  In case of pubsub the message cannot contain garbage data
                break;
            }

            if (input.getSocketType().equals(SocketType.XPUB)) {
                byte isSubscribe = self.getNumber1 ();
                self.isSubscribe = isSubscribe == 1;
                self.id = (char) self.getNumber1 ();
                final StringBuilder topicBuilder = new StringBuilder();
                char next = (char) self.needle.get();
                while (self.needle.hasRemaining()) {
                    topicBuilder.append(next);
                    next = (char) self.needle.get();
                }
                self.topic = topicBuilder.toString();
                return self;
            }

            //  Get message id, which is first byte in frame
            self.id = (char) self.getNumber1 ();
            final StringBuilder topicBuilder = new StringBuilder();
            char next = (char) self.needle.get();
            while ('\0' != next) {
                topicBuilder.append(next);
                next = (char) self.needle.get();
            }
            self.topic = topicBuilder.toString();
            int listSize;
            int hashSize;

            switch (self.id) {
            case MSG:
                {
                self.subject = self.getString ();
                self.address = self.getString ();
                self.sequence = self.getNumber8 ();
                //  Get next frame, leave current untouched
                if (!input.hasReceiveMore ())
                    throw new IllegalArgumentException ();
                self.content = ZFrame.recvFrame (input);
                }
                break;

            case DIRECT_MSG:
                {
                self.subject = self.getString ();
                self.address = self.getString ();
                self.sequence = self.getNumber8 ();
                //  Get next frame, leave current untouched
                if (!input.hasReceiveMore ())
                    throw new IllegalArgumentException ();
                self.content = ZFrame.recvFrame (input);
                }
                break;

            case FETCH:
                {
                self.subject = self.getString ();
                self.sequence = self.getNumber8 ();
                self.count = self.getNumber4 ();
                self.address = self.getString ();
                }
                break;

            case ACK:
                {
                self.subject = self.getString ();
                self.sequence = self.getNumber8 ();
                }
                break;

            case HEAD:
                {
                self.subject = self.getString ();
                self.address = self.getString ();
                self.sequence = self.getNumber8 ();
                }
                break;

            case DIRECT_HEAD:
                {
                self.subject = self.getString ();
                self.address = self.getString ();
                self.sequence = self.getNumber8 ();
                }
                break;

            case GET_HEADS:
                {
                self.address = self.getString ();
                }
                break;

            default:
                throw new IllegalArgumentException ();
            }

            return self;

        } catch (Exception e) {
            //  Error returns
            System.out.printf ("E: malformed message '%c'\n", self.id);
            self.destroy ();
            return null;
        } finally {
            if (frame != null)
                frame.destroy ();
        }
    }

    //  --------------------------------------------------------------------------
    //  Send the DafkaProto to the socket, and destroy it

    public boolean send (Socket socket)
    {
        assert (socket != null);

        ZMsg msg = new ZMsg();
        //  If we're sending to a ROUTER, send the routingId first
        if (socket.getType () == ZMQ.ROUTER) {
            msg.add (routingId);
        }

        int frameSize = 1 + this.topic.length() + 1; //  Message ID, topic and NULL
        switch (id) {
        case MSG:
            {
            //  subject is a string with 1-byte length
            frameSize ++;
            frameSize += (subject != null) ? subject.getBytes(ZMQ.CHARSET).length : 0;
            //  address is a string with 1-byte length
            frameSize ++;
            frameSize += (address != null) ? address.getBytes(ZMQ.CHARSET).length : 0;
            //  sequence is a 8-byte integer
            frameSize += 8;
            }
            break;

        case DIRECT_MSG:
            {
            //  subject is a string with 1-byte length
            frameSize ++;
            frameSize += (subject != null) ? subject.getBytes(ZMQ.CHARSET).length : 0;
            //  address is a string with 1-byte length
            frameSize ++;
            frameSize += (address != null) ? address.getBytes(ZMQ.CHARSET).length : 0;
            //  sequence is a 8-byte integer
            frameSize += 8;
            }
            break;

        case FETCH:
            {
            //  subject is a string with 1-byte length
            frameSize ++;
            frameSize += (subject != null) ? subject.getBytes(ZMQ.CHARSET).length : 0;
            //  sequence is a 8-byte integer
            frameSize += 8;
            //  count is a 4-byte integer
            frameSize += 4;
            //  address is a string with 1-byte length
            frameSize ++;
            frameSize += (address != null) ? address.getBytes(ZMQ.CHARSET).length : 0;
            }
            break;

        case ACK:
            {
            //  subject is a string with 1-byte length
            frameSize ++;
            frameSize += (subject != null) ? subject.getBytes(ZMQ.CHARSET).length : 0;
            //  sequence is a 8-byte integer
            frameSize += 8;
            }
            break;

        case HEAD:
            {
            //  subject is a string with 1-byte length
            frameSize ++;
            frameSize += (subject != null) ? subject.getBytes(ZMQ.CHARSET).length : 0;
            //  address is a string with 1-byte length
            frameSize ++;
            frameSize += (address != null) ? address.getBytes(ZMQ.CHARSET).length : 0;
            //  sequence is a 8-byte integer
            frameSize += 8;
            }
            break;

        case DIRECT_HEAD:
            {
            //  subject is a string with 1-byte length
            frameSize ++;
            frameSize += (subject != null) ? subject.getBytes(ZMQ.CHARSET).length : 0;
            //  address is a string with 1-byte length
            frameSize ++;
            frameSize += (address != null) ? address.getBytes(ZMQ.CHARSET).length : 0;
            //  sequence is a 8-byte integer
            frameSize += 8;
            }
            break;

        case GET_HEADS:
            {
            //  address is a string with 1-byte length
            frameSize ++;
            frameSize += (address != null) ? address.getBytes(ZMQ.CHARSET).length : 0;
            }
            break;

        default:
            System.out.printf ("E: bad message type '%d', not sent\n", id);
            assert (false);
        }
        //  Now serialize message into the frame
        ZFrame frame = new ZFrame (new byte [frameSize]);
        needle = ByteBuffer.wrap (frame.getData ());
        int frameFlags = 0;
        putNumber1 ((byte) id);
        needle.put (topic.getBytes(ZMQ.CHARSET));
        needle.put ((byte) '\0');

        switch (id) {
        case MSG:
            {
            if (subject != null)
                putString (subject);
            else
                putNumber1 ((byte) 0);      //  Empty string
            if (address != null)
                putString (address);
            else
                putNumber1 ((byte) 0);      //  Empty string
            putNumber8 (sequence);
            }
            break;

        case DIRECT_MSG:
            {
            if (subject != null)
                putString (subject);
            else
                putNumber1 ((byte) 0);      //  Empty string
            if (address != null)
                putString (address);
            else
                putNumber1 ((byte) 0);      //  Empty string
            putNumber8 (sequence);
            }
            break;

        case FETCH:
            {
            if (subject != null)
                putString (subject);
            else
                putNumber1 ((byte) 0);      //  Empty string
            putNumber8 (sequence);
            putNumber4 (count);
            if (address != null)
                putString (address);
            else
                putNumber1 ((byte) 0);      //  Empty string
            }
            break;

        case ACK:
            {
            if (subject != null)
                putString (subject);
            else
                putNumber1 ((byte) 0);      //  Empty string
            putNumber8 (sequence);
            }
            break;

        case HEAD:
            {
            if (subject != null)
                putString (subject);
            else
                putNumber1 ((byte) 0);      //  Empty string
            if (address != null)
                putString (address);
            else
                putNumber1 ((byte) 0);      //  Empty string
            putNumber8 (sequence);
            }
            break;

        case DIRECT_HEAD:
            {
            if (subject != null)
                putString (subject);
            else
                putNumber1 ((byte) 0);      //  Empty string
            if (address != null)
                putString (address);
            else
                putNumber1 ((byte) 0);      //  Empty string
            putNumber8 (sequence);
            }
            break;

        case GET_HEADS:
            {
            if (address != null)
                putString (address);
            else
                putNumber1 ((byte) 0);      //  Empty string
            }
            break;

        }
        //  Now send the data frame
        msg.add(frame);

        //  Now send any frame fields, in order
        switch (id) {
        case MSG:
            {
            //  If content isn't set, send an empty frame
            if (content == null)
                content = new ZFrame ("".getBytes ());
            msg.add(content);
            }
            break;
        case DIRECT_MSG:
            {
            //  If content isn't set, send an empty frame
            if (content == null)
                content = new ZFrame ("".getBytes ());
            msg.add(content);
            }
            break;
        }
        switch (id) {
        }
        //  Destroy DafkaProto object
        msg.send(socket);
        destroy ();
        return true;
    }


//  --------------------------------------------------------------------------
//  Send the MSG to the socket in one step

    public static void sendMsg (
        Socket output,
        String subject,
        String address,
        long sequence,
        ZFrame content)
    {
	sendMsg (
		    output,
		    null,
		    subject,
		    address,
		    sequence,
		    content);
    }

//  --------------------------------------------------------------------------
//  Send the MSG to a router socket in one step

    public static void sendMsg (
        Socket output,
	ZFrame routingId,
        String subject,
        String address,
        long sequence,
        ZFrame content)
    {
        DafkaProto self = new DafkaProto (DafkaProto.MSG);
        if (routingId != null)
        {
	        self.setRoutingId (routingId);
        }
        self.setSubject (subject);
        self.setAddress (address);
        self.setSequence (sequence);
        self.setContent (content.duplicate ());
        self.send (output);
    }

//  --------------------------------------------------------------------------
//  Send the DIRECT_MSG to the socket in one step

    public static void sendDirect_Msg (
        Socket output,
        String subject,
        String address,
        long sequence,
        ZFrame content)
    {
	sendDirect_Msg (
		    output,
		    null,
		    subject,
		    address,
		    sequence,
		    content);
    }

//  --------------------------------------------------------------------------
//  Send the DIRECT_MSG to a router socket in one step

    public static void sendDirect_Msg (
        Socket output,
	ZFrame routingId,
        String subject,
        String address,
        long sequence,
        ZFrame content)
    {
        DafkaProto self = new DafkaProto (DafkaProto.DIRECT_MSG);
        if (routingId != null)
        {
	        self.setRoutingId (routingId);
        }
        self.setSubject (subject);
        self.setAddress (address);
        self.setSequence (sequence);
        self.setContent (content.duplicate ());
        self.send (output);
    }

//  --------------------------------------------------------------------------
//  Send the FETCH to the socket in one step

    public static void sendFetch (
        Socket output,
        String subject,
        long sequence,
        long count,
        String address)
    {
	sendFetch (
		    output,
		    null,
		    subject,
		    sequence,
		    count,
		    address);
    }

//  --------------------------------------------------------------------------
//  Send the FETCH to a router socket in one step

    public static void sendFetch (
        Socket output,
	ZFrame routingId,
        String subject,
        long sequence,
        long count,
        String address)
    {
        DafkaProto self = new DafkaProto (DafkaProto.FETCH);
        if (routingId != null)
        {
	        self.setRoutingId (routingId);
        }
        self.setSubject (subject);
        self.setSequence (sequence);
        self.setCount (count);
        self.setAddress (address);
        self.send (output);
    }

//  --------------------------------------------------------------------------
//  Send the ACK to the socket in one step

    public static void sendAck (
        Socket output,
        String subject,
        long sequence)
    {
	sendAck (
		    output,
		    null,
		    subject,
		    sequence);
    }

//  --------------------------------------------------------------------------
//  Send the ACK to a router socket in one step

    public static void sendAck (
        Socket output,
	ZFrame routingId,
        String subject,
        long sequence)
    {
        DafkaProto self = new DafkaProto (DafkaProto.ACK);
        if (routingId != null)
        {
	        self.setRoutingId (routingId);
        }
        self.setSubject (subject);
        self.setSequence (sequence);
        self.send (output);
    }

//  --------------------------------------------------------------------------
//  Send the HEAD to the socket in one step

    public static void sendHead (
        Socket output,
        String subject,
        String address,
        long sequence)
    {
	sendHead (
		    output,
		    null,
		    subject,
		    address,
		    sequence);
    }

//  --------------------------------------------------------------------------
//  Send the HEAD to a router socket in one step

    public static void sendHead (
        Socket output,
	ZFrame routingId,
        String subject,
        String address,
        long sequence)
    {
        DafkaProto self = new DafkaProto (DafkaProto.HEAD);
        if (routingId != null)
        {
	        self.setRoutingId (routingId);
        }
        self.setSubject (subject);
        self.setAddress (address);
        self.setSequence (sequence);
        self.send (output);
    }

//  --------------------------------------------------------------------------
//  Send the DIRECT_HEAD to the socket in one step

    public static void sendDirect_Head (
        Socket output,
        String subject,
        String address,
        long sequence)
    {
	sendDirect_Head (
		    output,
		    null,
		    subject,
		    address,
		    sequence);
    }

//  --------------------------------------------------------------------------
//  Send the DIRECT_HEAD to a router socket in one step

    public static void sendDirect_Head (
        Socket output,
	ZFrame routingId,
        String subject,
        String address,
        long sequence)
    {
        DafkaProto self = new DafkaProto (DafkaProto.DIRECT_HEAD);
        if (routingId != null)
        {
	        self.setRoutingId (routingId);
        }
        self.setSubject (subject);
        self.setAddress (address);
        self.setSequence (sequence);
        self.send (output);
    }

//  --------------------------------------------------------------------------
//  Send the GET_HEADS to the socket in one step

    public static void sendGet_Heads (
        Socket output,
        String address)
    {
	sendGet_Heads (
		    output,
		    null,
		    address);
    }

//  --------------------------------------------------------------------------
//  Send the GET_HEADS to a router socket in one step

    public static void sendGet_Heads (
        Socket output,
	ZFrame routingId,
        String address)
    {
        DafkaProto self = new DafkaProto (DafkaProto.GET_HEADS);
        if (routingId != null)
        {
	        self.setRoutingId (routingId);
        }
        self.setAddress (address);
        self.send (output);
    }


    //  --------------------------------------------------------------------------
    //  Duplicate the DafkaProto message

    public DafkaProto dup ()
    {
        DafkaProto copy = new DafkaProto (this.id);
        if (this.routingId != null)
            copy.routingId = this.routingId.duplicate ();
        switch (this.id) {
        case MSG:
            {
            copy.subject = this.subject;
            copy.address = this.address;
            copy.sequence = this.sequence;
            copy.content = this.content.duplicate ();
        }
        break;
        case DIRECT_MSG:
            {
            copy.subject = this.subject;
            copy.address = this.address;
            copy.sequence = this.sequence;
            copy.content = this.content.duplicate ();
        }
        break;
        case FETCH:
            {
            copy.subject = this.subject;
            copy.sequence = this.sequence;
            copy.count = this.count;
            copy.address = this.address;
        }
        break;
        case ACK:
            {
            copy.subject = this.subject;
            copy.sequence = this.sequence;
        }
        break;
        case HEAD:
            {
            copy.subject = this.subject;
            copy.address = this.address;
            copy.sequence = this.sequence;
        }
        break;
        case DIRECT_HEAD:
            {
            copy.subject = this.subject;
            copy.address = this.address;
            copy.sequence = this.sequence;
        }
        break;
        case GET_HEADS:
            {
            copy.address = this.address;
        }
        break;
        }
        return copy;
    }


    //  --------------------------------------------------------------------------
    //  Print contents of message to stdout

    public void dump ()
    {
        switch (id) {
        case MSG:
            {
            System.out.println ("MSG:");
            System.out.printf ("    topic=%s\n", topic);
            if (subject != null)
                System.out.printf ("    subject='%s'\n", subject);
            else
                System.out.printf ("    subject=\n");
            if (address != null)
                System.out.printf ("    address='%s'\n", address);
            else
                System.out.printf ("    address=\n");
            System.out.printf ("    sequence=%d\n", (long)sequence);
            System.out.printf ("    content={\n");
            if (content != null) {
                int size = content.size ();
                byte [] data = content.getData ();
                System.out.printf ("        size=%d\n", content.size ());
                if (size > 32)
                    size = 32;
                int contentIndex;
                for (contentIndex = 0; contentIndex < size; contentIndex++) {
                    if (contentIndex != 0 && (contentIndex % 4 == 0))
                        System.out.printf ("-");
                    System.out.printf ("%02X", data [contentIndex]);
                }
            }
            System.out.printf ("    }\n");
            }
            break;

        case DIRECT_MSG:
            {
            System.out.println ("DIRECT_MSG:");
            System.out.printf ("    topic=%s\n", topic);
            if (subject != null)
                System.out.printf ("    subject='%s'\n", subject);
            else
                System.out.printf ("    subject=\n");
            if (address != null)
                System.out.printf ("    address='%s'\n", address);
            else
                System.out.printf ("    address=\n");
            System.out.printf ("    sequence=%d\n", (long)sequence);
            System.out.printf ("    content={\n");
            if (content != null) {
                int size = content.size ();
                byte [] data = content.getData ();
                System.out.printf ("        size=%d\n", content.size ());
                if (size > 32)
                    size = 32;
                int contentIndex;
                for (contentIndex = 0; contentIndex < size; contentIndex++) {
                    if (contentIndex != 0 && (contentIndex % 4 == 0))
                        System.out.printf ("-");
                    System.out.printf ("%02X", data [contentIndex]);
                }
            }
            System.out.printf ("    }\n");
            }
            break;

        case FETCH:
            {
            System.out.println ("FETCH:");
            System.out.printf ("    topic=%s\n", topic);
            if (subject != null)
                System.out.printf ("    subject='%s'\n", subject);
            else
                System.out.printf ("    subject=\n");
            System.out.printf ("    sequence=%d\n", (long)sequence);
            System.out.printf ("    count=%d\n", (long)count);
            if (address != null)
                System.out.printf ("    address='%s'\n", address);
            else
                System.out.printf ("    address=\n");
            }
            break;

        case ACK:
            {
            System.out.println ("ACK:");
            System.out.printf ("    topic=%s\n", topic);
            if (subject != null)
                System.out.printf ("    subject='%s'\n", subject);
            else
                System.out.printf ("    subject=\n");
            System.out.printf ("    sequence=%d\n", (long)sequence);
            }
            break;

        case HEAD:
            {
            System.out.println ("HEAD:");
            System.out.printf ("    topic=%s\n", topic);
            if (subject != null)
                System.out.printf ("    subject='%s'\n", subject);
            else
                System.out.printf ("    subject=\n");
            if (address != null)
                System.out.printf ("    address='%s'\n", address);
            else
                System.out.printf ("    address=\n");
            System.out.printf ("    sequence=%d\n", (long)sequence);
            }
            break;

        case DIRECT_HEAD:
            {
            System.out.println ("DIRECT_HEAD:");
            System.out.printf ("    topic=%s\n", topic);
            if (subject != null)
                System.out.printf ("    subject='%s'\n", subject);
            else
                System.out.printf ("    subject=\n");
            if (address != null)
                System.out.printf ("    address='%s'\n", address);
            else
                System.out.printf ("    address=\n");
            System.out.printf ("    sequence=%d\n", (long)sequence);
            }
            break;

        case GET_HEADS:
            {
            System.out.println ("GET_HEADS:");
            System.out.printf ("    topic=%s\n", topic);
            if (address != null)
                System.out.printf ("    address='%s'\n", address);
            else
                System.out.printf ("    address=\n");
            }
            break;

        }
    }


    //  --------------------------------------------------------------------------
    //  Get/set the message routing id

    public ZFrame routingId ()
    {
        return routingId;
    }

    public void setRoutingId (ZFrame routingId)
    {
        if (this.routingId != null)
            this.routingId.destroy ();
        this.routingId = routingId.duplicate ();
    }


    //  --------------------------------------------------------------------------
    //  Get/set the dafka_proto id

    public char id ()
    {
        return id;
    }

    public void setId (char id)
    {
        this.id = id;
    }

    //  --------------------------------------------------------------------------
    //  Get/set the subject field

    public String subject ()
    {
        return subject;
    }

    public void setSubject (String format, Object ... args)
    {
        //  Format into newly allocated string
        subject = String.format (format, args);
    }


    //  --------------------------------------------------------------------------
    //  Get/set the address field

    public String address ()
    {
        return address;
    }

    public void setAddress (String format, Object ... args)
    {
        //  Format into newly allocated string
        address = String.format (format, args);
    }


    //  --------------------------------------------------------------------------
    //  Get/set the sequence field

    public long sequence ()
    {
        return sequence;
    }

    public void setSequence (long sequence)
    {
        this.sequence = sequence;
    }


    //  --------------------------------------------------------------------------
    //  Get/set the content field

    public ZFrame content ()
    {
        return content;
    }

    //  Takes ownership of supplied frame
    public void setContent (ZFrame frame)
    {
        if (content != null)
            content.destroy ();
        content = frame;
    }

    //  --------------------------------------------------------------------------
    //  Get/set the count field

    public long count ()
    {
        return count;
    }

    public void setCount (long count)
    {
        this.count = count;
    }


    //  Get/set the topic of the message for publishing over pub/sub
    public String topic()
    {
        return topic;
    }

    public void setTopic(String topic)
    {
        this.topic = topic;
    }

    //  Subscribe a socket to a specific message id and a topic.
    public static void subscribe(Socket sub, char id, final String topic)
    {
        sub.subscribe(id + topic);
    }

    //  Unsubscribe a socket from a specific message id and a topic.
    public static void unsubscribe (Socket sub, char id, final String topic)
    {
        sub.unsubscribe(id + topic);
    }

    //  Get the type of subscription received from a XPUB socket
    public boolean isSubscribe ()
    {
        return this.isSubscribe;
    }

}
