package org.zeromq.dafka;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.Snapshot;
import org.zeromq.SocketType;
import org.zeromq.ZActor;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZPoller;
import org.zproto.DafkaProto;
import zmq.ZMQ;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.zeromq.ZActor.SimpleActor;

public class DafkaStoreReader extends SimpleActor
{

    private static final Logger log = LogManager.getLogger(DafkaStoreReader.class);

    private DB db;

    private String address;
    private String writerAddress;

    private Socket      publisher;
    private Socket      subscriber;
    private DafkaBeacon beacon;
    private ZActor      beaconActor;

    private DafkaProto   incomingMsg;
    private DafkaProto   outgoingMsg;
    private DafkaMsgKey  firstKey;
    private DafkaMsgKey  iterKey;
    private DafkaMsgKey  lastKey;
    private DafkaHeadKey headKey;

    public DafkaStoreReader(DB db)
    {
        this.address = UUID.randomUUID().toString();

        this.db = db;

        this.beacon = new DafkaBeacon();

        this.outgoingMsg = new DafkaProto('A');

        this.firstKey = new DafkaMsgKey();
        this.iterKey = new DafkaMsgKey();
        this.lastKey = new DafkaMsgKey();
        this.headKey = new DafkaHeadKey();
    }

    @Override
    public List<Socket> createSockets(ZContext ctx, Object... args)
    {
        Properties properties = (Properties) args[0];
        writerAddress = properties.getProperty("store.writerAddress");

        beaconActor = new ZActor(ctx, this.beacon, null, properties);
        beaconActor.recv(); // Wait for signal that beacon is connected to tower

        //  Create and bind publisher
        publisher = ctx.createSocket(SocketType.XPUB);
        publisher.setXpubVerbose(true);

        //  Create the subscriber and subscribe for fetch & get heads messages
        subscriber = ctx.createSocket(SocketType.SUB);

        return Arrays.asList(subscriber, publisher, beaconActor.pipe());
    }

    @Override
    public void start(Socket pipe, List<Socket> sockets, ZPoller poller)
    {
        int port = publisher.bindToRandomPort("tcp://*");

        beacon.start(beaconActor, address, port);

        DafkaProto.subscribe(subscriber, DafkaProto.FETCH, "");
        DafkaProto.subscribe(subscriber, DafkaProto.GET_HEADS, "");
        DafkaProto.subscribe(subscriber, DafkaProto.CONSUMER_HELLO, address);

        poller.register(beaconActor.pipe(), ZPoller.IN);
        poller.register(subscriber, ZPoller.IN);
        poller.register(publisher, ZPoller.IN);

        pipe.send(new byte[] { 0 });
        log.info("Store reader started...");
    }

    @Override
    public boolean finished(Socket pipe)
    {
        beacon.terminate(beaconActor);
        log.info("Store writer stopped!");
        return super.finished(pipe);
    }

    @Override
    public boolean stage(Socket socket, Socket pipe, ZPoller poller, int events)
    {
        if (socket.equals(beaconActor.pipe())) {
            String command = socket.recvStr();
            String address = socket.recvStr();

            if ("CONNECT".equals(command)) {
                log.info("Connecting to {}", address);
                subscriber.connect(address);
            }
            else if ("DISCONNECT".equals(command)) {
                log.info("Disconnecting from {}", address);
                subscriber.disconnect(address);
            }
            else {
                log.error("Transport: Unknown command {}", command);
                assert (false);
            }
        }
        else if (socket.equals(subscriber)) {
            DafkaProto incomingMsg = DafkaProto.recv(subscriber);
            if (incomingMsg == null) {
                return true;
            }

            String sender = incomingMsg.address();

            // Ignore messages from the write actor
            if (StringUtils.equals(sender, writerAddress)) {
                return true;
            }

            try (Snapshot snapshot = db.getSnapshot()) {
                ReadOptions roptions = new ReadOptions();
                roptions.snapshot(snapshot);
                final DBIterator iter = db.iterator(roptions);

                switch (incomingMsg.id()) {
                case DafkaProto.FETCH: {
                    String subject = incomingMsg.subject();
                    String address = incomingMsg.topic();
                    long sequence = incomingMsg.sequence();
                    long count = incomingMsg.count();

                    //  Search the first message
                    firstKey.setKey(subject, address, sequence);
                    iter.seek(firstKey.encode());

                    if (iter.peekNext() == null) {
                        log.info(
                                "No answer for consumer. Subject: {}, Address: {}, Seq: {}",
                                subject,
                                address,
                                sequence);
                        return true;
                    }

                    // For now we only sending from what the user requested, so if the sequence at the
                    // beginning doesn't match, don't send anything
                    // TODO: mark the first message as tail
                    iterKey.decode(iter.peekNext().getKey());
                    if (!firstKey.equals(iterKey)) {
                        log.info(
                                "No answer for consumer. Subject: {}, Address: {}, Seq: {}",
                                subject,
                                address,
                                sequence);
                        return false;
                    }

                    lastKey.setKey(subject, address, sequence + count);

                    outgoingMsg.setId(DafkaProto.DIRECT_MSG);
                    outgoingMsg.setTopic(sender);
                    outgoingMsg.setSubject(subject);
                    outgoingMsg.setAddress(address);

                    while (iter.hasNext() && iterKey.compareTo(lastKey) <= 0) {
                        final byte[] content = iter.next().getValue();
                        ZFrame frame = new ZFrame(content);

                        long iterSequence = iterKey.getSequence();

                        outgoingMsg.setSequence(iterSequence);
                        outgoingMsg.setContent(frame);
                        outgoingMsg.send(publisher);

                        log.info(
                                "Found answer for consumer. Subject: {}, Partition: {}, Seq: {}",
                                subject,
                                address,
                                iterSequence);

                        if (iter.hasNext()) {
                            //  Get the next key, if it not a valid key (different table), break the loop
                            if (!iterKey.decode(iter.peekNext().getKey())) {
                                break;
                            }
                        }
                    }
                    break;
                }
                case DafkaProto.CONSUMER_HELLO: {
                    final List<String> subjects = incomingMsg.subjects();

                    for (String subject : subjects) {
                        send_heads(iter, sender, subject);
                    }

                    break;
                }
                case DafkaProto.GET_HEADS: {
                    String subject = incomingMsg.topic();
                    send_heads(iter, sender, subject);
                    break;
                }
                default:
                    assert (false);
                }
            }
            catch (IOException exception) {
                log.error("Failure accessing/closing levelDB snapshot", exception);
            }
        }
        else if (socket.equals(publisher)) {
            DafkaProto incomingMsg = DafkaProto.recv(publisher);
            if (incomingMsg == null) {
                return true;
            }

            String consumerAddress = incomingMsg.topic();

            if (DafkaProto.STORE_HELLO == incomingMsg.id() && incomingMsg.isSubscribe()) {
                log.info("Consumer {} connected", consumerAddress);

                outgoingMsg.setId(DafkaProto.STORE_HELLO);
                outgoingMsg.setAddress(address);
                outgoingMsg.setTopic(consumerAddress);
                outgoingMsg.send(publisher);
            }
        }
        return true;
    }

    private void send_heads(DBIterator iter, String sender, String subject)
    {
        headKey.set(subject, "");
        iter.seek(headKey.encode());

        if (!iter.hasNext()) {
            log.info("No heads for subject {}", subject);
            return;
        }

        outgoingMsg.setId(DafkaProto.DIRECT_HEAD);
        outgoingMsg.setTopic(sender);

        boolean rc = headKey.decode(iter.peekNext().getKey());
        while (rc && StringUtils.startsWith(subject, headKey.getSubject())) {
            final byte[] sequenceBytes = iter.peekNext().getValue();
            long sequence = ByteUtils.bytesToLong(sequenceBytes);

            outgoingMsg.setSubject(headKey.getSubject());
            outgoingMsg.setAddress(headKey.getAddress());
            outgoingMsg.setSequence(sequence);
            outgoingMsg.send(publisher);

            log.info("Found head answer to consumer {} {} {}", headKey.getSubject(), headKey.getAddress(), sequence);

            if (!iter.hasNext()) {
                break;
            }
            iter.next();

            rc = headKey.decode(iter.peekNext().getKey());
        }

    }

    @Override
    public boolean backstage(Socket pipe, ZPoller poller, int events)
    {
        String command = pipe.recvStr();
        switch (command) {
        case "$TERM":
            return false;
        default:
            log.error("Invalid command {}", command);
        }
        return true;
    }

    public void terminate(ZActor actor)
    {
        actor.send("$TERM");
    }

    private boolean hasIn(Socket socket)
    {
        return (socket.getEvents() & ZMQ.ZMQ_POLLIN) != 0;
    }

}
