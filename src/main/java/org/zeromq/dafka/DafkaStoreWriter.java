package org.zeromq.dafka;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;
import org.zeromq.SocketType;
import org.zeromq.ZActor;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZPoller;
import org.zproto.DafkaProto;
import zmq.ZMQ;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;

import static org.zeromq.ZActor.SimpleActor;

public class DafkaStoreWriter extends SimpleActor
{

    private static final Logger log = LogManager.getLogger(DafkaStoreWriter.class);

    private String address;

    private       DB           db;
    private final WriteOptions wOptions;
    private final ReadOptions  rOptions;

    private Socket      publisher;
    private Socket      directSubscriber;
    private Socket      msgSubscriber;
    private DafkaBeacon beacon;
    private ZActor      beaconActor;

    private DafkaProto incomingMsg;
    private DafkaProto outgoingMsg;

    private DafkaHeadKey headKey;
    private DafkaMsgKey  msgKey;

    private Map<DafkaHeadKey, Long> heads;

    public DafkaStoreWriter(DB db)
    {
        this.db = db;
        this.wOptions = new WriteOptions();
        this.rOptions = new ReadOptions();

        this.beacon = new DafkaBeacon();
    }

    @Override
    public List<Socket> createSockets(ZContext ctx, Object... args)
    {
        Properties properties = (Properties) args[0];
        address = properties.getProperty("store.writerAddress");

        beaconActor = new ZActor(ctx, this.beacon, null, properties);
        beaconActor.recv(); // Wait for signal that beacon is connected to tower

        //  Create and bind publisher
        publisher = ctx.createSocket(SocketType.PUB);

        // We are using different subscribers for DIRECT and MSG in order to
        // give the DIRECT messages an higher priority

        // Create the direct subscriber and subscribe to direct messaging
        directSubscriber = ctx.createSocket(SocketType.SUB);
        directSubscriber.setRcvHWM(100000); // TODO: should be configurable, now it is the same is fetch max count

        //  Create the msg subscriber and subscribe to msg and head
        msgSubscriber = ctx.createSocket(SocketType.SUB);
        msgSubscriber.setRcvHWM(100000); // TODO: should be configurable

        //  Create the messages
        incomingMsg = new DafkaProto('X');
        msgKey = new DafkaMsgKey();
        headKey = new DafkaHeadKey();

        // Create cache of the heads
        heads = new HashMap<>();

        return Arrays.asList(msgSubscriber, directSubscriber, beaconActor.pipe());
    }

    @Override
    public void start(Socket pipe, List<Socket> sockets, ZPoller poller)
    {
        int port = publisher.bindToRandomPort("tcp://*");

        beacon.start(beaconActor, address, port);

        DafkaProto.subscribe(directSubscriber, DafkaProto.DIRECT_MSG, this.address);

        DafkaProto.subscribe(msgSubscriber, DafkaProto.MSG, "");
        DafkaProto.subscribe(msgSubscriber, DafkaProto.HEAD, "");

        poller.register(beaconActor.pipe(), ZPoller.IN);
        poller.register(msgSubscriber, ZPoller.IN);
        poller.register(directSubscriber, ZPoller.IN);
        poller.register(publisher, ZPoller.OUT);

        pipe.send(new byte[] { 0 });
        log.info("Store writer started...");
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
                directSubscriber.connect(address);
                msgSubscriber.connect(address);
            }
            else if ("DISCONNECT".equals(command)) {
                log.info("Disconnecting from {}", address);
                directSubscriber.disconnect(address);
                msgSubscriber.disconnect(address);
            }
            else {
                log.error("Transport: Unknown command {}", command);
                assert (false);
            }
        }
        else if (socket.equals(directSubscriber) || socket.equals(msgSubscriber)) {
            int batch_size = 0;
            WriteBatch batch = db.createWriteBatch();
            Map<DafkaHeadKey, DafkaProto> acks = new HashMap<>();
            Map<DafkaHeadKey, DafkaProto> fetches = new HashMap<>();

            while ((batch_size < 100000 && (hasIn(directSubscriber) || hasIn(msgSubscriber)))) {
                DafkaProto incomingMsg;

                // We always check the DIRECT subscriber first, this is how we gave it a priority over the MSG subscriber
                if (hasIn(directSubscriber)) {
                    incomingMsg = DafkaProto.recv(directSubscriber);
                }
                else {
                    incomingMsg = DafkaProto.recv(msgSubscriber);
                }

                if (incomingMsg == null) {
                    break;
                }

                switch (incomingMsg.id()) {
                case DafkaProto.HEAD: {
                    final String subject = incomingMsg.subject();
                    final String address = incomingMsg.address();
                    final long sequence = incomingMsg.sequence();

                    headKey.set(subject, address);

                    Long head = getHead();

                    if (head == null) {
                        long count = sequence + 1;

                        addFetch(fetches, 0, count);
                    }
                    else if (head < sequence) {
                        long count = sequence - head;

                        addFetch(fetches, head + 1, count);
                    }
                    break;
                }
                case DafkaProto.DIRECT_MSG:
                case DafkaProto.MSG: {
                    final String subject = incomingMsg.subject();
                    final String address = incomingMsg.address();
                    final long sequence = incomingMsg.sequence();
                    ZFrame content = incomingMsg.content();

                    headKey.set(subject, address);
                    Long head = getHead();

                    if (head != null && sequence <= head) {
                        log.debug(
                                "Head at {} dropping already received message {} {} {}",
                                head,
                                subject,
                                address,
                                sequence);
                    }
                    else if (head == null && sequence != 0) {
                        long count = sequence + 1;
                        addFetch(fetches, 0, count);
                    }
                    else if (head != null && head + 1 != sequence) {
                        long count = (sequence - head);
                        addFetch(fetches, head + 1, count);
                    }
                    else {
                        // Saving msg to db
                        msgKey.setKey(subject, address, sequence);
                        byte[] msgKeyBytes = msgKey.encode();
                        batch.put(msgKeyBytes, content.getData());

                        // update the head
                        byte[] headKeyBytes = headKey.encode();
                        // TODO; we might be override it multiple times in a batch
                        batch.put(headKeyBytes, ByteUtils.longToBytes(sequence));
                        batch_size++;

                        // Add the ack message
                        addAck(acks, sequence);

                        // Update the head in the heads hash
                        heads.put(headKey, sequence);
                    }

                    break;
                }
                default:
                    break;

                }

            }

            try {
                db.write(batch, wOptions);
            }
            catch (DBException exception) {
                log.error("Failed to save batch to db.", exception);
                System.exit(255);
            }

            // Sending the acks now
            for (DafkaProto ackMsg : acks.values()) {
                ackMsg.send(publisher);
                log.info("Acked {} {} {}", ackMsg.subject(), ackMsg.topic(), ackMsg.sequence());
            }

            // Sending the fetches now
            for (Entry<DafkaHeadKey, DafkaProto> fetchEntry : fetches.entrySet()) {
                DafkaProto fetchMsg = fetchEntry.getValue();
                boolean fetch = true;

                // Checking if the fetch is still needed according to the recent head
                Long head = heads.get(fetchEntry.getKey());
                if (head != null) {
                    long sequence = fetchMsg.sequence();
                    long count = fetchMsg.count();

                    if (head >= sequence + count) {
                        fetch = false;
                    }
                    else if (head > sequence) {
                        // We can change the sequence and count
                        fetchMsg.setSequence(head);
                        fetchMsg.setCount(count - head - sequence);
                    }
                }

                if (fetch) {
                    fetchMsg.send(publisher);
                    log.info(
                            "Fetching {} from {} {} {}",
                            fetchMsg.count(),
                            fetchMsg.subject(),
                            fetchMsg.topic(),
                            fetchMsg.sequence());
                }
            }

            log.info("Saved batch of {}", batch_size);
        }
        return true;
    }

    private Long getHead()
    {
        Long sequence = heads.get(headKey);
        if (sequence != null) {
            return sequence;
        }

        byte[] keyBytes = headKey.encode();

        try {
            final byte[] valueBytes = db.get(keyBytes, rOptions);

            if (valueBytes == null) {
                return null;
            }

            return ByteUtils.bytesToLong(valueBytes);
        }
        catch (DBException exception) {
            log.error("Failed to get head from db {}", exception.getMessage());
            System.exit(255);
            return null;
        }
    }

    private void addAck(Map<DafkaHeadKey, DafkaProto> acks, long sequence)
    {
        DafkaProto msg = acks.get(headKey);

        if (msg == null) {
            msg = new DafkaProto(DafkaProto.ACK);

            msg.setTopic(headKey.getAddress());
            msg.setSubject(headKey.getSubject());
            acks.put(headKey, msg);
        }

        msg.setSequence(sequence);
    }

    private void addFetch(Map<DafkaHeadKey, DafkaProto> fetches, long sequence, long count)
    {
        DafkaProto msg = fetches.get(headKey);

        if (msg == null) {
            msg = new DafkaProto(DafkaProto.FETCH);
            msg.setTopic(headKey.getAddress());
            msg.setSubject(headKey.getSubject());
            msg.setAddress(address);
            fetches.put(headKey, msg);
        }

        if (count > 100000) {
            count = 100000;
        }

        msg.setSequence(sequence);
        msg.setCount(count);
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
