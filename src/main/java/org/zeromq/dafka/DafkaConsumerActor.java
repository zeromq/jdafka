package org.zeromq.dafka;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.*;
import org.zeromq.ZMQ.Socket;
import org.zproto.DafkaProto;

import java.util.*;

import static org.zeromq.ZActor.SimpleActor;

/**
 * TODO: Send EARLIEST message when connecting to store
 */
class DafkaConsumerActor extends SimpleActor
{

    private static final Logger log = LogManager.getLogger(DafkaConsumerActor.class);

    private Socket      consumerSub;
    private Socket      consumerPub;
    private DafkaBeacon beacon;
    private ZActor      beaconActor;

    private final DafkaProto fetchMsg;
    private final DafkaProto getHeadsMsg;
    private final DafkaProto helloMsg;

    private final Map<String, Long> sequenceIndex;
    private final List<String>      topics;

    private boolean resetLatest;

    public DafkaConsumerActor()
    {
        this.fetchMsg = new DafkaProto(DafkaProto.FETCH);
        this.getHeadsMsg = new DafkaProto(DafkaProto.GET_HEADS);
        this.helloMsg = new DafkaProto(DafkaProto.CONSUMER_HELLO);
        this.sequenceIndex = new HashMap<>();
        this.topics = new ArrayList<>();
        this.resetLatest = true;
        this.beacon = new DafkaBeacon();
    }

    @Override
    public List<Socket> createSockets(ZContext ctx, Object... args)
    {
        Properties properties = (Properties) args[0];
        if (StringUtils.equals(properties.getProperty("consumer.offset.reset"), "earliest")) {
            this.resetLatest = false;
        }

        this.beaconActor = new ZActor(ctx, this.beacon, null, args);
        this.beaconActor.recv(); // Wait for signal that beacon is connected to tower

        consumerSub = ctx.createSocket(SocketType.SUB);
        consumerPub = ctx.createSocket(SocketType.PUB);
        assert consumerSub != null;
        return Arrays.asList(consumerSub, beaconActor.pipe());
    }

    @Override
    public void start(Socket pipe, List<Socket> sockets, ZPoller poller)
    {
        String consumerAddress = UUID.randomUUID().toString();

        int port = consumerPub.bindToRandomPort("tcp://*");

        beacon.start(beaconActor, consumerAddress, port);
        boolean rc = poller.register(beaconActor.pipe(), ZPoller.IN);
        assert rc == true;

        fetchMsg.setAddress(consumerAddress);
        getHeadsMsg.setAddress(consumerAddress);
        helloMsg.setAddress(consumerAddress);

        DafkaProto.subscribe(consumerSub, DafkaProto.DIRECT_MSG, consumerAddress);
        DafkaProto.subscribe(consumerSub, DafkaProto.DIRECT_HEAD, consumerAddress);
        DafkaProto.subscribe(consumerSub, DafkaProto.STORE_HELLO, consumerAddress);

        rc = poller.register(consumerSub, ZPoller.IN);
        pipe.send(new byte[]{0});
        assert rc == true;
        log.info("Consumer started...");
    }

    @Override
    public boolean finished(Socket pipe)
    {
        beacon.terminate(beaconActor);
        log.info("Consumer stopped!");
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

                boolean rc = consumerSub.connect(address);
                assert rc;
            } else if ("DISCONNECT".equals(command)) {
                log.info("Disconnecting from {}", address);
                consumerSub.disconnect(address);
            } else {
                log.error("Transport: Unknown command {}", command);
                assert (false);
            }
        } else if (socket.equals(consumerSub)) {
            DafkaProto consumerMsg = DafkaProto.recv(consumerSub);
            if (consumerMsg == null) {
                return true;    // Interrupted!
            }
            final char   id              = consumerMsg.id();
            final String address         = consumerMsg.address();
            final String subject         = consumerMsg.subject();
            final long   currentSequence = consumerMsg.sequence();
            final ZFrame content         = consumerMsg.content();

            log.debug(
                    "Received message {} from {} on subject {} with sequence {}",
                    id,
                    address,
                    subject,
                    currentSequence);

            String sequenceKey = subject + "/" + address;

            Long    lastKnownSequence = -1L;
            boolean lastSequenceKnown = sequenceIndex.containsKey(sequenceKey);
            if (lastSequenceKnown) {
                lastKnownSequence = sequenceIndex.get(sequenceKey);
            }

            switch (id) {
                case DafkaProto.MSG:
                case DafkaProto.DIRECT_MSG:
                    if (!lastSequenceKnown) {
                        if (resetLatest) {
                            log.debug(
                                    "Setting offset for topic {} on partition {} to latest {}",
                                    subject,
                                    address,
                                    currentSequence - 1);
                            // Set to latest - 1 in order to process the current message
                            lastKnownSequence = currentSequence - 1;
                            sequenceIndex.put(sequenceKey, lastKnownSequence);
                        } else {
                            log.debug(
                                    "Setting offset for topic {} on partition {} to earliest {}",
                                    subject,
                                    address,
                                    lastKnownSequence);
                            sequenceIndex.put(sequenceKey, lastKnownSequence);
                        }
                    }

                    //  Check if we missed some messages
                    if (currentSequence > lastKnownSequence + 1) {
                        sendFetch(address, subject, currentSequence, lastKnownSequence);
                    }

                    if (currentSequence == lastKnownSequence + 1) {
                        log.debug("Send message {} to client", currentSequence);

                        sequenceIndex.put(sequenceKey, currentSequence);

                        pipe.send(subject, ZMQ.SNDMORE);
                        pipe.send(address, ZMQ.SNDMORE);
                        pipe.send(content.getData());
                    }
                    break;
                case DafkaProto.HEAD:
                case DafkaProto.DIRECT_HEAD:
                    if (!lastSequenceKnown) {
                        if (resetLatest) {
                            log.debug(
                                    "Setting offset for topic {} on partition {} to latest {}",
                                    subject,
                                    address,
                                    currentSequence);

                            // Set to latest in order to skip fetching older messages
                            lastKnownSequence = currentSequence;
                            sequenceIndex.put(sequenceKey, lastKnownSequence);
                            lastSequenceKnown = true;
                        }
                    }

                    //  Check if we missed some messages
                    if (!lastSequenceKnown || currentSequence > lastKnownSequence) {
                        sendFetch(address, subject, currentSequence, lastKnownSequence);
                    }
                    break;
                case DafkaProto.STORE_HELLO:
                    String storeAddress = consumerMsg.address();

                    log.info("Consumer: Consumer is connected to store {}", storeAddress);

                    sendConsumerHelloMsg(storeAddress);
                    break;
                default:
                    log.warn("Unknown message type {}", id);
                    return true;     // Unexpected message id
            }
        }
        return true;
    }

    private void sendGetHeadsMsg(final String topic)
    {
        log.debug("Consumer: Send EARLIEST message for topic {}", topic);

        getHeadsMsg.setTopic(topic);
        getHeadsMsg.send(consumerPub);
    }

    private void sendFetch(String address, String subject, long currentSequence, Long lastKnownSequence)
    {
        long noOfMissedMessages = currentSequence - lastKnownSequence;
        log.debug(
                "FETCHING {} messages on subject {} from {} starting at sequence {}",
                noOfMissedMessages,
                subject,
                address,
                lastKnownSequence + 1);

        fetchMsg.setTopic(address);
        fetchMsg.setSubject(subject);
        fetchMsg.setSequence(lastKnownSequence + 1);
        fetchMsg.setCount(noOfMissedMessages);
        fetchMsg.send(consumerPub);
    }

    private void sendConsumerHelloMsg(String storeAddress)
    {
        List<String> topics;

        if (!this.resetLatest) {
            topics = new ArrayList<>(this.topics);
        } else {
            topics = new ArrayList<>();
        }

        this.helloMsg.setSubjects(topics);
        this.helloMsg.setTopic(storeAddress);
        this.helloMsg.send(this.consumerPub);
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

    public void subscribe(String topic)
    {
        log.debug("Subscribe to topic {}", topic);
        DafkaProto.subscribe(consumerSub, DafkaProto.MSG, topic);
        DafkaProto.subscribe(consumerSub, DafkaProto.HEAD, topic);

        if (!this.resetLatest) {
            sendGetHeadsMsg(topic);
        }

        topics.add(topic);
    }

}
