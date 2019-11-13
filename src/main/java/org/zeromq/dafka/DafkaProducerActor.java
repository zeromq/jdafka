package org.zeromq.dafka;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.*;
import org.zeromq.ZMQ.Socket;
import org.zproto.DafkaProto;

import java.util.*;

import static org.zeromq.ZActor.SimpleActor;

class DafkaProducerActor extends SimpleActor
{

    private static final Logger log = LogManager.getLogger(DafkaProducerActor.class);

    private Thread       timerThread;
    private boolean      timerThreadRunning;
    private ZTimer       ztimer;
    private ZTimer.Timer headTimer;

    private Socket      socket;
    private Socket      producerSub;
    private DafkaBeacon beacon;
    private ZActor      beaconActor;

    private final DafkaProto msg;       // Reusable MSG message to publish
    private final DafkaProto headMsg;   // Reusable HEAD message to publish

    private long                  lastAckedSequence;
    private Map<Long, DafkaProto> messageCache;
    private long                  headInterval;

    public DafkaProducerActor()
    {
        this.ztimer = new ZTimer();
        timerThread = new Thread(() -> {
            while (timerThreadRunning)
                ztimer.sleepAndExecute();
        });
        timerThreadRunning = true;
        timerThread.start();

        this.beacon = new DafkaBeacon();

        this.msg = new DafkaProto(DafkaProto.MSG);
        this.headMsg = new DafkaProto(DafkaProto.HEAD);
        this.lastAckedSequence = -1;
        this.messageCache = new HashMap<>();
        this.headInterval = 1000;
    }

    @Override
    public List<Socket> createSockets(ZContext ctx, Object... args)
    {
        String     topic      = (String) args[0];
        Properties properties = (Properties) args[1];

        this.beaconActor = new ZActor(ctx, this.beacon, null, args[1]);
        this.beaconActor.recv(); // Wait for signal that beacon is connected to tower

        this.msg.setTopic(topic);
        this.msg.setSubject(topic);
        this.msg.setSequence(-1);

        this.headMsg.setTopic(topic);
        this.headMsg.setSubject(topic);

        socket = ctx.createSocket(SocketType.PUB);
        producerSub = ctx.createSocket(SocketType.SUB);
        assert socket != null;
        return Arrays.asList(producerSub, beaconActor.pipe());
    }

    @Override
    public void start(Socket pipe, List<Socket> sockets, ZPoller poller)
    {
        int port = socket.bindToRandomPort("tcp://*");

        beacon.start(beaconActor, msg.address(), port);
        boolean rc = poller.register(beaconActor.pipe(), ZPoller.IN);
        assert rc == true;

        String producerAddress = UUID.randomUUID().toString();
        this.msg.setAddress(producerAddress);
        this.headMsg.setAddress(producerAddress);

        DafkaProto.subscribe(producerSub, DafkaProto.ACK, producerAddress);
        DafkaProto.subscribe(producerSub, DafkaProto.FETCH, producerAddress);

        rc = poller.register(producerSub, ZPoller.IN);
        assert rc == true;
        pipe.send(new byte[]{0});
        log.info("Producer started...");
    }

    @Override
    public boolean finished(Socket pipe)
    {
        beacon.terminate(beaconActor);
        timerThreadRunning = false;
        synchronized (timerThread) {
            try {
                timerThread.join();
            } catch (InterruptedException exception) {
                log.error("Failed to stop timer thread", exception);
            }
        }
        log.info("Producer stopped!");
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

                boolean rc = this.producerSub.connect(address);
                assert rc;
            } else if ("DISCONNECT".equals(command)) {
                log.info("Disconnecting from {}", address);
                this.producerSub.disconnect(address);
            } else {
                log.error("Transport: Unknown command {}", command);
                assert (false);
            }
        } else if (socket.equals(this.producerSub)) {
            DafkaProto subMsg = DafkaProto.recv(this.producerSub);
            if (subMsg == null) {
                return true;    // Interrupted!
            }

            switch (subMsg.id()) {
                case DafkaProto.ACK:
                    final long ackSequence = subMsg.sequence();
                    log.debug("Received ACK with sequence {}", ackSequence);

                    for (long index = this.lastAckedSequence + 1; index <= ackSequence; index++) {
                        this.messageCache.remove(index);
                    }
                    break;

                case DafkaProto.FETCH:
                    final String subject = subMsg.subject();
                    final String address = subMsg.topic();
                    final long sequence = subMsg.sequence();
                    final long count = subMsg.count();

                    for (long index = 0; index < count; index++) {
                        final long lookupKey = sequence + index;
                        DafkaProto cachedMsg = messageCache.get(lookupKey);
                        if (cachedMsg != null) {
                            log.info(
                                    "Found answer for subscriber. Subject: {}, Partition: {}, Seq: {}",
                                    subject,
                                    address,
                                    sequence + index);
                            cachedMsg.setId(DafkaProto.DIRECT_MSG);
                            cachedMsg.setTopic(subMsg.address());
                            cachedMsg.send(this.socket);
                        } else {
                            break;
                        }
                    }
                    break;
            }
        }
        return true;
    }

    @Override
    public boolean backstage(Socket pipe, ZPoller poller, int events)
    {
        String command = pipe.recvStr();
        switch (command) {
            case "PUBLISH":
                ZFrame content = (ZFrame) pipe.recvBinaryPicture("f")[0];
                publish(content);
                break;
            case "$TERM":
                if (headTimer != null) {
                    ztimer.cancel(headTimer);
                    headTimer = null;
                }
                return false;
            default:
                log.error("Invalid command {}", command);
        }
        return true;
    }

    public boolean publish(ZFrame content)
    {
        final long sequence = msg.sequence() + 1;
        msg.setContent(content);
        msg.setSequence(sequence);
        DafkaProto cacheMsg = msg.dup();    // Duplicate now, because send will clear content

        boolean success = msg.send(socket);

        if (success) {
            log.debug("Send MSG message with sequence {}", msg.sequence());

            messageCache.put(sequence, cacheMsg);

            // Starts the HEAD timer once the first message has been send
            if (sequence == 0) {
                headTimer = ztimer.add(headInterval, args -> {
                    // Send HEAD message
                    headMsg.setSequence(msg.sequence());
                    log.debug("Send HEAD message with sequence {}", headMsg.sequence());
                    headMsg.send(socket);
                }, null);
            }
        }

        return success;
    }

}
