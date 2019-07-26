package org.zeromq.dafka;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.zeromq.SocketType;
import org.zeromq.ZActor;
import org.zeromq.ZActor.SimpleActor;
import org.zeromq.ZContext;
import org.zeromq.ZMQException;
import org.zeromq.ZPoller;
import org.zeromq.ZTimer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import static org.zeromq.ZMQ.Socket;
import static org.zeromq.ZTimer.Timer;

public class DafkaBeacon extends SimpleActor
{

    private static final Logger log = LogManager.getLogger(DafkaBeacon.class);

    private Thread  timerThread;
    private boolean timerThreadRunning;
    private ZTimer  ztimer;
    private Timer   broadcastTimer;
    private Timer   reaperTimer;

    private boolean           connected;
    private Map<String, Long> peers;

    private String sender;
    private String address;
    private int    port;

    private int    beaconTimeout;
    private int    interval;
    private String towerPubAddress;
    private String towerSubAddress;

    private Socket pub;
    private Socket sub;

    public DafkaBeacon()
    {
        this.ztimer = new ZTimer();
        timerThread = new Thread(() -> {
            while (timerThreadRunning)
                ztimer.sleepAndExecute();
        });
        timerThreadRunning = true;
        timerThread.start();

        this.peers = new HashMap<>();
        this.port = -1;

        this.beaconTimeout = 4000;
        this.interval = 1000;

        this.towerSubAddress = "tcp://127.0.0.1:5556";
        this.towerPubAddress = "tcp://127.0.0.1:5557";
        this.address = "";
    }

    @Override
    public List<Socket> createSockets(ZContext ctx, Object... args)
    {
        Properties properties = (Properties) args[0];
        String towerPubAddress = properties.getProperty("beacon.pub_address");
        if (StringUtils.isNoneBlank(towerPubAddress)) {
            this.towerPubAddress = towerPubAddress;
        }
        String towerSubAddress = properties.getProperty("beacon.sub_address");
        if (StringUtils.isNoneBlank(towerSubAddress)) {
            this.towerSubAddress = towerSubAddress;
        }

        // Creating publisher socket
        this.pub = ctx.createSocket(SocketType.PUB);

        // Create Subscriber socket
        this.sub = ctx.createSocket(SocketType.SUB);
        return Collections.singletonList(sub);
    }

    @Override
    public void start(final Socket pipe, List<Socket> sockets, ZPoller poller)
    {
        // Subscribe to welcome message and beacon topic
        sub.subscribe("W");
        sub.subscribe("B");

        //  Register the clear dead peers interval
        reaperTimer = ztimer.add(interval, args -> {
            long now = System.currentTimeMillis();

            Iterator<Entry<String, Long>> it = peers.entrySet().iterator();
            while (it.hasNext()) {
                final Entry<String, Long> peer = it.next();
                long expire = peer.getValue();
                if (now > expire) {
                    String address = peer.getKey();

                    log.debug("Beacon: peer dead {}", address);

                    pipe.sendPicture("ss", "DISCONNECT", address);
                    it.remove();
                }
            }
        });

        // Connect subscriber to tower
        boolean rc = sub.connect(towerPubAddress);
        if (!rc) {
            log.error("Error while connecting subscriber to towers {}", towerPubAddress);
            pipe.send("$TERM");
            return;
        }

        // Connect publisher to tower
        rc = pub.connect(towerSubAddress);
        if (!rc) {
            log.error("Error while connecting subscriber to towers {}", towerPubAddress);
            pipe.send("$TERM");
            return;
        }

        poller.register(sub, ZPoller.IN);
    }

    @Override
    public boolean finished(Socket pipe)
    {
        timerThreadRunning = false;
        synchronized (timerThread) {
            try {
                timerThread.wait();
            }
            catch (InterruptedException exception) {
                log.error("Failed to stop timer thread", exception);
            }
        }
        stopBeacon();
        return true;
    }

    @Override
    public boolean stage(Socket socket, Socket pipe, ZPoller poller, int events)
    {
        String topic = sub.recvStr();

        switch (topic) {
        case "W":
            if (!connected) {
                connected = true;
                //  Signal actor successfully initiated
                pipe.send(new byte[] { 0 });

                log.info("Beacon: connected to tower");
            }
            break;
        case "B":
            final Object[] objects = sub.recvPicture("ss");
            String sender = (String) objects[0];
            String address = (String) objects[1];

            // Drop our own beaconing
            if (sender == null || !this.sender.equals(sender)) {
                Long expire = peers.get(address);

                if (expire == null) {
                    expire = System.currentTimeMillis() + beaconTimeout;
                    peers.put(address, expire);

                    pipe.sendPicture("ss", "CONNECT", address);

                    // New node on the network, sending a beacon immediately
                    if (port != -1) {
                        pub.sendPicture("sssi", "B", this.sender, this.address, this.port);
                    }

                }
                else {
                    expire = System.currentTimeMillis() + beaconTimeout;
                    peers.put(address, expire);
                }
            }
            break;
        }
        return true;
    }

    private void startBeacon(Socket pipe)
    {
        // Cancel old beaconTimer if running
        if (broadcastTimer != null) {
            ztimer.cancel(broadcastTimer);
        }

        try {
            final Object[] messageResult = pipe.recvPicture("si");
            this.sender = (String) messageResult[0];
            this.port = (int) messageResult[1];

            // Enable the beacon beaconTimer
            broadcastTimer = ztimer.add(interval,
                                        args -> pub.sendPicture("sssi", "B", args[0], args[1], args[2]),
                                        sender,
                                        address,
                                        port);

            // Sending the first beacon immediately
            pub.sendPicture("sssi", "B", sender, address, port);
        }
        catch (ZMQException exception) {
            log.error("Beacon: error while receiving start command", exception);
            pipe.send(new byte[] { (byte) 255 });
        }
        log.debug("Beacon: started. port: {} interval: {} uuid: {}", port, interval, sender);
    }

    private void stopBeacon()
    {
        if (broadcastTimer != null) {
            ztimer.cancel(broadcastTimer);
        }
        if (reaperTimer != null) {
            ztimer.cancel(reaperTimer);
        }
        log.info("Beacon stopped!");
    }

    @Override
    public boolean backstage(Socket pipe, ZPoller poller, int events)
    {
        String command = pipe.recvStr();
        switch (command) {
        case "START":
            startBeacon(pipe);
            break;
        case "STOP":
            stopBeacon();
            break;
        case "$TERM":
            return false;
        default:
            log.error("Invalid command {}", command);
        }
        return true;
    }

    public void start(ZActor actor, String consumerName, int port)
    {
        actor.pipe().sendPicture("ssi", "START", consumerName, port);
    }

    public void stop(ZActor actor)
    {
        actor.send("STOP");
    }

    public void terminate(ZActor actor)
    {
        actor.send("$TERM");
    }

}
