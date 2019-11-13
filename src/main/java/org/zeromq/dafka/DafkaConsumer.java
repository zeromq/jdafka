package org.zeromq.dafka;

import org.apache.commons.cli.*;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.zeromq.ZActor;
import org.zeromq.ZContext;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

import java.util.Arrays;
import java.util.Properties;

import static org.zeromq.ZActor.SimpleActor;

/**
 * TODO: Send EARLIEST message when connecting to store
 */
public class DafkaConsumer extends SimpleActor
{

    private static final Logger log = LogManager.getLogger(DafkaConsumer.class);

    private final DafkaConsumerActor dafkaConsumer;
    private final ZActor             actor;

    public DafkaConsumer(ZContext context, Properties properties)
    {
        dafkaConsumer = new DafkaConsumerActor();
        actor = new ZActor(context, dafkaConsumer, null, Arrays.asList(properties).toArray());
        // Wait until actor is ready
        Socket pipe   = actor.pipe();
        byte[] signal = pipe.recv();
        assert signal[0] == 0;

    }

    public void subscribe(String hello)
    {
        dafkaConsumer.subscribe(hello);
    }

    public DafkaMsg recv()
    {
        ZMsg msg = actor.recv();
        return msg == null ? null : buildDafkaMsg(msg);
    }

    public DafkaMsg recv(int timeout)
    {
        ZMsg msg = actor.recv(timeout);
        return msg == null ? null : buildDafkaMsg(msg);
    }

    public void terminate()
    {
        actor.send("$TERM");
    }

    private DafkaMsg buildDafkaMsg(ZMsg msg)
    {
        String       topic     = msg.popString();
        String       partition = msg.popString();
        final byte[] data      = msg.pop().getData();
        return new DafkaMsg(topic, partition, data);
    }

    public static void main(String[] args) throws InterruptedException, ParseException
    {
        Properties consumerProperties = new Properties();
        Options    options            = new Options();
        options.addOption("from_beginning", "Consume messages from beginning of partition");
        options.addOption("pub", true, "Beacon publisher address");
        options.addOption("sub", true, "Beacon subscriber address");
        options.addOption("verbose", "Enable verbose logging");
        options.addOption("help", "Displays this help");
        CommandLineParser parser = new DefaultParser();
        try {
            final CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption("help")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("dafka_console_consumer", options);
                return;
            }

            if (cmd.hasOption("verbose")) {
                Configurator.setRootLevel(Level.DEBUG);
            } else {
                Configurator.setRootLevel(Level.ERROR);
            }

            if (cmd.hasOption("from_beginning")) {
                consumerProperties.setProperty("consumer.offset.reset", "earliest");
            }
            if (cmd.hasOption("pub")) {
                consumerProperties.setProperty("beacon.pub_address", cmd.getOptionValue("pub"));
            }
            if (cmd.hasOption("sub")) {
                consumerProperties.setProperty("beacon.sub_address", cmd.getOptionValue("sub"));
            }
        } catch (UnrecognizedOptionException exception) {
            System.out.println(exception.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("dafka_console_consumer", options);
            return;
        }

        ZContext      context       = new ZContext();
        DafkaConsumer dafkaConsumer = new DafkaConsumer(context, consumerProperties);

        // Give time until connected to pubs and stores
        Thread.sleep(1000);
        dafkaConsumer.subscribe("HELLO");

        final Thread zmqThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                DafkaMsg msg = dafkaConsumer.recv(100);
                if (msg != null) {
                    System.out.println(msg.getTopic());
                    System.out.println(msg.getPartition());
                    System.out.println(new String(msg.getData()));
                }
            }
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Interrupted! Stopping dafka_console_consumer.");
            dafkaConsumer.terminate();
            context.close();
            try {
                zmqThread.interrupt();
                zmqThread.join();
            } catch (InterruptedException e) {
            }
        }));

        zmqThread.start();
    }

}
