package org.zeromq.dafka;

import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.zeromq.ZActor;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ.Socket;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Properties;

public class DafkaProducer
{

    private static final Logger log = LogManager.getLogger(DafkaProducer.class);

    private final DafkaProducerActor dafkaProducer;
    private final ZActor             actor;

    public DafkaProducer(ZContext context, Properties properties)
    {
        dafkaProducer = new DafkaProducerActor();
        actor = new ZActor(context, dafkaProducer, null, Arrays.asList("HELLO", properties).toArray());
        // Wait until actor is ready
        Socket pipe   = actor.pipe();
        byte[] signal = pipe.recv();
        assert signal[0] == 0;
    }

    private void publish(ZFrame content)
    {
        dafkaProducer.publish(content);
    }

    public void terminate()
    {
        actor.send("$TERM");
    }

    public static void main(String[] args) throws ParseException
    {
        Properties consumerProperties = new Properties();
        Options    options            = new Options();
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

            if (cmd.hasOption("pub")) {
                consumerProperties.setProperty("beacon.pub_address", cmd.getOptionValue("pub"));
            }
            if (cmd.hasOption("sub")) {
                consumerProperties.setProperty("beacon.sub_address", cmd.getOptionValue("sub"));
            }
        } catch (UnrecognizedOptionException exception) {
            System.out.println(exception.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("dafka_console_producer", options);
            return;
        }

        ZContext      context       = new ZContext();
        DafkaProducer dafkaProducer = new DafkaProducer(context, consumerProperties);

        final Thread zmqThread = new Thread(() -> {
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    if (reader.ready()) {
                        String line = reader.readLine();
                        if (StringUtils.isNoneBlank(line)) {
                            ZFrame content = new ZFrame(line);
                            dafkaProducer.publish(content);
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Interrupted! Stopping dafka_console_producer.");
            dafkaProducer.terminate();
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
