package com.example;


import org.apache.commons.cli.*;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SensorDataProducer {
    private final static Logger logger = LogManager.getLogger(SensorDataProducer.class);


    /**
     *  Define Command Line Arguments
     */
    static Options options = new Options();

    static {
        Option helpOption = new Option(
                SensorDataProducerUtil.CMD_OPTION_HELP_SHORT, SensorDataProducerUtil.CMD_OPTION_HELP_LONG,
                false, "Displays this help message.");
        Option cfgOption = new Option(
                SensorDataProducerUtil.CMD_OPTION_CFG_FILE_SHORT,
                SensorDataProducerUtil.CMD_OPTION_CFG_FILE_LONG,
                true, "Pulsar cluster connection configuration file.");
        Option workloadOption = new Option(
                SensorDataProducerUtil.CMD_OPTION_WORKLOAD_SOURCE_SHORT,
                SensorDataProducerUtil.CMD_OPTION_WORKLOAD_SOURCE_LONG,
                true, "Input workload source file.");

        options.addOption(helpOption);
        options.addOption(cfgOption);
        options.addOption(workloadOption);
    }

    static void usageAndExit(int errorCode) {

        System.out.println();

        PrintWriter errWriter = new PrintWriter(System.out, true);

        try {
            HelpFormatter formatter = new HelpFormatter();

            formatter.printHelp(errWriter, 150, "SensorDataProducer",
                    "\nSensorDataProducer options:",
                    options, 2, 1, "", true);

            System.out.println();
            System.out.println();
        }
        catch ( Exception e ) {
            e.printStackTrace();
        }

        System.exit(errorCode);
    }
    static void usageAndExit() {
        usageAndExit(0);
    }

    // Main function
    public static void main(String[] args) {
        try {
            DefaultParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, args);

            // Print help message
            if ( cmd.hasOption(SensorDataProducerUtil.CMD_OPTION_HELP_SHORT) ) {
                usageAndExit();
            }

            // "-f/--config" option is a must.
            String cfgFileStr = cmd.getOptionValue(SensorDataProducerUtil.CMD_OPTION_CFG_FILE_SHORT);
            if ( StringUtils.isBlank(cfgFileStr) ) {
                cfgFileStr = "resources/main/pulsar.properties";
            }

            String canonicalFilePath = cfgFileStr;
            try {
                File file = new File(cfgFileStr);
                canonicalFilePath = file.getCanonicalPath();
            }
            catch (IOException ioe) {
                System.out.println("Can't read the specified config properties file (" + cfgFileStr + ")!");
                // ioe.printStackTrace();
                usageAndExit(20);
            }

            // "-w/--workload" option is a must
            String workloadSrcFileStr = cmd.getOptionValue(SensorDataProducerUtil.CMD_OPTION_WORKLOAD_SOURCE_SHORT);
            if ( StringUtils.isBlank(workloadSrcFileStr) ) {
                workloadSrcFileStr = "workload_gen.csv";
            }

            canonicalFilePath = workloadSrcFileStr;
            try {
                File file = new File(workloadSrcFileStr);
                canonicalFilePath = file.getCanonicalPath();
            }
            catch (IOException ioe) {
                System.out.println("Can't read the specified workload source file (" + workloadSrcFileStr + ")!");
                // ioe.printStackTrace();
                usageAndExit(30);
            }

            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }

            // Read Pulsar cluster connection configuration from the provided properties file
            Parameters params = new Parameters();
            FileBasedConfigurationBuilder<FileBasedConfiguration> builder =
                    new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class)
                            .configure(params.properties()
                                    .setFileName(cfgFileStr));

            Configuration config = builder.getConfiguration();

            // Pulsar service URL
            String pulsarSvcUrl = config.getString("pulsar_svc_url");
            if (StringUtils.isBlank(pulsarSvcUrl)) {
                String errMsg = String.format("\"pulsar_svc_url\" value (%s) must NOT be empty!", pulsarSvcUrl);
                throw new RuntimeException(errMsg);
            }

            // Create a Pulsar client
            String authNEnabledStr = config.getString("authNEnabled");
            boolean authNEnabled = BooleanUtils.toBoolean(authNEnabledStr);

            HashMap<String, Object> clientConf = new HashMap<>();
            String clientPrefix = "client";
            for (Iterator<String> it = config.getKeys(clientPrefix); it.hasNext(); ) {
                String confKey = it.next();
                String confVal = config.getProperty(confKey).toString();
                if (!StringUtils.isBlank(confVal))
                    clientConf.put(confKey.substring(clientPrefix.length() + 1), config.getProperty(confKey));
            }
            PulsarClient pulsarClient = SensorDataProducerUtil.createPulsarClient(authNEnabled, clientConf, pulsarSvcUrl);

            // Get Pulsar schema
            String schemaType = config.getString("schema.type");
            String schemaDefinitionStr = config.getString("schema.definition");
            Schema<?> pulsarSchema = SensorDataProducerUtil.getPulsarSchema(schemaType, schemaDefinitionStr);

            // Topic name
            String pulsarTopic = config.getString("topic_uri");
            if (StringUtils.isBlank(pulsarTopic)) {
                String errMsg = String.format("\"topic_uri\" value (%s) must NOT be empty!", pulsarSvcUrl);
                throw new RuntimeException(errMsg);
            }

            // Create a Pulsar producer with certain schema on the specified topic
            Producer producer = pulsarClient
                    .newProducer(pulsarSchema)
                    .enableBatching(true)
                    .blockIfQueueFull(true)
                    .topic(pulsarTopic)
                    .create();

            FileInputStream fileInputStream=new FileInputStream(workloadSrcFileStr);
            Scanner scanner = new Scanner(fileInputStream);

            String msgLine;
            int totalMsg = 0;
            int lineNum = 1;
            AtomicInteger msgSucceedCnt = new AtomicInteger();
            AtomicInteger msgFailedCnt = new AtomicInteger();

            CompletableFuture<MessageId> future = null;

            System.out.println("\n=========================");
            System.out.println("Sending messages ...");

            while ( scanner.hasNext() ) {
                totalMsg++;
                msgLine = scanner.nextLine();

                String[] strArr = StringUtils.split(msgLine, ',');
                String msgPayloadJson = String.format(
                        "{" +
                            "\"DrillID\": \"%s\", " +
                            "\"SensorID\": \"%s\", " +
                            "\"SensorType\": \"%s\", " +
                            "\"ReadingTime\": \"%s\", " +
                            "\"ReadingValue\": %f" +
                        "}",
                        strArr[0],
                        strArr[1],
                        strArr[2],
                        strArr[4],
                        Float.parseFloat(strArr[5])
                );

                TypedMessageBuilder messageBuilder =
                        producer.newMessage(pulsarSchema);

                if ( StringUtils.equalsIgnoreCase(schemaType, "avro") ) {
                    org.apache.avro.generic.GenericRecord avroRecord =
                            SensorDataProducerUtil.GetGenericRecord_ApacheAvro(
                                    pulsarSchema.getSchemaInfo().getSchemaDefinition(),
                                    msgPayloadJson
                            );
                    GenericRecord payloadAvro = SensorDataProducerUtil.GetGenericRecord_PulsarAvro(
                            (GenericAvroSchema) pulsarSchema,
                            avroRecord);

                    messageBuilder.value(payloadAvro);
                }
                else {
                    byte[] payloadBytes = msgPayloadJson.getBytes(StandardCharsets.UTF_8);
                    messageBuilder.value(payloadBytes);

                }

                messageBuilder.property("line", String.valueOf(lineNum));
                //messageBuilder.key("line" + lineNum);

                // Sync API
                //try {
                //    MessageId msgId = messageBuilder.send();
                //} catch (PulsarClientException e) {
                //    e.printStackTrace();
                //}

                // Async API
                future = messageBuilder.sendAsync();
                future.whenComplete((messageId, throwable) -> msgSucceedCnt.getAndIncrement())
                    .exceptionally(ex -> {
                        logger.trace("Failed to publish message: " + msgPayloadJson );
                        msgFailedCnt.getAndIncrement();
                        //System.out.println("Failed to publish message: " + msgPayloadJson );
                        return null;
                    });

                lineNum++;
            }

            // Wait for the message sending process to complete before it exits.
            if (future != null)
                future.get(10, TimeUnit.SECONDS);

            System.out.println("  Total message read: " + totalMsg);
            System.out.println("  Total Messages successfully sent : " + msgSucceedCnt.get());
            System.out.println("  Total Messages failed to send: " + msgFailedCnt.get());

            // Finish processing
            scanner.close();
            fileInputStream.close();
            producer.close();
            pulsarClient.close();
        }
        catch (Exception e) {
            e.printStackTrace();
            System.err.format("\nERROR: Unexpected error happens: %s.\n", e.getMessage());
            usageAndExit(100);
        }
    }
}
