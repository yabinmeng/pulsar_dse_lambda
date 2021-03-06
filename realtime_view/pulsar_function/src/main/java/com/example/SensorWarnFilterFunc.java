package com.example;

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericRecordBuilder;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.slf4j.Logger;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;

public class SensorWarnFilterFunc implements Function<String, Void> {
    private static final float TEMP_WARN_THRESHOLD = 400;
    private static final float SPEED_WARN_THRESHOLD = 2500;

    private static final String avroSchemDef = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"IotSensor\",\n" +
            "  \"namespace\": \"TestNS\",\n" +
            "  \"fields\" : [\n" +
            "    {\"name\": \"DrillID\", \"type\": \"string\"},\n" +
            "    {\"name\": \"SensorID\", \"type\": \"string\"},\n" +
            "    {\"name\": \"ReadingDate\", \"type\": \"string\"},\n" +
            "    {\"name\": \"ReadingTime\", \"type\": \"string\"},\n" +
            "    {\"name\": \"SensorType\", \"type\": \"string\"},\n" +
            "    {\"name\": \"ReadingValue\", \"type\": \"float\"}\n" +
            "  ]\n" +
            "}";

    // for testing purposes
    //private static final String input = "{\"DrillID\": \"DRL-001\", \"SensorID\": \"SNS-temp-01\", \"SensorType\": \"temp\", \"ReadingTime\": \"2021-04-05T17:10:22\", \"ReadingValue\": 399.000000}";
    //private static final String input2 = "{\"DrillID\": \"DRL-001\", \"SensorID\": \"SNS-speed-02\", \"SensorType\": \"speed\", \"ReadingTime\": \"2021-04-05T17:10:22\", \"ReadingValue\": 1831.130005}";

    @Override
    public Void process(String input, Context context) throws Exception {
        // Incoming sensor data payload has the following json format (see above)

        LocalDate now = LocalDate.now(ZoneOffset.UTC);
        LocalDateTime todayStart = now.atStartOfDay();

        if (!StringUtils.isBlank(input)) {
            String str = StringUtils.strip(input, "{}");

            String[] arr = StringUtils.split(str, ",");

            String drillIdStr = StringUtils.strip(StringUtils.split(arr[0], ":")[1].trim(), "\"");
            String sensorIdStr = StringUtils.strip(StringUtils.split(arr[1], ":")[1].trim(), "\"");
            String sensorType = StringUtils.strip(StringUtils.split(arr[2], ":")[1].trim(), "\"");
            String readingTimeStr = StringUtils.strip(StringUtils.substringAfter(arr[3], ":").trim(), "\"");
            LocalDateTime readingTime = LocalDateTime.parse(readingTimeStr, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            LocalDate readingDate = readingTime.toLocalDate();
            float readValue = Float.parseFloat(
                    StringUtils.strip(StringUtils.substringAfter(arr[4], ":").trim(), "\""));

            // Only keep most recent messages
            if ( ! readingTime.isBefore(todayStart) ) {
                // Only keep messages with reading values beyond the thresholds
                boolean tempWarning =
                        ( StringUtils.equalsIgnoreCase(sensorType, "temp") &&
                          (readValue >TEMP_WARN_THRESHOLD) );

                boolean speedWarning =
                        ( StringUtils.equalsIgnoreCase(sensorType, "speed") &&
                                (readValue > SPEED_WARN_THRESHOLD) );

                if (tempWarning || speedWarning) {
                    String outputMsgPayload = String.format(
                            "{" +
                                    "\"DrillID\": \"%s\", " +
                                    "\"SensorID\": \"%s\", " +
                                    "\"ReadingDate\": \"%s\", " +
                                    "\"ReadingTime\": \"%s\", " +
                                    "\"SensorType\": \"%s\", " +
                                    "\"ReadingValue\": %f" +
                            "}",
                            drillIdStr,
                            sensorIdStr,
                            readingDate.format(DateTimeFormatter.ISO_LOCAL_DATE),
                            readingTimeStr,
                            sensorType,
                            readValue);

                    String outputTopic = context.getOutputTopic();

                    // Use Avro schema for the output topic
                    SchemaInfo schemaInfo = SchemaInfo.builder()
                            .schema(avroSchemDef.getBytes(StandardCharsets.UTF_8))
                            .type(SchemaType.AVRO)
                            .properties(new HashMap<>())
                            .name("NBAvro")
                            .build();
                    Schema pulsarSchema = new GenericAvroSchema(schemaInfo);

                    TypedMessageBuilder messageBuilder
                            = context.newOutputMessage(outputTopic, pulsarSchema);

                    // Generate the Apache Avro record from the JSON payload
                    org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(avroSchemDef);
                    org.apache.avro.generic.GenericDatumReader<org.apache.avro.generic.GenericData.Record> reader;
                    reader = new org.apache.avro.generic.GenericDatumReader<>(schema);
                    JsonDecoder decoder = DecoderFactory.get().jsonDecoder(schema, outputMsgPayload);
                    org.apache.avro.generic.GenericRecord apacheAvroRecord = reader.read(null, decoder);

                    // Convert Apache Avro record to Pulsar Avro record
                    GenericRecordBuilder recordBuilder = ((GenericAvroSchema)pulsarSchema).newRecordBuilder();
                    List<Field> fieldList = ((GenericAvroSchema)pulsarSchema).getFields();
                    for (Field field : fieldList) {
                        String fieldName = field.getName();
                        recordBuilder.set(fieldName, apacheAvroRecord.get(fieldName));
                    }
                    GenericRecord pulsarAvroRecord = recordBuilder.build();

                    messageBuilder.value(pulsarAvroRecord);

                    Logger LOG = context.getLogger();
                    CompletableFuture<MessageId> future = messageBuilder.sendAsync();
                    future.whenComplete((messageId, throwable) ->
                                LOG.trace("Failed to publish message: " + outputMsgPayload ))
                            .exceptionally(ex -> {
                                LOG.trace("Failed to publish message: " + outputMsgPayload );
                                return null;
                            });
                }
            }
        }

        return null;
    }
}
