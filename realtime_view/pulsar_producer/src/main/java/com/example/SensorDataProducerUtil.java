package com.example;

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericRecordBuilder;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SensorDataProducerUtil {
    public static final String CMD_OPTION_HELP_SHORT = "h";
    public static final String CMD_OPTION_HELP_LONG = "help";
    public static final String CMD_OPTION_CFG_FILE_SHORT = "f";
    public static final String CMD_OPTION_CFG_FILE_LONG = "config";
    public static final String CMD_OPTION_WORKLOAD_SOURCE_SHORT = "w";
    public static final String CMD_OPTION_WORKLOAD_SOURCE_LONG = "workload";

    /**
     * Create Pulsar client
     */
    public static PulsarClient createPulsarClient(
            boolean authNEnabled,
            Map<String, Object> clientConf,
            String pulsarSvcUrl)
    {
        ClientBuilder clientBuilder = PulsarClient.builder();
        clientBuilder.loadConf(clientConf).serviceUrl(pulsarSvcUrl);
        PulsarClient pulsarClient;

        try {
            if (authNEnabled) {
                String authPluginClassName = (String) clientConf.get("authPluginClassName");
                String authParams = (String) clientConf.get("authParams");

                String useTlsStr = (String) clientConf.get("useTls");
                boolean useTls = BooleanUtils.toBoolean(useTlsStr);

                String tlsTrustCertsFilePath = (String) clientConf.get("tlsTrustCertsFilePath");

                String tlsAllowInsecureConnectionStr = (String) clientConf.get("tlsAllowInsecureConnection");
                boolean tlsAllowInsecureConnection = BooleanUtils.toBoolean(tlsAllowInsecureConnectionStr);

                String tlsHostnameVerificationEnableStr = (String) clientConf.get("tlsHostnameVerificationEnable");
                boolean tlsHostnameVerificationEnable = BooleanUtils.toBoolean(tlsHostnameVerificationEnableStr);

                if (!StringUtils.isAnyBlank(authPluginClassName, authParams)) {
                    clientBuilder.authentication(authPluginClassName, authParams);
                }

                if (useTls) {
                    clientBuilder
                            .useKeyStoreTls(true)
                            .enableTlsHostnameVerification(tlsHostnameVerificationEnable);

                    if (!StringUtils.isBlank(tlsTrustCertsFilePath))
                        clientBuilder.tlsTrustCertsFilePath(tlsTrustCertsFilePath);
                }

                // Put this outside "if (useTls)" block for easier handling of "tlsAllowInsecureConnection"
                clientBuilder.allowTlsInsecureConnection(tlsAllowInsecureConnection);
            }

            pulsarClient = clientBuilder.build();
        }
        catch (PulsarClientException pce) {
            String errMsg = "Fail to create PulsarClient: " + pce.getMessage();
            throw new RuntimeException(errMsg);
        }

        return  pulsarClient;
    }

    // Get Pulsar schema
    public static Schema<?> getPulsarSchema(String schemaType, String schemaDefinitionStr) {
        String filePrefix = "file://";
        Schema<?> schema;

        // Default schema type: byte[]
        if ( StringUtils.isBlank(schemaType) ) {
            schema = Schema.BYTES;
        }
        // Avro schema type
        else if ( StringUtils.equalsIgnoreCase(schemaType, "avro")) {
            if (StringUtils.isBlank(schemaDefinitionStr)) {
                throw new RuntimeException("Schema definition must be provided for \"Avro\" schema type!");
            } else if (schemaDefinitionStr.startsWith(filePrefix)) {
                try {
                    Path filePath = Paths.get(URI.create(schemaDefinitionStr));
                    schemaDefinitionStr = Files.readString(filePath, StandardCharsets.US_ASCII);
                } catch (IOException ioe) {
                    throw new RuntimeException("Error reading the specified \"Avro\" schema definition file: " + schemaDefinitionStr);
                }
            }

            SchemaInfo schemaInfo = SchemaInfo.builder()
                    .schema(schemaDefinitionStr.getBytes(StandardCharsets.UTF_8))
                    .type(SchemaType.AVRO)
                    .properties(new HashMap<>())
                    .name("NBAvro")
                    .build();

            schema = new GenericAvroSchema(schemaInfo);

        } else {
            throw new RuntimeException("Unsupported schema type string: " + schemaType + "; " +
                    "Only primitive type and Avro type are supported at the moment!");
        }

        return schema;
    }


    // Create a general (Apache) Avro record
    public static org.apache.avro.generic.GenericRecord GetGenericRecord_ApacheAvro(String avroSchemaDef, String jsonData)  {
        org.apache.avro.generic.GenericRecord record = null;

        try {
            org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(avroSchemaDef);

            org.apache.avro.generic.GenericDatumReader<org.apache.avro.generic.GenericData.Record> reader;
            reader = new org.apache.avro.generic.GenericDatumReader<>(schema);

            JsonDecoder decoder = DecoderFactory.get().jsonDecoder(schema, jsonData);

            record = reader.read(null, decoder);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

        return record;
    }

    // Convert a general (Apache) avro record to Pulsar avro record
    public static GenericRecord GetGenericRecord_PulsarAvro(
            GenericAvroSchema pulsarGenericAvroSchema,
            org.apache.avro.generic.GenericRecord apacheAvroGenericRecord)
    {
        GenericRecordBuilder recordBuilder = pulsarGenericAvroSchema.newRecordBuilder();

        List<Field> fieldList = pulsarGenericAvroSchema.getFields();
        for (Field field : fieldList) {
            String fieldName = field.getName();
            recordBuilder.set(fieldName, apacheAvroGenericRecord.get(fieldName));
        }

        return recordBuilder.build();
    }
}
