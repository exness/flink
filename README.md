# Flink Utils

Various tools and improvements for Flink

[[_TOC_]]

## File formats

### json-schema (Json With Schema Registry)

'json-schema' format has been created in order to support Schema Registry Json messages.
</br>
It's better to use this format with `kafka-exness` connector in order to have correct headers when using `'json-schema.apicurio.registry.headers.enabled' = 'true'`.
</br>
This format is based on the [Apicurio Json SerDe](https://www.apicur.io/registry/docs/apicurio-registry/2.5.x/getting-started/assembly-configuring-kafka-client-serdes.html#registry-serdes-types-json_registry)
All the Apicurio configs can be used with this format.
Connector `kafka-exness` will supply the default `json-schema.apicurio.registry.artifact.artifact-id` equals to `topic.name-value`, but it can be changed in the DDL.

This format has the same configuration as [Json Format](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/table/formats/json/#format-options) with addition of all the Apicurio configs and the following ones:

| Parameter                               | Description                                                                                       | Example        | Default        |
|-----------------------------------------|---------------------------------------------------------------------------------------------------|----------------|----------------|
| json-schema.schema-registry.schema-name | `title` field in Json Schema definition. By default equals to the artifact-id in Upper Camel Case | SomeSchemaName | TopicNameValue |

Default Apicurio configs for this format:

| Parameter                                     | Description                                                                                                                                                                                                                                                                     | Example  | Default   |
|-----------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|-----------|
| json-schema.apicurio.registry.as-confluent    | Use Confluent Id format                                                                                                                                                                                                                                                         | false    | true      |
| json-schema.apicurio.registry.headers.enabled | Boolean to indicate whether serde classes should pass Global Id information via message headers instead of in the message payload.                                                                                                                                              | false    | true      |
| json-schema.apicurio.registry.auto-register   | Boolean to indicate whether serializer classes should attempt to create an artifact in the registry.                                                                                                                                                                            | false    | true      |
| json-schema.apicurio.registry.use-id          | Configures the serdes to use the specified IdOption as the identifier for the artifacts. Instructs the serializer to write the specified id into the kafka records and instructs the deserializer to read and use the specified id from the kafka records (to find the schema). | globalId | contentId |

Schema Registry URL should be configured via `json-schema.apicurio.registry.url` parameter.

<b> Keep in mind that the URL should contain only https://schema-registry.test.env or http://schema-registry.prod.env value and should NOT contain /apis/ccompat7 part of it </b>

### avro-schema (Avro With Schema Registry)

'avro-schema' format has been created in order to support Schema Registry Avro messages.
</br>
It's better to use this format with `kafka-exness` connector in order to have correct headers when using `'avro-schema.apicurio.registry.headers.enabled' = 'true'`.
</br>
This format is based on the [Apicurio Avro SerDe](https://www.apicur.io/registry/docs/apicurio-registry/2.5.x/getting-started/assembly-configuring-kafka-client-serdes.html#registry-serdes-types-avro_registry)
All the Apicurio configs can be used with this format.
Connector `kafka-exness` will supply the default `avro-schema.apicurio.registry.artifact.artifact-id` equals to `topic.name-value`, but it can be changed in the DDL.

This format has all the Apicurio configs and the following ones:

| Parameter                                    | Description                                                                                      | Example          | Default              |
|----------------------------------------------|--------------------------------------------------------------------------------------------------|------------------|----------------------|
| avro-schema.schema-registry.schema-name      | `name` field in Avro Schema definition. By default equals to the artifact-id in Upper Camel Case | SomeSchemaName   | TopicNameValue       |
| avro-schema.schema-registry.schema-namespace | `namespace` field in Avro Schema definition. By default equals to `com.exness.sdp.flink`.        | custom.namespace | com.exness.sdp.flink |

Default Apicurio configs for this format:

| Parameter                                     | Description                                                                                                                                                                                                                                                                     | Example  | Default   |
|-----------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|-----------|
| avro-schema.apicurio.registry.as-confluent    | Use Confluent Id format                                                                                                                                                                                                                                                         | false    | true      |
| avro-schema.apicurio.registry.headers.enabled | Boolean to indicate whether serde classes should pass Global Id information via message headers instead of in the message payload.                                                                                                                                              | false    | false     |
| avro-schema.apicurio.registry.auto-register   | Boolean to indicate whether serializer classes should attempt to create an artifact in the registry.                                                                                                                                                                            | false    | true      |
| avro-schema.apicurio.registry.use-id          | Configures the serdes to use the specified IdOption as the identifier for the artifacts. Instructs the serializer to write the specified id into the kafka records and instructs the deserializer to read and use the specified id from the kafka records (to find the schema). | globalId | contentId |

Schema Registry URL should be configured via `avro-schema.apicurio.registry.url` parameter.

<b> Keep in mind that the URL should contain only https://schema-registry.test.env or http://schema-registry.prod.env value and should NOT contain /apis/ccompat7 part of it </b>

