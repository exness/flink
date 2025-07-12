package com.exness.sdp.flink.format.avro;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.apicurio.registry.resolver.SchemaResolverConfig;
import io.apicurio.registry.serde.SerdeConfig;
import io.apicurio.registry.serde.config.IdOption;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.util.FlinkRuntimeException;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import static io.apicurio.registry.resolver.SchemaResolverConfig.AUTO_REGISTER_ARTIFACT;
import static io.apicurio.registry.resolver.SchemaResolverConfig.REGISTRY_URL;
import static io.apicurio.registry.serde.SerdeConfig.ENABLE_CONFLUENT_ID_HANDLER;
import static io.apicurio.registry.serde.SerdeConfig.ENABLE_HEADERS;
import static io.apicurio.registry.serde.SerdeConfig.USE_ID;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class AvroSchemaFormatOptions {
    public static final String APICURIO_REGISTRY_PREFIX = "apicurio.registry.";
    public static final ConfigOption<String> SCHEMA_REGISTRY_URL = ConfigOptions.key(REGISTRY_URL)
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<Boolean> SCHEMA_REGISTRY_ENABLE_CONFLUENT_ID_HANDLER =
            ConfigOptions.key(ENABLE_CONFLUENT_ID_HANDLER).booleanType().defaultValue(true);

    public static final ConfigOption<Boolean> SCHEMA_REGISTRY_ENABLE_HEADERS =
            ConfigOptions.key(ENABLE_HEADERS).booleanType().defaultValue(false);

    public static final ConfigOption<Boolean> SCHEMA_REGISTRY_AUTO_REGISTER_ARTIFACT =
            ConfigOptions.key(AUTO_REGISTER_ARTIFACT).booleanType().defaultValue(true);

    public static final ConfigOption<String> SCHEMA_REGISTRY_USE_ID =
            ConfigOptions.key(USE_ID).stringType().defaultValue(IdOption.contentId.name());

    public static final ConfigOption<String> SCHEMA_NAME =
            ConfigOptions.key("schema-registry.schema-name").stringType().noDefaultValue();

    public static final ConfigOption<String> SCHEMA_NAMESPACE =
            ConfigOptions.key("schema-registry.schema-namespace").stringType().defaultValue("com.exness.sdp.flink");

    public static final ConfigOption<String> SCHEMA_REGISTRY_SUBJECT =
        ConfigOptions.key("schema-registry.subject").stringType().noDefaultValue();

    public static Map<String, Object> getRegistryProperties(Map<String, String> tableOptions, ReadableConfig config) {
        final Map<String, Object> registryProperties = new HashMap<>();
        tableOptions.keySet().stream()
                .filter(key -> key.contains(APICURIO_REGISTRY_PREFIX))
                .forEach(
                        key -> {
                            final String value = tableOptions.get(key);
                            final int prefixStart = key.indexOf(APICURIO_REGISTRY_PREFIX);
                            registryProperties.put(key.substring(prefixStart), value);
                        });

        registryProperties.put(SCHEMA_REGISTRY_ENABLE_CONFLUENT_ID_HANDLER.key(), config.get(SCHEMA_REGISTRY_ENABLE_CONFLUENT_ID_HANDLER));
        registryProperties.put(SCHEMA_REGISTRY_ENABLE_HEADERS.key(), config.get(SCHEMA_REGISTRY_ENABLE_HEADERS));
        registryProperties.put(SCHEMA_REGISTRY_AUTO_REGISTER_ARTIFACT.key(), config.get(SCHEMA_REGISTRY_AUTO_REGISTER_ARTIFACT));
        registryProperties.put(SCHEMA_REGISTRY_USE_ID.key(), config.get(SCHEMA_REGISTRY_USE_ID));

        return registryProperties;
    }

    public static List<ConfigOption<String>> getApicurioRegistryOptions() {
        List<ConfigOption<String>> options = new ArrayList<>();
        Class<SchemaResolverConfig> schemaResolverConfigClass = SchemaResolverConfig.class;
        Class<SerdeConfig> serdeConfigClass = SerdeConfig.class;
        List<Field> fields = new ArrayList<>(List.of(schemaResolverConfigClass.getDeclaredFields()));
        fields.addAll(List.of(serdeConfigClass.getDeclaredFields()));
        for (Field field : fields) {
            // Check if the field is static
            if (Modifier.isStatic(field.getModifiers()) && field.getType().equals(String.class) && field.canAccess(null)) {
                // Since it's static, we don't need an instance to get the value
                try {
                    // Get the value of the static field
                    String value = (String) field.get(null);
                    ConfigOption<String> configOption = ConfigOptions.key(value).stringType().noDefaultValue();
                    options.add(configOption);
                } catch (IllegalAccessException e) {
                    throw new FlinkRuntimeException("Error while getting the value of the field: " + field.getName(), e);
                }
            }
        }
        return options;
    }
}
