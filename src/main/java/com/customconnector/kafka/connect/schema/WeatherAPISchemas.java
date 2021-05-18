package com.customconnector.kafka.connect.schema;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public class WeatherAPISchemas {

    public static Schema WEATHER_SCHEMA = SchemaBuilder.struct()
            .name("Weather")
            .version(1)
            .field(WeatherAPISchemaFields.ID, Schema.INT64_SCHEMA)
            .field(WeatherAPISchemaFields.MAIN, Schema.STRING_SCHEMA)
            .field(WeatherAPISchemaFields.DESCRIPTION, Schema.STRING_SCHEMA)
            .field(WeatherAPISchemaFields.ICON, Schema.STRING_SCHEMA)
            .build();

    public static Schema WEATHER_ARRAY_SCHEMA = SchemaBuilder.array(WEATHER_SCHEMA)
            .build();

    public static Schema MAIN_SCHEMA = SchemaBuilder.struct()
            .name("Main")
            .version(1)
            .field(WeatherAPISchemaFields.TEMP, Schema.FLOAT32_SCHEMA)
            .field(WeatherAPISchemaFields.PRESSURE, Schema.INT32_SCHEMA)
            .field(WeatherAPISchemaFields.HUMIDITY, Schema.INT32_SCHEMA)
            .field(WeatherAPISchemaFields.TEMP_MIN, Schema.FLOAT32_SCHEMA)
            .field(WeatherAPISchemaFields.TEMP_MAX, Schema.FLOAT32_SCHEMA)
            .build();

    public static Schema WIND_SCHEMA = SchemaBuilder.struct()
            .name("Wind")
            .version(1)
            .field(WeatherAPISchemaFields.SPEED, Schema.OPTIONAL_FLOAT32_SCHEMA)
            .field(WeatherAPISchemaFields.DEG, Schema.OPTIONAL_INT32_SCHEMA)
            .optional()
            .build();

    public static Schema KEY_SCHEMA = SchemaBuilder.struct()
            .name("Key Schema")
            .version(1)
            .field(WeatherAPISchemaFields.ID, Schema.INT64_SCHEMA)
            .build();

    public static Schema VALUE_SCHEMA = SchemaBuilder.struct()
            .name("Value Schema")
            .version(1)
            .field(WeatherAPISchemaFields.NAME, Schema.STRING_SCHEMA)
            .field(WeatherAPISchemaFields.WEATHER, WEATHER_ARRAY_SCHEMA)
            .field(WeatherAPISchemaFields.MAIN, MAIN_SCHEMA)
            .field(WeatherAPISchemaFields.WIND, WIND_SCHEMA)
            .build();

}
