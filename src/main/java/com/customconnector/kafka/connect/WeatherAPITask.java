package com.customconnector.kafka.connect;

import com.customconnector.kafka.connect.model.Weather;
import com.customconnector.kafka.connect.schema.WeatherAPISchemaFields;
import com.customconnector.kafka.connect.schema.WeatherAPISchemas;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class WeatherAPITask extends SourceTask {

    private WeatherAPIConfig config;
    private WeatherAPIClient client;

    private AtomicBoolean isRunning = new AtomicBoolean(false);

    @Override
    public void start(Map<String, String> props) {
        config = new WeatherAPIConfig(props);
        client = new WeatherAPIClient(config);
        isRunning.set(true);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        if(!isRunning.get()) {
            return Collections.emptyList();
        }

        Thread.sleep(config.getPollFrequency());

        return client.getCurrentWeather()
                .stream()
                .map(weather -> new SourceRecord(sourcePartition(weather),
                        sourceOffset(),
                        config.getKafkaTopic(),
                        WeatherAPISchemas.KEY_SCHEMA, buildKey(weather.getId()),
                        WeatherAPISchemas.VALUE_SCHEMA, buildValue(weather)))
                .collect(Collectors.toList());
    }

    private Map<String, ?> sourcePartition(Weather weather) {
        Map<String, String> sourcePartition = new HashMap<>();
        sourcePartition.put("location", weather.getName());
        return sourcePartition;
    }

    private Map<String, ?> sourceOffset() {
        return new HashMap<>();
    }

    private Struct buildKey(Long id) {
        return new Struct(WeatherAPISchemas.KEY_SCHEMA)
                .put(WeatherAPISchemaFields.ID, id);
    }

    private Struct buildValue(Weather weather) {
        return new Struct(WeatherAPISchemas.VALUE_SCHEMA)
                .put(WeatherAPISchemaFields.NAME, weather.getName())
                .put(WeatherAPISchemaFields.MAIN, new Struct(WeatherAPISchemas.MAIN_SCHEMA)
                        .put(WeatherAPISchemaFields.TEMP, weather.getMain().getTemp())
                        .put(WeatherAPISchemaFields.PRESSURE, weather.getMain().getPressure())
                        .put(WeatherAPISchemaFields.HUMIDITY, weather.getMain().getHumidity())
                        .put(WeatherAPISchemaFields.TEMP_MIN, weather.getMain().getTempMin())
                        .put(WeatherAPISchemaFields.TEMP_MAX, weather.getMain().getTempMax()))
                .put(WeatherAPISchemaFields.WIND, new Struct(WeatherAPISchemas.WIND_SCHEMA)
                        .put(WeatherAPISchemaFields.SPEED, weather.getWind().getSpeed())
                        .put(WeatherAPISchemaFields.DEG, weather.getWind().getDeg()))
                .put(WeatherAPISchemaFields.WEATHER, weather.getWeather()
                        .stream()
                        .map(weatherDetails -> new Struct(WeatherAPISchemas.WEATHER_SCHEMA)
                                .put(WeatherAPISchemaFields.ID, weatherDetails.getId())
                                .put(WeatherAPISchemaFields.MAIN, weatherDetails.getMain())
                                .put(WeatherAPISchemaFields.DESCRIPTION, weatherDetails.getDescription())
                                .put(WeatherAPISchemaFields.ICON, weatherDetails.getIcon()))
                        .collect(toList()));
    }

    @Override
    public void stop() {
        isRunning.set(false);
    }

    @Override
    public String version() {
        return "1.0";
    }
}
