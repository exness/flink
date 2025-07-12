package com.exness.sdp.flink.format.avro.util;

import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;

public class JodaConverter {
    private static JodaConverter instance;
    private static boolean instantiated = false;

    public static JodaConverter getConverter() {
        if (instantiated) {
            return instance;
        }

        try {
            Class.forName(
                    "org.joda.time.DateTime",
                    false,
                    Thread.currentThread().getContextClassLoader());
            instance = new JodaConverter();
        } catch (ClassNotFoundException e) {
            instance = null;
        } finally {
            instantiated = true;
        }
        return instance;
    }

    public long convertDate(Object object) {
        final LocalDate value = (LocalDate) object;
        return value.toDate().getTime();
    }

    public int convertTime(Object object) {
        final LocalTime value = (LocalTime) object;
        return value.get(DateTimeFieldType.millisOfDay());
    }

    public long convertTimestamp(Object object) {
        final DateTime value = (DateTime) object;
        return value.toDate().getTime();
    }

    private JodaConverter() {}
}
