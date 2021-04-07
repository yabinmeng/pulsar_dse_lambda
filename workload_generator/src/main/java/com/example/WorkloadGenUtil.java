package com.example;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

public class WorkloadGenUtil {

    public static final String CMD_OPTION_HELP_SHORT = "h";
    public static final String CMD_OPTION_HELP_LONG = "help";
    public static final String CMD_OPTION_CFG_FILE_SHORT = "f";
    public static final String CMD_OPTION_CFG_FILE_LONG = "config";
    public static final String CMD_OPTION_OUTPUT_SHORT = "o";
    public static final String CMD_OPTION_OUTPUT_LONG = "output";

    // Sensor type
    public enum SENSOR_TYPE {
        // temperature
        TEMP("temp"),
        // speed
        SPEED("speed"),
        // depth
        //DEPTH("depth"),
        // pressure
        //PRESSURE("pressure")
        ;
        public final String label;

        SENSOR_TYPE(String label) {
            this.label = label;
        }
    }
    public static boolean isValidSensorType(String type) {
        return Arrays.stream(SENSOR_TYPE.values()).anyMatch((t) -> t.label.equalsIgnoreCase(type));
    }
    public static String getValidSensorTypeList() {
        return Arrays.stream(SENSOR_TYPE.values()).map(t -> t.label).collect(Collectors.joining(","));
    }

    // Workload frequency unit - currently only supports "second", "minute", and "hour"
    public enum WL_FREQUENCY_UNIT {
        // second
        S("s"),
        // minute
        M("m"),
        // hour
        H("h");

        public final String label;

        WL_FREQUENCY_UNIT(String label) {
            this.label = label;
        }
    }
    public static boolean isValidWLFrequencyUnit(String unit) {
        return Arrays.stream(WL_FREQUENCY_UNIT.values()).anyMatch((t) -> t.name().equals(unit.toLowerCase()));
    }

    // Workload frequency unit - currently only supports "day" and "week"
    public enum WL_PERIOD_UNIT {
        // second
        S("s"),
        // minute
        M("m"),
        // hour
        H("h"),
        // day
        D("d"),
        // week
        W("w");

        public final String label;

        WL_PERIOD_UNIT(String label) {
            this.label = label;
        }
    }
    public static boolean isValidWLPeriodUnit(String unit) {
        return Arrays.stream(WL_PERIOD_UNIT.values()).anyMatch((t) -> t.name().equals(unit.toLowerCase()));
    }

    public static final String dateOnlyFormatStr = "uuuu-MM-dd";
    public static final SimpleDateFormat dateOnlyDateFormat = new SimpleDateFormat(dateOnlyFormatStr);
    public static final String datetimeSecFormatStr = "uuuu-MM-dd hh:mm:ss";
    public static final SimpleDateFormat datetimeSecDateFormat = new SimpleDateFormat(datetimeSecFormatStr);
    public static final String datetimeMillSecFormatStr = "uuuu-MM-dd hh:mm:ss.SSS";
    public static final SimpleDateFormat datetimeMillSecDateFormat = new SimpleDateFormat(datetimeMillSecFormatStr);
}
