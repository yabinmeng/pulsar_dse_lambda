package com.example;

import org.apache.commons.cli.*;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;

import java.io.*;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WorkloadGen {

    /**
     *  Define Command Line Arguments
     */
    static Options options = new Options();

    static {
        Option helpOption = new Option(
                WorkloadGenUtil.CMD_OPTION_HELP_SHORT, WorkloadGenUtil.CMD_OPTION_HELP_LONG,
                false, "Displays this help message.");
        Option cfgOption = new Option(
                WorkloadGenUtil.CMD_OPTION_CFG_FILE_SHORT, WorkloadGenUtil.CMD_OPTION_CFG_FILE_LONG,
                true, "Configuration properties file.");
        Option outCsvOption = new Option(
                WorkloadGenUtil.CMD_OPTION_OUTPUT_SHORT, WorkloadGenUtil.CMD_OPTION_OUTPUT_LONG,
                true, "Output CSV file name.");

        options.addOption(helpOption);
        options.addOption(cfgOption);
        options.addOption(outCsvOption);
    }

    static void usageAndExit(int errorCode) {

        System.out.println();

        PrintWriter errWriter = new PrintWriter(System.out, true);

        try {
            HelpFormatter formatter = new HelpFormatter();

            formatter.printHelp(errWriter, 150, "WuPocMain",
                    "\nWuPocMain options:",
                    options, 2, 1, "", true);

            System.out.println();
            System.out.println();
        }
        catch ( Exception e ) {
            e.printStackTrace();
        }

        System.exit(errorCode);
    }

    // Get workload frequency in seconds
    public static int getWorkloadFreqInSec(String workloadFreqStr) {

        // Check if "workloadPeriodStr" in "[0-9]+[s|m|h]" format
        String validFormat = String.format("[0-9]+[%s|%s|%s]",
                WorkloadGenUtil.WL_FREQUENCY_UNIT.S.label,
                WorkloadGenUtil.WL_FREQUENCY_UNIT.M.label,
                WorkloadGenUtil.WL_FREQUENCY_UNIT.H.label);

        Pattern pattern = Pattern.compile(validFormat);
        Matcher matcher = pattern.matcher(workloadFreqStr);

        if (!matcher.matches()) {
            String errMsg = String.format("Specified \"workload_frequency\" value (%s) doesn't follow the expected format (%s)",
                    workloadFreqStr,
                    validFormat);
            throw new RuntimeException(errMsg);
        }

        String wlFreqUnit = workloadFreqStr.substring(workloadFreqStr.length() - 1);
        String wlFreqNumStr = workloadFreqStr.substring(0, workloadFreqStr.length() - 1);
        int wlFreqNum = Integer.parseInt(wlFreqNumStr);

        int wlFreqInSec = 0;

        if (StringUtils.equalsIgnoreCase(wlFreqUnit, WorkloadGenUtil.WL_FREQUENCY_UNIT.S.label)) {
            wlFreqInSec = wlFreqNum;
        }
        else if (StringUtils.equalsIgnoreCase(wlFreqUnit, WorkloadGenUtil.WL_FREQUENCY_UNIT.M.label)) {
            wlFreqInSec = 60 * wlFreqNum;
        }
        else if (StringUtils.equalsIgnoreCase(wlFreqUnit, WorkloadGenUtil.WL_FREQUENCY_UNIT.H.label)) {
            wlFreqInSec = 3600 * wlFreqNum;
        }

        return wlFreqInSec;
    }

    // Get workload period in seconds
    public static int getWorkloadPeriodInSec(String workloadPeriodStr) {

        // Check if "workloadPeriodStr" in "[0-9]+[d|w]" format
        String validFormat = String.format("[0-9]+[%s|%s|%s|%s|%s]",
                WorkloadGenUtil.WL_PERIOD_UNIT.S.label,
                WorkloadGenUtil.WL_PERIOD_UNIT.M.label,
                WorkloadGenUtil.WL_PERIOD_UNIT.H.label,
                WorkloadGenUtil.WL_PERIOD_UNIT.D.label,
                WorkloadGenUtil.WL_PERIOD_UNIT.W.label);

        Pattern pattern = Pattern.compile(validFormat);
        Matcher matcher = pattern.matcher(workloadPeriodStr);

        if (!matcher.matches()) {
            String errMsg = String.format("Specified \"workload_period\" value (%s) doesn't follow the expected format (%s)",
                    workloadPeriodStr,
                    validFormat);
            throw new RuntimeException(errMsg);
        }

        String wlPeriodUnit = workloadPeriodStr.substring(workloadPeriodStr.length() - 1);
        String wlPeriodNumStr = workloadPeriodStr.substring(0, workloadPeriodStr.length() - 1);
        int wlPeriodNum = Integer.parseInt(wlPeriodNumStr);

        int wlPeriodInSec = 0;

        if (StringUtils.equalsIgnoreCase(wlPeriodUnit, WorkloadGenUtil.WL_PERIOD_UNIT.S.label)) {
            wlPeriodInSec = wlPeriodNum;
        }
        else if (StringUtils.equalsIgnoreCase(wlPeriodUnit, WorkloadGenUtil.WL_PERIOD_UNIT.M.label)) {
            wlPeriodInSec = 60 * wlPeriodNum;
        }
        else if (StringUtils.equalsIgnoreCase(wlPeriodUnit, WorkloadGenUtil.WL_PERIOD_UNIT.H.label)) {
            wlPeriodInSec = 3600 * wlPeriodNum;
        }
        else if (StringUtils.equalsIgnoreCase(wlPeriodUnit, WorkloadGenUtil.WL_PERIOD_UNIT.D.label)) {
            wlPeriodInSec = 24 * 3600 * wlPeriodNum;
        }
        else if (StringUtils.equalsIgnoreCase(wlPeriodUnit, WorkloadGenUtil.WL_PERIOD_UNIT.W.label)) {
            wlPeriodInSec = 7 * 24 * 3600 * wlPeriodNum;
        }

        return wlPeriodInSec;
    }

    // Get Unix time of a given timestamp
    public static long getUnixTime(Date timestamp) {
        return timestamp.getTime()/1000L;
    }

    // Get Date from a Unix time
    public static Date getDate(long unixTime) {
        return new Date(unixTime * 1000L);
    }

    // Get workload end time in Unix time format
    public static long getWorkloadEndUnixTime(String workloadEndDateStr) {

        Date endDateTime = Calendar.getInstance().getTime();

        if (!StringUtils.isBlank(workloadEndDateStr)) {

            // Check if "workloadEndDateStr" in "yyyy-MM-dd" format
            try {
                WorkloadGenUtil.dateOnlyDateFormat.parse(workloadEndDateStr);
            }
            catch (java.text.ParseException pe) {
                String errMsg = String.format("Specified \"workload_enddate\" value (%s) doesn't follow the expected format (%s)",
                        workloadEndDateStr,
                        WorkloadGenUtil.dateOnlyFormatStr);
                throw new RuntimeException(errMsg);
            }

            String endDateTimeStr = workloadEndDateStr + " 23:59:59";
            try {
                endDateTime = WorkloadGenUtil.datetimeSecDateFormat.parse(endDateTimeStr);
            }
            catch (java.text.ParseException pe) {
                pe.printStackTrace();
            }
        }

        return getUnixTime(endDateTime);
    }


    // Main function
    public static void main(String[] args) {

        DefaultParser parser = new DefaultParser();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);

            // Print help message
            if (cmd.hasOption(WorkloadGenUtil.CMD_OPTION_HELP_SHORT)) {
                usageAndExit(20);
            }

            // "-f/--config" option is a must.
            String cfgFileStr = cmd.getOptionValue(WorkloadGenUtil.CMD_OPTION_CFG_FILE_SHORT);
            if (StringUtils.isBlank(cfgFileStr)) {
                cfgFileStr = "resources/main/generator.properties";
            }

            File cfgFile;
            String canonicalFilePath = cfgFileStr;
            try {
                cfgFile = new File(cfgFileStr);
                canonicalFilePath = cfgFile.getCanonicalPath();
            } catch (IOException ioe) {
                System.out.println("Can't read the specified config properties file (" + cfgFileStr + ")!");
                // ioe.printStackTrace();
                usageAndExit(30);
            }

            // "-o/--output" option is a must
            String csvFileStr = cmd.getOptionValue(WorkloadGenUtil.CMD_OPTION_OUTPUT_SHORT);
            if (StringUtils.isBlank(csvFileStr)) {
                csvFileStr = "workload_gen.csv";
            }

            File csvFile;
            canonicalFilePath = csvFileStr;
            try {
                csvFile = new File(csvFileStr);
                canonicalFilePath = csvFile.getCanonicalPath();
            } catch (IOException ioe) {
                System.out.println("Can't read the specified output csv file (" + cfgFileStr + ")!");
                // ioe.printStackTrace();
                usageAndExit(40);
            }

            // Read configuration settings from properties file
            Parameters params = new Parameters();
            FileBasedConfigurationBuilder<FileBasedConfiguration> builder =
                    new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class)
                            .configure(params.properties()
                                    .setFileName(cfgFileStr));

            Configuration config = builder.getConfiguration();

            int drillNum = config.getInt("drill_num");
            if (drillNum <= 0) {
                String errMsg = String.format("Specified \"drill_num\" value (%d) must be positive integer!",
                        drillNum);
                throw new RuntimeException(errMsg);
            }

            String sensorTypeList = config.getString("sensor_types");
            String[] sensorTypeArr = StringUtils.split(sensorTypeList,",");
            int sensorTypeArrLen = sensorTypeArr.length;
            for (String type : sensorTypeArr) {
                if (!WorkloadGenUtil.isValidSensorType(type)) {
                    String errMsg = String.format("Specified \"sensor_types\" value (%s) is not valid. Valid sensor types: %s",
                            sensorTypeList,
                            WorkloadGenUtil.getValidSensorTypeList());
                    throw new RuntimeException(errMsg);
                }
            }

            int sensorNumPerType = config.getInt("sensor_num_per_type");
            if (sensorNumPerType <= 0) {
                String errMsg = String.format("Specified \"sensor_num_per_type\" value (%d) must be positive integer!",
                        sensorNumPerType);
                throw new RuntimeException(errMsg);
            }

            String workloadFrequencyStr = config.getString("workload_frequency");
            int wlFreqInSec = getWorkloadFreqInSec(workloadFrequencyStr);

            String workloadPeriodStr = config.getString("workload_period");
            int wlPeriodInSec = getWorkloadPeriodInSec(workloadPeriodStr);
            if (wlPeriodInSec < wlFreqInSec) {
                String errMsg = String.format("\"workload_frequency\" value (%s) must be lower than " +
                                "\"workload_period\" value (%s)!",
                        workloadFrequencyStr, workloadPeriodStr);
                throw new RuntimeException(errMsg);
            }

            String workloadEndDateStr = config.getString("workload_enddate");
            long wlEndTimeUnix = getWorkloadEndUnixTime(workloadEndDateStr);
            long wlStartTimeUnix = wlEndTimeUnix - wlPeriodInSec;
            if (wlStartTimeUnix < 0) {
                throw new RuntimeException("Negative workload start Unix time!");
            }

            System.out.printf("\n==============================\n" +
                    "drillNum: %d\n" +
                    "sensorTypeList: %s\n" +
                    "sensorNumPerType: %d\n" +
                    "wlPeriodInSec: %d\n" +
                    "wlFreqInSec: %d\n" +
                    "wlStartTimeUnix: %d\n" +
                    "wlEndTimeUnix: %d\n" +
                    "==============================\n\n",
                    drillNum, sensorTypeList, sensorNumPerType,
                    wlPeriodInSec, wlFreqInSec, wlStartTimeUnix, wlEndTimeUnix);

            FileOutputStream fos = new FileOutputStream(canonicalFilePath);
            DataOutputStream outStream = new DataOutputStream(new BufferedOutputStream(fos));

            for ( long cycle_time = wlStartTimeUnix;
                  cycle_time <= wlEndTimeUnix;
                  cycle_time = cycle_time + wlFreqInSec)
            {
                // Randomly pick a number of drills and a number of sensors for this cycle
                int drillCycleNum = RandomUtils.nextInt(1, (drillNum+1));
                int sensorCycleNum = RandomUtils.nextInt(1, (sensorNumPerType+1));

                for (int drillIdx = 0; drillIdx < drillCycleNum; drillIdx++) {
                    String drillName = String.format("DRL-%03d", (drillIdx + 1));

                    for (int sensorIdx = 0; sensorIdx < sensorCycleNum; sensorIdx++ ) {
                        String sensorType = sensorTypeArr[RandomUtils.nextInt(0, sensorTypeArrLen)];
                        String sensorName = String.format("SNS-%s-%02d",sensorType, (sensorIdx + 1));

                        float readingValue = 0;

                        if (StringUtils.equalsIgnoreCase(sensorType, WorkloadGenUtil.SENSOR_TYPE.TEMP.label)) {
                            readingValue = RandomUtils.nextFloat(200, 500);
                        }
                        else if (StringUtils.equalsIgnoreCase(sensorType, WorkloadGenUtil.SENSOR_TYPE.SPEED.label)) {
                            readingValue = RandomUtils.nextFloat(1000, 3000);
                        }

                        String record = String.format("%s,%s,%s,%s,%s,%.2f\n",
                                drillName, sensorName, sensorType,
                                DateFormatUtils.formatUTC(cycle_time*1000L, "yyyy-MM-dd"),
                                DateFormatUtils.formatUTC(cycle_time*1000L, "yyyy-MM-dd'T'HH:mm:ss"),
                                readingValue);

                        outStream.write(record.getBytes());
                    }
                }
            }

            outStream.close();

        } catch (Exception e) {
            e.printStackTrace();
            System.err.format("\nERROR: Unexpected error: %s.\n", e.getMessage());
            usageAndExit(50);
        }

        System.out.print("Workload generation is complete!\n");
    }
}
