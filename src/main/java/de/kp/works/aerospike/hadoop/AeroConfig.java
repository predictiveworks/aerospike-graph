package de.kp.works.aerospike.hadoop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class AeroConfig {

    private static final Log log = LogFactory.getLog(AeroConfig.class);
    private final Configuration conf;

    // ---------------- INPUT ----------------

    public static final String INPUT_HOST = "aerospike.input.host";
    public static final String DEFAULT_INPUT_HOST = "localhost";
    public static final String INPUT_PORT = "aerospike.input.port";
    public static final int DEFAULT_INPUT_PORT = 3000;
    public static final String INPUT_NAMESPACE = "aerospike.input.namespace";
    public static final String INPUT_SETNAME = "aerospike.input.setname";
    public static final String INPUT_BINNAMES = "aerospike.input.binnames";
    public static final String DEFAULT_INPUT_BINNAMES = "";
    public static final String INPUT_OPERATION = "aerospike.input.operation";
    public static final String DEFAULT_INPUT_OPERATION = "scan";
    public static final String INPUT_NUMRANGE_BIN = "aerospike.input.numrange.bin";
    public static final String INPUT_NUMRANGE_BEGIN = "aerospike.input.numrange.begin";
    public static final String INPUT_NUMRANGE_END = "aerospike.input.numrange.end";
    /**
     * The timeout of an Aerospike database connection
     * in milliseconds. Default is 1000.
     */
    public static final String INPUT_TIMEOUT  = "aerospike.input.timeout";
    public static final int DEFAULT_INPUT_TIMEOUT = 1000;


    public static final long INVALID_LONG = 762492121482318889L;

    // ---------------- OUTPUT ----------------

    public static final String OUTPUT_HOST = "aerospike.output.host";
    public static final String DEFAULT_OUTPUT_HOST = "localhost";
    public static final String OUTPUT_PORT = "aerospike.output.port";
    public static final int DEFAULT_OUTPUT_PORT = 3000;
    public static final String OUTPUT_NAMESPACE = "aerospike.output.namespace";
    public static final String OUTPUT_SETNAME = "aerospike.output.setname";
    public static final String OUTPUT_BINNAME = "aerospike.output.binname";
    public static final String OUTPUT_KEYNAME = "aerospike.output.keyname";

    public AeroConfig(Configuration conf) {
        this.conf = conf;
    }

    // ---------------- INPUT ----------------

    public static void setInputHost(Configuration conf, String host) {
        log.info("setting " + INPUT_HOST + " to " + host);
        conf.set(INPUT_HOST, host);
    }

    public String getInputHost() {
        String host = conf.get(INPUT_HOST, DEFAULT_INPUT_HOST);
        log.info("using " + INPUT_HOST + " = " + host);
        return host;
    }

    public static void setInputPort(Configuration conf, int port) {
        log.info("setting " + INPUT_PORT + " to " + port);
        conf.setInt(INPUT_PORT, port);
    }

    public int getInputPort() {
        int port = conf.getInt(INPUT_PORT, DEFAULT_INPUT_PORT);
        log.info("using " + INPUT_PORT + " = " + port);
        return port;
    }

    public static void setInputNamespace(Configuration conf, String namespace) {
        log.info("setting " + INPUT_NAMESPACE + " to " + namespace);
        conf.set(INPUT_NAMESPACE, namespace);
    }

    public String getInputNamespace() {
        String namespace = conf.get(INPUT_NAMESPACE);
        if (namespace == null)
            throw new UnsupportedOperationException(
                    "you must set the input namespace");
        log.info("using " + INPUT_NAMESPACE + " = " + namespace);
        return namespace;
    }

    public static void setInputSetName(Configuration conf, String setname) {
        log.info("setting " + INPUT_SETNAME + " to " + setname);
        conf.set(INPUT_SETNAME, setname);
    }

    public String getInputSetName() {
        String setname = conf.get(INPUT_SETNAME);
        log.info("using " + INPUT_SETNAME + " = " + setname);
        return setname;
    }

    public int getInputTimeout() {
        int timeout = conf.getInt(INPUT_TIMEOUT, DEFAULT_INPUT_TIMEOUT);
        log.info("using " + INPUT_TIMEOUT + " = " + timeout);
        return timeout;
    }

    public static void setInputBinNames(Configuration conf, String bins) {
        log.info("setting " + INPUT_BINNAMES + " to " + bins);
        conf.set(INPUT_BINNAMES, bins);
    }

    public String[] getInputBinNames() {
        String bins = conf.get(INPUT_BINNAMES);
        log.info("using " + INPUT_BINNAMES + " = " + bins);
        if (bins == null || bins.equals(""))
            return null;
        else
            return bins.split(",");
    }

    public static void setInputOperation(Configuration conf, String operation) {
        if (!operation.equals("scan") && !operation.equals("numrange"))
            throw new UnsupportedOperationException(
                    "input operation must be 'scan' or 'numrange'");
        log.info("setting " + INPUT_OPERATION + " to " + operation);
        conf.set(INPUT_OPERATION, operation);
    }

    public String getInputOperation() {
        String operation = conf.get(INPUT_OPERATION, DEFAULT_INPUT_OPERATION);
        if (!operation.equals("scan") && !operation.equals("numrange"))
            throw new UnsupportedOperationException(
                    "input operation must be 'scan' or 'numrange'");
        log.info("using " + INPUT_OPERATION + " = " + operation);
        return operation;
    }

    public static void setInputNumRangeBin(Configuration conf, String binname) {
        log.info("setting " + INPUT_NUMRANGE_BIN + " to " + binname);
        conf.set(INPUT_NUMRANGE_BIN, binname);
    }

    public String getInputNumRangeBin() {
        String binName = conf.get(INPUT_NUMRANGE_BIN);
        log.info("using " + INPUT_NUMRANGE_BIN + " = " + binName);
        return binName;
    }

    public static void setInputNumRangeBegin(Configuration conf, long begin) {
        log.info("setting " + INPUT_NUMRANGE_BEGIN + " to " + begin);
        conf.setLong(INPUT_NUMRANGE_BEGIN, begin);
    }

    public long getInputNumRangeBegin() {

        long begin = conf.getLong(INPUT_NUMRANGE_BEGIN, INVALID_LONG);
        String operation = conf.get(INPUT_OPERATION, DEFAULT_INPUT_OPERATION);

        if (begin == INVALID_LONG && operation.equals("numrange"))
            throw new UnsupportedOperationException(
                    "missing input numrange begin");
        log.info("using " + INPUT_NUMRANGE_BEGIN + " = " + begin);
        return begin;

    }

    public static void setInputNumRangeEnd(Configuration conf, long end) {
        log.info("setting " + INPUT_NUMRANGE_END + " to " + end);
        conf.setLong(INPUT_NUMRANGE_END, end);
    }

    public long getInputNumRangeEnd() {

        long end = conf.getLong(INPUT_NUMRANGE_END, INVALID_LONG);
        String operation = conf.get(INPUT_OPERATION, DEFAULT_INPUT_OPERATION);

        if (end == INVALID_LONG && operation.equals("numrange"))
            throw new UnsupportedOperationException(
                    "missing input numrange end");
        log.info("using " + INPUT_NUMRANGE_END + " = " + end);
        return end;

    }

    // ---------------- OUTPUT ----------------

    public static void setOutputHost(Configuration conf, String host) {
        log.info("setting " + OUTPUT_HOST + " to " + host);
        conf.set(OUTPUT_HOST, host);
    }

    public static String getOutputHost(Configuration conf) {
        String host = conf.get(OUTPUT_HOST, DEFAULT_OUTPUT_HOST);
        log.info("using " + OUTPUT_HOST + " = " + host);
        return host;
    }

    public static void setOutputPort(Configuration conf, int port) {
        log.info("setting " + OUTPUT_PORT + " to " + port);
        conf.setInt(OUTPUT_PORT, port);
    }

    public static int getOutputPort(Configuration conf) {
        int port = conf.getInt(OUTPUT_PORT, DEFAULT_OUTPUT_PORT);
        log.info("using " + OUTPUT_PORT + " = " + port);
        return port;
    }

    public static void setOutputNamespace(Configuration conf, String namespace) {
        log.info("setting " + OUTPUT_NAMESPACE + " to " + namespace);
        conf.set(OUTPUT_NAMESPACE, namespace);
    }

    public static String getOutputNamespace(Configuration conf) {
        String namespace = conf.get(OUTPUT_NAMESPACE);
        if (namespace == null)
            throw new UnsupportedOperationException(
                    "you must set the output namespace");
        log.info("using " + OUTPUT_NAMESPACE + " = " + namespace);
        return namespace;
    }

    public static void setOutputSetName(Configuration conf, String setname) {
        log.info("setting " + OUTPUT_SETNAME + " to " + setname);
        conf.set(OUTPUT_SETNAME, setname);
    }

    public static String getOutputSetName(Configuration conf) {
        String setname = conf.get(OUTPUT_SETNAME);
        log.info("using " + OUTPUT_SETNAME + " = " + setname);
        return setname;
    }

    public static void setOutputBinName(Configuration conf, String binname) {
        log.info("setting " + OUTPUT_BINNAME + " to " + binname);
        conf.set(OUTPUT_BINNAME, binname);
    }

    public static String getOutputBinName(Configuration conf) {
        String binname = conf.get(OUTPUT_BINNAME);
        log.info("using " + OUTPUT_BINNAME + " = " + binname);
        return binname;
    }

    public static void setOutputKeyName(Configuration conf, String keyname) {
        log.info("setting " + OUTPUT_KEYNAME + " to " + keyname);
        conf.set(OUTPUT_KEYNAME, keyname);
    }

    public static String getOutputKeyName(Configuration conf) {
        String keyname = conf.get(OUTPUT_KEYNAME);
        log.info("using " + OUTPUT_KEYNAME + " = " + keyname);
        return keyname;
    }

}