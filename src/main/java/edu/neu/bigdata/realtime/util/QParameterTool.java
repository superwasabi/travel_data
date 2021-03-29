package edu.neu.bigdata.realtime.util;

import org.apache.commons.lang3.math.NumberUtils;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
*@Description  请求参数工具类
**/
public class QParameterTool implements Serializable, Cloneable{

    private static final long serialVersionUID = 1L;

    protected static final String NO_VALUE_KEY = "__NO_VALUE_KEY";
    protected static final String DEFAULT_UNDEFINED = "<undefined>";

    // ------------------ Constructors ------------------------

    /**
     * Returns {@link QParameterTool} for the given arguments. The arguments are keys followed by values.
     * Keys have to start with '-' or '--'
     *
     * <p><strong>Example arguments:</strong>
     * --key1 value1 --key2 value2 -key3 value3
     *
     * @param args Input array arguments
     * @return A {@link QParameterTool}
     */
    public static QParameterTool fromArgs(String[] args) {
        final Map<String, String> map = new HashMap<>(args.length / 2);

        int i = 0;
        while (i < args.length) {
            final String key;

            if (args[i].startsWith("--")) {
                key = args[i].substring(2);
            } else if (args[i].startsWith("-")) {
                key = args[i].substring(1);
            } else {
                throw new IllegalArgumentException(
                        String.format("Error parsing arguments '%s' on '%s'. Please prefix keys with -- or -.",
                                Arrays.toString(args), args[i]));
            }

            if (key.isEmpty()) {
                throw new IllegalArgumentException(
                        "The input " + Arrays.toString(args) + " contains an empty argument");
            }

            i += 1; // try to find the value

            if (i >= args.length) {
                map.put(key, NO_VALUE_KEY);
            } else if (NumberUtils.isNumber(args[i])) {
                map.put(key, args[i]);
                i += 1;
            } else if (args[i].startsWith("--") || args[i].startsWith("-")) {
                // the argument cannot be a negative number because we checked earlier
                // -> the next argument is a parameter name
                map.put(key, NO_VALUE_KEY);
            } else {
                map.put(key, args[i]);
                i += 1;
            }
        }

        return fromMap(map);
    }

    /**
     * Returns {@link QParameterTool} for the given map.
     *
     * @param map A map of arguments. Both Key and Value have to be Strings
     * @return A {@link QParameterTool}
     */
    public static QParameterTool fromMap(Map<String, String> map) {
        return new QParameterTool(map);
    }

    // ------------------ QParameterTool  ------------------------
    public  Map<String, String> data = null;

    // data which is only used on the client and does not need to be transmitted
    protected transient Map<String, String> defaultData;
    protected transient Set<String> unrequestedParameters;

    private QParameterTool(Map<String, String> data) {
        this.data = Collections.unmodifiableMap(new HashMap<>(data));

        this.defaultData = new ConcurrentHashMap<>(data.size());

        this.unrequestedParameters = Collections.newSetFromMap(new ConcurrentHashMap<>(data.size()));

        unrequestedParameters.addAll(data.keySet());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QParameterTool that = (QParameterTool) o;
        return Objects.equals(data, that.data) &&
                Objects.equals(defaultData, that.defaultData) &&
                Objects.equals(unrequestedParameters, that.unrequestedParameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data, defaultData, unrequestedParameters);
    }

    // ------------------ Get data from the util ----------------

    public String get(String key) {
        addToDefaults(key, null);
        unrequestedParameters.remove(key);
        return data.get(key);
    }

    /**
     * Returns the String value for the given key.
     * If the key does not exist it will throw a {@link RuntimeException}.
     */
    public String getRequired(String key) {
        addToDefaults(key, null);
        String value = get(key);
        if (value == null) {
            throw new RuntimeException("No data for required key '" + key + "'");
        }
        return value;
    }



    // -------------- Integer

    /**
     * Returns the Integer value for the given key.
     * The method fails if the key does not exist or the value is not an Integer.
     */
    public int getInt(String key) {
        addToDefaults(key, null);
        String value = getRequired(key);
        return Integer.parseInt(value);
    }

    // --------------- Internals

    protected void addToDefaults(String key, String value) {
        final String currentValue = defaultData.get(key);
        if (currentValue == null) {
            if (value == null) {
                value = DEFAULT_UNDEFINED;
            }
            defaultData.put(key, value);
        } else {
            // there is already an entry for this key. Check if the value is the undefined
            if (currentValue.equals(DEFAULT_UNDEFINED) && value != null) {
                // update key with better default value
                defaultData.put(key, value);
            }
        }
    }

    // ------------------------- Export to different targets -------------------------



    protected Object clone() throws CloneNotSupportedException {
        return new QParameterTool(this.data);
    }
    // ------------------------- Serialization ---------------------------------------------

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        defaultData = new ConcurrentHashMap<>(data.size());
        unrequestedParameters = Collections.newSetFromMap(new ConcurrentHashMap<>(data.size()));
    }

}
