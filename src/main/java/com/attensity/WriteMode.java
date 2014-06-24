package com.attensity;

/**
 * @author lmedina
 */
public enum WriteMode {
    MAPR_RAW_UNCOMPRESSED("mapRRawUncompressed"),
    MAPR_RAW_COMPRESSED("mapRRawCompressed"),
    MAPR_HIVE_UNCOMPRESSED("mapRHiveUncompressed"),
    MAPR_HIVE_COMPRESSED("mapRHiveCompressed");

    private final String value;

    private WriteMode(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
