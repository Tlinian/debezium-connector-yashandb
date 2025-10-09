package io.debezium.connector.yashandb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * 此类用于记录YaShan数据库本身的分区信息.
 */
public class YaShanDBPartitionInfo {
    public record SubPartitionInfo(String tableName, String partitionName, long size) {}

    private final String schema;
    private final String tableName;
    private boolean isPartition;
    private List<SubPartitionInfo> subPartitionInfo;

    /** Constructor. */
    public YaShanDBPartitionInfo(final String schema, final String tableName, final boolean isPartition) {
        this.schema = schema;
        this.tableName = tableName;
        this.isPartition = isPartition;
        this.subPartitionInfo = new ArrayList<>();//Collections.emptyList();
    }

    // Getter for schema (final field, no setter)
    public String getSchema() {
        return schema;
    }

    // Getter for name (final field, no setter)
    public String getTableName() {
        return tableName;
    }

    // Getter for isPartition
    public boolean getIsPartition() {
        return isPartition;
    }

    // Setter for isPartition
    public void setIsPartition(boolean partition) {
        isPartition = partition;
    }

    // Getter for subPartitionInfo
    public List<SubPartitionInfo> getSubPartitionInfo() {
        return subPartitionInfo;
    }

    // Setter for subPartitionInfo
    public void setSubPartitionInfo(List<SubPartitionInfo> subPartitionInfo) {
        this.subPartitionInfo = subPartitionInfo;
    }

    // 可选：添加一个便捷方法来添加单个子分区信息
    public void addSubPartitionInfo(SubPartitionInfo info) {
        // 如果当前是空列表，需要先初始化为可修改的列表
        if (this.subPartitionInfo == Collections.EMPTY_LIST) {
            this.subPartitionInfo = new java.util.ArrayList<>();
        }
        this.subPartitionInfo.add(info);
    }
}
