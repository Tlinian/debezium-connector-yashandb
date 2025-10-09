package io.debezium.connector.yashandb;

import io.debezium.relational.TableId;

public record YaShanPartitionSplitRecord(
        TableId info, String partitionName, YaShanRowid minrowid, YaShanRowid maxrowid) {}
