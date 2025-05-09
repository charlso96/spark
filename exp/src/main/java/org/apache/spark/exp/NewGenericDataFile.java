package org.apache.spark.exp;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.shaded.org.apache.avro.Schema;
import org.apache.iceberg.types.Types;

class NewGenericDataFile extends NewBaseFile<DataFile> implements DataFile {
    NewGenericDataFile(Schema avroSchema) {
        super(avroSchema);
    }

    NewGenericDataFile(int specId, String filePath, FileFormat format, PartitionData partition, long fileSizeInBytes, long recordCount, Map<Integer, Long> columnSizes, Map<Integer, Long> valueCounts, Map<Integer, Long> nullValueCounts, Map<Integer, Long> nanValueCounts, Map<Integer, ByteBuffer> lowerBounds, Map<Integer, ByteBuffer> upperBounds, List<Long> splitOffsets, Integer sortOrderId, ByteBuffer keyMetadata) {
        super(specId, FileContent.DATA, filePath, format, partition, fileSizeInBytes, recordCount, columnSizes, valueCounts, nullValueCounts, nanValueCounts, lowerBounds, upperBounds, splitOffsets, (int[])null, sortOrderId, keyMetadata);
    }

    private NewGenericDataFile(NewGenericDataFile toCopy, boolean copyStats, Set<Integer> requestedColumnIds) {
        super(toCopy, copyStats, requestedColumnIds);
    }

    NewGenericDataFile() {
    }

    public DataFile copyWithoutStats() {
        return new NewGenericDataFile(this, false, (Set)null);
    }

    public DataFile copyWithStats(Set<Integer> requestedColumnIds) {
        return new NewGenericDataFile(this, true, requestedColumnIds);
    }

    public DataFile copy() {
        return new NewGenericDataFile(this, true, (Set)null);
    }

    protected Schema getAvroSchema(Types.StructType partitionStruct) {
        Types.StructType type = DataFile.getType(partitionStruct);
        return AvroSchemaUtil.convert(type, ImmutableMap.of(type, NewGenericDataFile.class.getName(), partitionStruct, PartitionData.class.getName()));
    }
}
