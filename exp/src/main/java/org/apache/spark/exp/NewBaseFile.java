package org.apache.spark.exp;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.iceberg.*;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.shaded.org.apache.avro.Schema;
import org.apache.iceberg.shaded.org.apache.avro.generic.IndexedRecord;
import org.apache.iceberg.shaded.org.apache.avro.specific.SpecificData;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.SerializableMap;

abstract class NewBaseFile<F> implements ContentFile<F>, IndexedRecord, StructLike, SpecificData.SchemaConstructable, Serializable {
    static final Types.StructType EMPTY_STRUCT_TYPE = Types.StructType.of(new Types.NestedField[0]);
    static final PartitionData EMPTY_PARTITION_DATA;
    private int[] fromProjectionPos;
    private Types.StructType partitionType;
    private Long fileOrdinal = null;
    private int partitionSpecId = -1;
    private FileContent content;
    private String filePath;
    private FileFormat format;
    private PartitionData partitionData;
    private Long recordCount;
    private long fileSizeInBytes;
    private Long dataSequenceNumber;
    private Long fileSequenceNumber;
    private Map<Integer, Long> columnSizes;
    private Map<Integer, Long> valueCounts;
    private Map<Integer, Long> nullValueCounts;
    private Map<Integer, Long> nanValueCounts;
    private Map<Integer, ByteBuffer> lowerBounds;
    private Map<Integer, ByteBuffer> upperBounds;
    private long[] splitOffsets;
    private int[] equalityIds;
    private byte[] keyMetadata;
    private Integer sortOrderId;
    private transient org.apache.iceberg.shaded.org.apache.avro.Schema avroSchema;

    NewBaseFile(org.apache.iceberg.shaded.org.apache.avro.Schema avroSchema) {
        this.content = FileContent.DATA;
        this.filePath = null;
        this.format = null;
        this.partitionData = null;
        this.recordCount = null;
        this.fileSizeInBytes = -1L;
        this.dataSequenceNumber = null;
        this.fileSequenceNumber = null;
        this.columnSizes = null;
        this.valueCounts = null;
        this.nullValueCounts = null;
        this.nanValueCounts = null;
        this.lowerBounds = null;
        this.upperBounds = null;
        this.splitOffsets = null;
        this.equalityIds = null;
        this.keyMetadata = null;
        this.avroSchema = null;
        this.avroSchema = avroSchema;
        Types.StructType schema = AvroSchemaUtil.convert(avroSchema).asNestedType().asStructType();
        Type partType = schema.fieldType("partition");
        if (partType != null) {
            this.partitionType = partType.asNestedType().asStructType();
        } else {
            this.partitionType = EMPTY_STRUCT_TYPE;
        }

        List<Types.NestedField> fields = schema.fields();
        List<Types.NestedField> allFields = Lists.newArrayList();
        allFields.addAll(DataFile.getType(this.partitionType).fields());
        allFields.add(MetadataColumns.ROW_POSITION);
        this.fromProjectionPos = new int[fields.size()];

        for(int i = 0; i < this.fromProjectionPos.length; ++i) {
            boolean found = false;

            for(int j = 0; j < allFields.size(); ++j) {
                if (((Types.NestedField)fields.get(i)).fieldId() == ((Types.NestedField)allFields.get(j)).fieldId()) {
                    found = true;
                    this.fromProjectionPos[i] = j;
                }
            }

            if (!found) {
                throw new IllegalArgumentException("Cannot find projected field: " + fields.get(i));
            }
        }

        this.partitionData = new PartitionData(this.partitionType);
    }

    NewBaseFile(int specId, FileContent content, String filePath, FileFormat format, PartitionData partition, long fileSizeInBytes, long recordCount, Map<Integer, Long> columnSizes, Map<Integer, Long> valueCounts, Map<Integer, Long> nullValueCounts, Map<Integer, Long> nanValueCounts, Map<Integer, ByteBuffer> lowerBounds, Map<Integer, ByteBuffer> upperBounds, List<Long> splitOffsets, int[] equalityFieldIds, Integer sortOrderId, ByteBuffer keyMetadata) {
        this.content = FileContent.DATA;
        this.filePath = null;
        this.format = null;
        this.partitionData = null;
        this.recordCount = null;
        this.fileSizeInBytes = -1L;
        this.dataSequenceNumber = null;
        this.fileSequenceNumber = null;
        this.columnSizes = null;
        this.valueCounts = null;
        this.nullValueCounts = null;
        this.nanValueCounts = null;
        this.lowerBounds = null;
        this.upperBounds = null;
        this.splitOffsets = null;
        this.equalityIds = null;
        this.keyMetadata = null;
        this.avroSchema = null;
        this.partitionSpecId = specId;
        this.content = content;
        this.filePath = filePath;
        this.format = format;
        if (partition == null) {
            this.partitionData = EMPTY_PARTITION_DATA;
            this.partitionType = EMPTY_PARTITION_DATA.getPartitionType();
        } else {
            this.partitionData = partition;
            this.partitionType = partition.getPartitionType();
        }

        this.recordCount = recordCount;
        this.fileSizeInBytes = fileSizeInBytes;
        this.columnSizes = columnSizes;
        this.valueCounts = valueCounts;
        this.nullValueCounts = nullValueCounts;
        this.nanValueCounts = nanValueCounts;
        this.lowerBounds = NewSerializableByteBufferMap.wrap(lowerBounds);
        this.upperBounds = NewSerializableByteBufferMap.wrap(upperBounds);
        this.splitOffsets = ArrayUtil.toLongArray(splitOffsets);
        this.equalityIds = equalityFieldIds;
        this.sortOrderId = sortOrderId;
        this.keyMetadata = ByteBuffers.toByteArray(keyMetadata);
    }

    NewBaseFile(org.apache.spark.exp.NewBaseFile<F> toCopy, boolean copyStats, Set<Integer> requestedColumnIds) {
        this.content = FileContent.DATA;
        this.filePath = null;
        this.format = null;
        this.partitionData = null;
        this.recordCount = null;
        this.fileSizeInBytes = -1L;
        this.dataSequenceNumber = null;
        this.fileSequenceNumber = null;
        this.columnSizes = null;
        this.valueCounts = null;
        this.nullValueCounts = null;
        this.nanValueCounts = null;
        this.lowerBounds = null;
        this.upperBounds = null;
        this.splitOffsets = null;
        this.equalityIds = null;
        this.keyMetadata = null;
        this.avroSchema = null;
        this.fileOrdinal = toCopy.fileOrdinal;
        this.partitionSpecId = toCopy.partitionSpecId;
        this.content = toCopy.content;
        this.filePath = toCopy.filePath;
        this.format = toCopy.format;
        this.partitionData = toCopy.partitionData.copy();
        this.partitionType = toCopy.partitionType;
        this.recordCount = toCopy.recordCount;
        this.fileSizeInBytes = toCopy.fileSizeInBytes;
        if (copyStats) {
            this.columnSizes = copyMap(toCopy.columnSizes, requestedColumnIds);
            this.valueCounts = copyMap(toCopy.valueCounts, requestedColumnIds);
            this.nullValueCounts = copyMap(toCopy.nullValueCounts, requestedColumnIds);
            this.nanValueCounts = copyMap(toCopy.nanValueCounts, requestedColumnIds);
            this.lowerBounds = copyByteBufferMap(toCopy.lowerBounds, requestedColumnIds);
            this.upperBounds = copyByteBufferMap(toCopy.upperBounds, requestedColumnIds);
        } else {
            this.columnSizes = null;
            this.valueCounts = null;
            this.nullValueCounts = null;
            this.nanValueCounts = null;
            this.lowerBounds = null;
            this.upperBounds = null;
        }

        this.fromProjectionPos = toCopy.fromProjectionPos;
        this.keyMetadata = toCopy.keyMetadata == null ? null : Arrays.copyOf(toCopy.keyMetadata, toCopy.keyMetadata.length);
        this.splitOffsets = toCopy.splitOffsets == null ? null : Arrays.copyOf(toCopy.splitOffsets, toCopy.splitOffsets.length);
        this.equalityIds = toCopy.equalityIds != null ? Arrays.copyOf(toCopy.equalityIds, toCopy.equalityIds.length) : null;
        this.sortOrderId = toCopy.sortOrderId;
        this.dataSequenceNumber = toCopy.dataSequenceNumber;
        this.fileSequenceNumber = toCopy.fileSequenceNumber;
    }

    NewBaseFile() {
        this.content = FileContent.DATA;
        this.filePath = null;
        this.format = null;
        this.partitionData = null;
        this.recordCount = null;
        this.fileSizeInBytes = -1L;
        this.dataSequenceNumber = null;
        this.fileSequenceNumber = null;
        this.columnSizes = null;
        this.valueCounts = null;
        this.nullValueCounts = null;
        this.nanValueCounts = null;
        this.lowerBounds = null;
        this.upperBounds = null;
        this.splitOffsets = null;
        this.equalityIds = null;
        this.keyMetadata = null;
        this.avroSchema = null;
    }

    public int specId() {
        return this.partitionSpecId;
    }

    void setSpecId(int specId) {
        this.partitionSpecId = specId;
    }

    public Long dataSequenceNumber() {
        return this.dataSequenceNumber;
    }

    public void setDataSequenceNumber(Long dataSequenceNumber) {
        this.dataSequenceNumber = dataSequenceNumber;
    }

    public Long fileSequenceNumber() {
        return this.fileSequenceNumber;
    }

    public void setFileSequenceNumber(Long fileSequenceNumber) {
        this.fileSequenceNumber = fileSequenceNumber;
    }

    protected abstract org.apache.iceberg.shaded.org.apache.avro.Schema getAvroSchema(Types.StructType var1);

    public Schema getSchema() {
        if (this.avroSchema == null) {
            this.avroSchema = this.getAvroSchema(this.partitionType);
        }

        return this.avroSchema;
    }

    public void put(int i, Object value) {
        int pos = i;
        if (this.fromProjectionPos != null) {
            pos = this.fromProjectionPos[i];
        }

        switch (pos) {
            case 0:
                this.content = value != null ? FileContent.values()[(Integer)value] : FileContent.DATA;
                return;
            case 1:
                this.filePath = value.toString();
                return;
            case 2:
                this.format = FileFormat.fromString(value.toString());
                return;
            case 3:
                this.partitionSpecId = value != null ? (Integer)value : -1;
                return;
            case 4:
                this.partitionData = (PartitionData)value;
                return;
            case 5:
                this.recordCount = (Long)value;
                return;
            case 6:
                this.fileSizeInBytes = (Long)value;
                return;
            case 7:
                this.columnSizes = (Map)value;
                return;
            case 8:
                this.valueCounts = (Map)value;
                return;
            case 9:
                this.nullValueCounts = (Map)value;
                return;
            case 10:
                this.nanValueCounts = (Map)value;
                return;
            case 11:
                this.lowerBounds = NewSerializableByteBufferMap.wrap((Map)value);
                return;
            case 12:
                this.upperBounds = NewSerializableByteBufferMap.wrap((Map)value);
                return;
            case 13:
                this.keyMetadata = ByteBuffers.toByteArray((ByteBuffer)value);
                return;
            case 14:
                this.splitOffsets = ArrayUtil.toLongArray((List)value);
                return;
            case 15:
                this.equalityIds = ArrayUtil.toIntArray((List)value);
                return;
            case 16:
                this.sortOrderId = (Integer)value;
                return;
            case 17:
                this.fileOrdinal = (Long)value;
                return;
            default:
        }
    }

    public <T> void set(int pos, T value) {
        this.put(pos, value);
    }

    public Object get(int i) {
        int pos = i;
        if (this.fromProjectionPos != null) {
            pos = this.fromProjectionPos[i];
        }

        switch (pos) {
            case 0:
                return this.content.id();
            case 1:
                return this.filePath;
            case 2:
                return this.format != null ? this.format.toString() : null;
            case 3:
                return this.partitionSpecId;
            case 4:
                return this.partitionData;
            case 5:
                return this.recordCount;
            case 6:
                return this.fileSizeInBytes;
            case 7:
                return this.columnSizes;
            case 8:
                return this.valueCounts;
            case 9:
                return this.nullValueCounts;
            case 10:
                return this.nanValueCounts;
            case 11:
                return this.lowerBounds;
            case 12:
                return this.upperBounds;
            case 13:
                return this.keyMetadata();
            case 14:
                return this.splitOffsets();
            case 15:
                return this.equalityFieldIds();
            case 16:
                return this.sortOrderId;
            case 17:
                return this.fileOrdinal;
            default:
                throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
        }
    }

    public <T> T get(int pos, Class<T> javaClass) {
        return (T)javaClass.cast(this.get(pos));
    }

    public int size() {
        return DataFile.getType(EMPTY_STRUCT_TYPE).fields().size();
    }

    public Long pos() {
        return this.fileOrdinal;
    }

    public FileContent content() {
        return this.content;
    }

    public CharSequence path() {
        return this.filePath;
    }

    public FileFormat format() {
        return this.format;
    }

    public StructLike partition() {
        return this.partitionData;
    }

    public long recordCount() {
        return this.recordCount;
    }

    public long fileSizeInBytes() {
        return this.fileSizeInBytes;
    }

    public Map<Integer, Long> columnSizes() {
        return toReadableMap(this.columnSizes);
    }

    public Map<Integer, Long> valueCounts() {
        return toReadableMap(this.valueCounts);
    }

    public Map<Integer, Long> nullValueCounts() {
        return toReadableMap(this.nullValueCounts);
    }

    public Map<Integer, Long> nanValueCounts() {
        return toReadableMap(this.nanValueCounts);
    }

    public Map<Integer, ByteBuffer> lowerBounds() {
        return toReadableByteBufferMap(this.lowerBounds);
    }

    public Map<Integer, ByteBuffer> upperBounds() {
        return toReadableByteBufferMap(this.upperBounds);
    }

    public ByteBuffer keyMetadata() {
        return this.keyMetadata != null ? ByteBuffer.wrap(this.keyMetadata) : null;
    }

    public List<Long> splitOffsets() {
        return this.hasWellDefinedOffsets() ? ArrayUtil.toUnmodifiableLongList(this.splitOffsets) : null;
    }

    long[] splitOffsetArray() {
        return this.hasWellDefinedOffsets() ? this.splitOffsets : null;
    }

    private boolean hasWellDefinedOffsets() {
        return this.splitOffsets != null && this.splitOffsets.length != 0 && this.splitOffsets[this.splitOffsets.length - 1] < this.fileSizeInBytes;
    }

    public List<Integer> equalityFieldIds() {
        return ArrayUtil.toIntList(this.equalityIds);
    }

    public Integer sortOrderId() {
        return this.sortOrderId;
    }

    private static <K, V> Map<K, V> copyMap(Map<K, V> map, Set<K> keys) {
        return keys == null ? SerializableMap.copyOf(map) : SerializableMap.filteredCopyOf(map, keys);
    }

    private static Map<Integer, ByteBuffer> copyByteBufferMap(Map<Integer, ByteBuffer> map, Set<Integer> keys) {
        return NewSerializableByteBufferMap.wrap(copyMap(map, keys));
    }

    private static <K, V> Map<K, V> toReadableMap(Map<K, V> map) {
        if (map == null) {
            return null;
        } else {
            return map instanceof SerializableMap ? ((SerializableMap)map).immutableMap() : Collections.unmodifiableMap(map);
        }
    }

    private static Map<Integer, ByteBuffer> toReadableByteBufferMap(Map<Integer, ByteBuffer> map) {
        if (map == null) {
            return null;
        } else {
            return map instanceof NewSerializableByteBufferMap ? ((NewSerializableByteBufferMap)map).immutableMap() : Collections.unmodifiableMap(map);
        }
    }

    public String toString() {
        return MoreObjects.toStringHelper(this).add("content", this.content.toString().toLowerCase(Locale.ROOT)).add("file_path", this.filePath).add("file_format", this.format).add("spec_id", this.specId()).add("partition", this.partitionData).add("record_count", this.recordCount).add("file_size_in_bytes", this.fileSizeInBytes).add("column_sizes", this.columnSizes).add("value_counts", this.valueCounts).add("null_value_counts", this.nullValueCounts).add("nan_value_counts", this.nanValueCounts).add("lower_bounds", this.lowerBounds).add("upper_bounds", this.upperBounds).add("key_metadata", this.keyMetadata == null ? "null" : "(redacted)").add("split_offsets", this.splitOffsets == null ? "null" : this.splitOffsets()).add("equality_ids", this.equalityIds == null ? "null" : this.equalityFieldIds()).add("sort_order_id", this.sortOrderId).add("data_sequence_number", this.dataSequenceNumber == null ? "null" : this.dataSequenceNumber).add("file_sequence_number", this.fileSequenceNumber == null ? "null" : this.fileSequenceNumber).toString();
    }

    static {
        EMPTY_PARTITION_DATA = new PartitionData(EMPTY_STRUCT_TYPE) {
            public PartitionData copy() {
                return this;
            }
        };
    }
}