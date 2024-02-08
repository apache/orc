---
layout: docs
title: ORC Java configuration
permalink: /docs/core-java-config.html
---
## Configuration properties

<table class="configtable">
<tr>
  <th>Key</th>
  <th>Default</th>
  <th>Notes</th>
</tr>
<tr>
  <td><code>orc.stripe.size</code></td>
  <td>67108864</td>
  <td>
    Define the default ORC stripe size, in bytes.
  </td>
</tr>
<tr>
  <td><code>orc.stripe.row.count</code></td>
  <td>2147483647</td>
  <td>
    This value limit the row count in one stripe.  The number of stripe rows can be controlled at  (0, "orc.stripe.row.count" + max(batchSize, "orc.rows.between.memory.checks"))
  </td>
</tr>
<tr>
  <td><code>orc.block.size</code></td>
  <td>268435456</td>
  <td>
    Define the default file system block size for ORC files.
  </td>
</tr>
<tr>
  <td><code>orc.create.index</code></td>
  <td>true</td>
  <td>
    Should the ORC writer create indexes as part of the file.
  </td>
</tr>
<tr>
  <td><code>orc.row.index.stride</code></td>
  <td>10000</td>
  <td>
    Define the default ORC index stride in number of rows. (Stride is the  number of rows an index entry represents.)
  </td>
</tr>
<tr>
  <td><code>orc.compress.size</code></td>
  <td>262144</td>
  <td>
    Define the default ORC buffer size, in bytes.
  </td>
</tr>
<tr>
  <td><code>orc.base.delta.ratio</code></td>
  <td>8</td>
  <td>
    The ratio of base writer and delta writer in terms of STRIPE_SIZE and BUFFER_SIZE.
  </td>
</tr>
<tr>
  <td><code>orc.block.padding</code></td>
  <td>true</td>
  <td>
    Define whether stripes should be padded to the HDFS block boundaries.
  </td>
</tr>
<tr>
  <td><code>orc.compress</code></td>
  <td>ZSTD</td>
  <td>
    Define the default compression codec for ORC file. It can be NONE, ZLIB, SNAPPY, LZO, LZ4, ZSTD, BROTLI.
  </td>
</tr>
<tr>
  <td><code>orc.write.format</code></td>
  <td>0.12</td>
  <td>
    Define the version of the file to write. Possible values are 0.11 and  0.12. If this parameter is not defined, ORC will use the run  length encoding (RLE) introduced in Hive 0.12.
  </td>
</tr>
<tr>
  <td><code>orc.buffer.size.enforce</code></td>
  <td>false</td>
  <td>
    Defines whether to enforce ORC compression buffer size.
  </td>
</tr>
<tr>
  <td><code>orc.encoding.strategy</code></td>
  <td>SPEED</td>
  <td>
    Define the encoding strategy to use while writing data. Changing this will only affect the light weight encoding for integers. This flag will not change the compression level of higher level compression codec (like ZLIB).
  </td>
</tr>
<tr>
  <td><code>orc.compression.strategy</code></td>
  <td>SPEED</td>
  <td>
    Define the compression strategy to use while writing data. This changes the compression level of higher level compression codec (like ZLIB).
  </td>
</tr>
<tr>
  <td><code>orc.compression.zstd.level</code></td>
  <td>3</td>
  <td>
    Define the compression level to use with ZStandard codec while writing data. The valid range is 1~22.
  </td>
</tr>
<tr>
  <td><code>orc.compression.zstd.windowlog</code></td>
  <td>0</td>
  <td>
    Set the maximum allowed back-reference distance for ZStandard codec, expressed as power of 2.
  </td>
</tr>
<tr>
  <td><code>orc.block.padding.tolerance</code></td>
  <td>0.05</td>
  <td>
    Define the tolerance for block padding as a decimal fraction of stripe size (for example, the default value 0.05 is 5% of the stripe size). For the defaults of 64Mb ORC stripe and 256Mb HDFS blocks, the default block padding tolerance of 5% will reserve a maximum of 3.2Mb for padding within the 256Mb block. In that case, if the available size within the block is more than 3.2Mb, a new smaller stripe will be inserted to fit within that space. This will make sure that no stripe written will block  boundaries and cause remote reads within a node local task.
  </td>
</tr>
<tr>
  <td><code>orc.bloom.filter.fpp</code></td>
  <td>0.01</td>
  <td>
    Define the default false positive probability for bloom filters.
  </td>
</tr>
<tr>
  <td><code>orc.use.zerocopy</code></td>
  <td>false</td>
  <td>
    Use zerocopy reads with ORC. (This requires Hadoop 2.3 or later.)
  </td>
</tr>
<tr>
  <td><code>orc.skip.corrupt.data</code></td>
  <td>false</td>
  <td>
    If ORC reader encounters corrupt data, this value will be used to determine whether to skip the corrupt data or throw exception. The default behavior is to throw exception.
  </td>
</tr>
<tr>
  <td><code>orc.tolerate.missing.schema</code></td>
  <td>true</td>
  <td>
    Writers earlier than HIVE-4243 may have inaccurate schema metadata. This setting will enable best effort schema evolution rather than rejecting mismatched schemas
  </td>
</tr>
<tr>
  <td><code>orc.memory.pool</code></td>
  <td>0.5</td>
  <td>
    Maximum fraction of heap that can be used by ORC file writers
  </td>
</tr>
<tr>
  <td><code>orc.dictionary.key.threshold</code></td>
  <td>0.8</td>
  <td>
    If the number of distinct keys in a dictionary is greater than this fraction of the total number of non-null rows, turn off  dictionary encoding.  Use 1 to always use dictionary encoding.
  </td>
</tr>
<tr>
  <td><code>orc.dictionary.early.check</code></td>
  <td>true</td>
  <td>
    If enabled dictionary check will happen after first row index stride (default 10000 rows) else dictionary check will happen before writing first stripe. In both cases, the decision to use dictionary or not will be retained thereafter.
  </td>
</tr>
<tr>
  <td><code>orc.dictionary.implementation</code></td>
  <td>rbtree</td>
  <td>
    the implementation for the dictionary used for string-type column encoding. The choices are:  rbtree - use red-black tree as the implementation for the dictionary.  hash - use hash table as the implementation for the dictionary.
  </td>
</tr>
<tr>
  <td><code>orc.bloom.filter.columns</code></td>
  <td></td>
  <td>
    List of columns to create bloom filters for when writing.
  </td>
</tr>
<tr>
  <td><code>orc.bloom.filter.write.version</code></td>
  <td>utf8</td>
  <td>
    (Deprecated) Which version of the bloom filters should we write. The choices are:   original - writes two versions of the bloom filters for use by              both old and new readers.   utf8 - writes just the new bloom filters.
  </td>
</tr>
<tr>
  <td><code>orc.bloom.filter.ignore.non-utf8</code></td>
  <td>false</td>
  <td>
    Should the reader ignore the obsolete non-UTF8 bloom filters.
  </td>
</tr>
<tr>
  <td><code>orc.max.file.length</code></td>
  <td>9223372036854775807</td>
  <td>
    The maximum size of the file to read for finding the file tail. This is primarily used for streaming ingest to read intermediate footers while the file is still open
  </td>
</tr>
<tr>
  <td><code>orc.mapred.input.schema</code></td>
  <td>null</td>
  <td>
    The schema that the user desires to read. The values are interpreted using TypeDescription.fromString.
  </td>
</tr>
<tr>
  <td><code>orc.mapred.map.output.key.schema</code></td>
  <td>null</td>
  <td>
    The schema of the MapReduce shuffle key. The values are interpreted using TypeDescription.fromString.
  </td>
</tr>
<tr>
  <td><code>orc.mapred.map.output.value.schema</code></td>
  <td>null</td>
  <td>
    The schema of the MapReduce shuffle value. The values are interpreted using TypeDescription.fromString.
  </td>
</tr>
<tr>
  <td><code>orc.mapred.output.schema</code></td>
  <td>null</td>
  <td>
    The schema that the user desires to write. The values are interpreted using TypeDescription.fromString.
  </td>
</tr>
<tr>
  <td><code>orc.include.columns</code></td>
  <td>null</td>
  <td>
    The list of comma separated column ids that should be read with 0 being the first column, 1 being the next, and so on. .
  </td>
</tr>
<tr>
  <td><code>orc.kryo.sarg</code></td>
  <td>null</td>
  <td>
    The kryo and base64 encoded SearchArgument for predicate pushdown.
  </td>
</tr>
<tr>
  <td><code>orc.kryo.sarg.buffer</code></td>
  <td>8192</td>
  <td>
    The kryo buffer size for SearchArgument for predicate pushdown.
  </td>
</tr>
<tr>
  <td><code>orc.sarg.column.names</code></td>
  <td>null</td>
  <td>
    The list of column names for the SearchArgument.
  </td>
</tr>
<tr>
  <td><code>orc.force.positional.evolution</code></td>
  <td>false</td>
  <td>
    Require schema evolution to match the top level columns using position rather than column names. This provides backwards compatibility with Hive 2.1.
  </td>
</tr>
<tr>
  <td><code>orc.force.positional.evolution.level</code></td>
  <td>1</td>
  <td>
    Require schema evolution to match the defined no. of level columns using position rather than column names. This provides backwards compatibility with Hive 2.1.
  </td>
</tr>
<tr>
  <td><code>orc.rows.between.memory.checks</code></td>
  <td>5000</td>
  <td>
    How often should MemoryManager check the memory sizes? Measured in rows added to all of the writers.  Valid range is [1,10000] and is primarily meant fortesting.  Setting this too low may negatively affect performance. Use orc.stripe.row.count instead if the value larger than orc.stripe.row.count.
  </td>
</tr>
<tr>
  <td><code>orc.overwrite.output.file</code></td>
  <td>false</td>
  <td>
    A boolean flag to enable overwriting of the output file if it already exists. 
  </td>
</tr>
<tr>
  <td><code>orc.schema.evolution.case.sensitive</code></td>
  <td>true</td>
  <td>
    A boolean flag to determine if the comparision of field names in schema evolution is case sensitive . 
  </td>
</tr>
<tr>
  <td><code>orc.sarg.to.filter</code></td>
  <td>false</td>
  <td>
    A boolean flag to determine if a SArg is allowed to become a filter
  </td>
</tr>
<tr>
  <td><code>orc.filter.use.selected</code></td>
  <td>false</td>
  <td>
    A boolean flag to determine if the selected vector is supported by the reading application. If false, the output of the ORC reader must have the filter reapplied to avoid using unset values in the unselected rows. If unsure please leave this as false.
  </td>
</tr>
<tr>
  <td><code>orc.filter.plugin</code></td>
  <td>false</td>
  <td>
    Enables the use of plugin filters during read. The plugin filters are discovered against the service org.apache.orc.filter.PluginFilterService, if multiple filters are determined, they are combined using AND. The order of application is non-deterministic and the filter functionality should not depend on the order of application.
  </td>
</tr>
<tr>
  <td><code>orc.filter.plugin.allowlist</code></td>
  <td>*</td>
  <td>
    A list of comma-separated class names. If specified it restricts the PluginFilters to just these classes as discovered by the PluginFilterService. The default of * allows all discovered classes and an empty string would not allow any plugins to be applied.
  </td>
</tr>
<tr>
  <td><code>orc.write.variable.length.blocks</code></td>
  <td>false</td>
  <td>
    A boolean flag whether the ORC writer should write variable length HDFS blocks.
  </td>
</tr>
<tr>
  <td><code>orc.column.encoding.direct</code></td>
  <td></td>
  <td>
    Comma-separated list of columns for which dictionary encoding is to be skipped.
  </td>
</tr>
<tr>
  <td><code>orc.max.disk.range.chunk.limit</code></td>
  <td>2147482623</td>
  <td>
    When reading stripes >2GB, specify max limit for the chunk size.
  </td>
</tr>
<tr>
  <td><code>orc.min.disk.seek.size</code></td>
  <td>0</td>
  <td>
    When determining contiguous reads, gaps within this size are read contiguously and not seeked. Default value of zero disables this optimization
  </td>
</tr>
<tr>
  <td><code>orc.min.disk.seek.size.tolerance</code></td>
  <td>0.0</td>
  <td>
    Define the tolerance for extra bytes read as a result of orc.min.disk.seek.size. If the (bytesRead - bytesNeeded) / bytesNeeded is greater than this threshold then extra work is performed to drop the extra bytes from memory after the read.
  </td>
</tr>
<tr>
  <td><code>orc.encrypt</code></td>
  <td>null</td>
  <td>
    The list of keys and columns to encrypt with
  </td>
</tr>
<tr>
  <td><code>orc.mask</code></td>
  <td>null</td>
  <td>
    The masks to apply to the encrypted columns
  </td>
</tr>
<tr>
  <td><code>orc.key.provider</code></td>
  <td>hadoop</td>
  <td>
    The kind of KeyProvider to use for encryption.
  </td>
</tr>
<tr>
  <td><code>orc.proleptic.gregorian</code></td>
  <td>false</td>
  <td>
    Should we read and write dates & times using the proleptic Gregorian calendar instead of the hybrid Julian Gregorian? Hive before 3.1 and Spark before 3.0 used hybrid.
  </td>
</tr>
<tr>
  <td><code>orc.proleptic.gregorian.default</code></td>
  <td>false</td>
  <td>
    This value controls whether pre-ORC 27 files are using the hybrid or proleptic calendar. Only Hive 3.1 and the C++ library wrote using the proleptic, so hybrid is the default.
  </td>
</tr>
<tr>
  <td><code>orc.row.batch.size</code></td>
  <td>1024</td>
  <td>
    The number of rows to include in an orc vectorized reader batch. The value should be carefully chosen to minimize overhead and avoid OOMs in reading data.
  </td>
</tr>
<tr>
  <td><code>orc.row.child.limit</code></td>
  <td>32768</td>
  <td>
    The maximum number of child elements to buffer before the ORC row writer writes the batch to the file.
  </td>
</tr>
</table>
