<!---
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->
# File Format Benchmarks

These big data file format benchmarks, compare:

* Avro
* Json
* ORC
* Parquet

There are three sub-modules to try to mitigate dependency hell:

* core - the shared part of the benchmarks
* hive - the Hive benchmarks
* spark - the Spark benchmarks

To build this library:

```% mvn clean package```

To fetch the source data:

```% ./fetch-data.sh```

To generate the derived data:

```% java -jar core/target/orc-benchmarks-core-*-uber.jar generate data```

To run a scan of all of the data:

```% java -jar core/target/orc-benchmarks-core-*-uber.jar scan data```

To run full read benchmark:

```% java -jar hive/target/orc-benchmarks-hive-*-uber.jar read-all data```

To run column projection benchmark:

```% java -jar hive/target/orc-benchmarks-hive-*-uber.jar read-some data```

To run decimal/decimal64 benchmark:

```% java -jar hive/target/orc-benchmarks-hive-*-uber.jar decimal data```

To run spark benchmark:

```% java -jar spark/target/orc-benchmarks-spark-*.jar spark data```

