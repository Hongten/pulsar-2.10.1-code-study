/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.api;

import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * The compression type that can be specified on a {@link Producer}.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public enum CompressionType {
    // TODO: 10/18/23 这里和Kafka生产端的压缩类型有一些不同： kafka(2.7)支持的压缩类型为none, gzip, snappy, lz4, zstd
    /** No compression. */
    NONE,

    /** Compress with LZ4 algorithm. Faster but lower compression than ZLib. */
    LZ4,

    /** Compress with ZLib. */
    ZLIB, //todo pulsar支持zlib， kafka支持gzip

    /** Compress with Zstandard codec. */
    ZSTD,

    /** Compress with Snappy codec. */
    SNAPPY
}
