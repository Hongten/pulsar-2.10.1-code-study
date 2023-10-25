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
package org.apache.pulsar.broker.protocol;

import static com.google.common.base.Preconditions.checkArgument;
import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.nar.NarClassLoaderBuilder;
import org.apache.pulsar.common.util.ObjectMapperFactory;

/**
 * Util class to search and load {@link ProtocolHandler}s.
 */
@UtilityClass
@Slf4j
class ProtocolHandlerUtils {

    // TODO: 12/30/22 kop里面有提供这个文件
    /**
     * name: kafka
     * description: Kafka Protocol Handler
     * handlerClass: io.streamnative.pulsar.handlers.kop.KafkaProtocolHandler
     */
    static final String PULSAR_PROTOCOL_HANDLER_DEFINITION_FILE = "pulsar-protocol-handler.yml";

    /**
     * Retrieve the protocol handler definition from the provided handler nar package.
     *
     * @param narPath the path to the protocol handler NAR package
     * @return the protocol handler definition
     * @throws IOException when fail to load the protocol handler or get the definition
     */
    public static ProtocolHandlerDefinition getProtocolHandlerDefinition(String narPath, String narExtractionDirectory)
            throws IOException {
        // TODO: 2/15/23 加载nar文件
        try (NarClassLoader ncl = NarClassLoaderBuilder.builder()
                .narFile(new File(narPath))
                .extractionDirectory(narExtractionDirectory)
                .build()) {
            return getProtocolHandlerDefinition(ncl);
        }
    }

    private static ProtocolHandlerDefinition getProtocolHandlerDefinition(NarClassLoader ncl) throws IOException {
        // TODO: 2/15/23 pulsar-protocol-handler.yml
        String configStr = ncl.getServiceDefinition(PULSAR_PROTOCOL_HANDLER_DEFINITION_FILE);

        return ObjectMapperFactory.getThreadLocalYaml().readValue(
            configStr, ProtocolHandlerDefinition.class
        );
    }

    /**
     * Search and load the available protocol handlers.
     *
     * @param handlersDirectory the directory where all the protocol handlers are stored
     * @return a collection of protocol handlers
     * @throws IOException when fail to load the available protocol handlers from the provided directory.
     */
    public static ProtocolHandlerDefinitions searchForHandlers(String handlersDirectory,
                                                               String narExtractionDirectory) throws IOException {
        // TODO: 2/15/23 构建路径 
        Path path = Paths.get(handlersDirectory).toAbsolutePath();
        log.info("Searching for protocol handlers in {}", path);

        ProtocolHandlerDefinitions handlers = new ProtocolHandlerDefinitions();
        // TODO: 2/15/23 如果路径不存在，则返回
        if (!path.toFile().exists()) {
            log.warn("Protocol handler directory not found");
            return handlers;
        }

        // TODO: 2/15/23 如果路径存在，则加载nar文件 
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, "*.nar")) {
            for (Path archive : stream) {
                try {
                    ProtocolHandlerDefinition phDef =
                        ProtocolHandlerUtils.getProtocolHandlerDefinition(archive.toString(), narExtractionDirectory);
                    log.info("Found protocol handler from {} : {}", archive, phDef);

                    checkArgument(StringUtils.isNotBlank(phDef.getName()));
                    checkArgument(StringUtils.isNotBlank(phDef.getHandlerClass()));

                    ProtocolHandlerMetadata metadata = new ProtocolHandlerMetadata();
                    // TODO: 2/15/23 协议定义以及路径
                    metadata.setDefinition(phDef);
                    metadata.setArchivePath(archive);
                    // TODO: 2/15/23 把协议放入到tree map中
                    handlers.handlers().put(phDef.getName(), metadata);
                } catch (Throwable t) {
                    log.warn("Failed to load connector from {}."
                        + " It is OK however if you want to use this protocol handler,"
                        + " please make sure you put the correct protocol handler NAR"
                        + " package in the handlers directory.", archive, t);
                }
            }
        }

        return handlers;
    }

    /**
     * Load the protocol handler according to the handler definition.
     *
     * @param metadata the protocol handler definition.
     * @return
     */
    static ProtocolHandlerWithClassLoader load(ProtocolHandlerMetadata metadata,
                                               String narExtractionDirectory) throws IOException {
        // TODO: 12/30/22 加载对应的协议以及对应的处理nar文件 e.g KOP
        final File narFile = metadata.getArchivePath().toAbsolutePath().toFile();
        NarClassLoader ncl = NarClassLoaderBuilder.builder()
                .narFile(narFile)
                .parentClassLoader(ProtocolHandler.class.getClassLoader())
                .extractionDirectory(narExtractionDirectory)
                .build();

        ProtocolHandlerDefinition phDef = getProtocolHandlerDefinition(ncl);
        if (StringUtils.isBlank(phDef.getHandlerClass())) {
            throw new IOException("Protocol handler `" + phDef.getName() + "` does NOT provide a protocol"
                + " handler implementation");
        }

        try {
            // TODO: 12/30/22 在kop里面 - io.streamnative.pulsar.handlers.kop.KafkaRequestHandler
            Class handlerClass = ncl.loadClass(phDef.getHandlerClass());
            Object handler = handlerClass.getDeclaredConstructor().newInstance();
            if (!(handler instanceof ProtocolHandler)) {
                throw new IOException("Class " + phDef.getHandlerClass()
                    + " does not implement protocol handler interface");
            }
            ProtocolHandler ph = (ProtocolHandler) handler;
            // TODO: 2/15/23 创建一个handler实例
            return new ProtocolHandlerWithClassLoader(ph, ncl);
        } catch (Throwable t) {
            rethrowIOException(t);
            return null;
        }
    }

    private static void rethrowIOException(Throwable cause)
            throws IOException {
        if (cause instanceof IOException) {
            throw (IOException) cause;
        } else if (cause instanceof RuntimeException) {
            throw (RuntimeException) cause;
        } else if (cause instanceof Error) {
            throw (Error) cause;
        } else {
            throw new IOException(cause.getMessage(), cause);
        }
    }

}
