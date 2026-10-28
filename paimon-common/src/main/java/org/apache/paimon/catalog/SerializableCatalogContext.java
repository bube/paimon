/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.catalog;

import org.apache.paimon.fs.FileIOLoader;
import org.apache.paimon.hadoop.SerializableConfiguration;
import org.apache.paimon.options.Options;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * Serialisable form of a {@link CatalogContext}. Java serialisation of {@link CatalogContext}
 * itself fails without Hadoop, because its method signatures name Hadoop's Configuration.
 */
public class SerializableCatalogContext implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Options options;
    @Nullable private final SerializableConfiguration hadoopConf;
    @Nullable private final FileIOLoader preferIOLoader;
    @Nullable private final FileIOLoader fallbackIOLoader;

    private transient CatalogContext context;

    public SerializableCatalogContext(CatalogContext context) {
        this.options = context.options();
        this.hadoopConf = hadoopConf(context);
        this.preferIOLoader = context.preferIO();
        this.fallbackIOLoader = context.fallbackIO();
        this.context = context;
    }

    public CatalogContext get() {
        if (context == null) {
            context =
                    CatalogContext.create(
                            options,
                            hadoopConf == null ? null : hadoopConf.get(),
                            preferIOLoader,
                            fallbackIOLoader);
        }
        return context;
    }

    @Nullable
    private static SerializableConfiguration hadoopConf(CatalogContext context) {
        try {
            return new SerializableConfiguration(context.hadoopConf());
        } catch (IllegalStateException e) {
            // The context was created without Hadoop.
            return null;
        }
    }
}
