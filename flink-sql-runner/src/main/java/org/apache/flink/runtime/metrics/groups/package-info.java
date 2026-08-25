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

/**
 * Holds a verbatim copy of {@code InternalSourceSplitMetricGroup} carrying the FLINK-40093 fix,
 * which is released upstream in Flink 2.3.1 but not in the 2.3.0 we pin. A split racing between
 * "paused" and "idle" is benign once {@code SourceOperator} can resume idle splits, so the warning
 * it used to log is demoted to info.
 *
 * <p>See {@link org.apache.flink.streaming.api.operators} for why these copies exist and when to
 * remove them.
 *
 * @see <a href="https://issues.apache.org/jira/browse/FLINK-40093">FLINK-40093</a>
 */
package org.apache.flink.runtime.metrics.groups;
