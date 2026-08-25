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
 * Holds a verbatim copy of {@code SourceOperator} carrying the FLINK-40093 fix, which is released
 * upstream in Flink 2.3.1 but not in the 2.3.0 we pin. Without it, a source split that is paused by
 * watermark alignment and marked idle in the same window is never resumed, so it stops emitting
 * records for good.
 *
 * <p>The file is taken unmodified from the {@code release-2.3} backport commit {@code 2a2d3590};
 * the 2.3.0 sources it replaces are byte-identical to that commit's parent, so there is no local
 * merge to maintain. Classes here land in {@code sql-runner.uber.jar}, which sits in {@code
 * /opt/flink/lib} ahead of {@code flink-dist} on the Flink classpath and therefore shadows the
 * unpatched originals.
 *
 * <p>Delete this package when {@code flink.version} moves to 2.3.1 or later. {@code
 * SourceOperatorPatchTest} fails the build on that bump as a reminder.
 *
 * @see <a href="https://issues.apache.org/jira/browse/FLINK-40093">FLINK-40093</a>
 */
package org.apache.flink.streaming.api.operators;
