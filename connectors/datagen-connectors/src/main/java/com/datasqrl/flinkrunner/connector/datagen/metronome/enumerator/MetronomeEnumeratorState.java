/*
 * Copyright © 2026 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.flinkrunner.connector.datagen.metronome.enumerator;

import com.datasqrl.flinkrunner.connector.datagen.metronome.split.MetronomeSplit;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/** Checkpointed enumerator state for initial assignment and pending split reassignment. */
@Getter
@RequiredArgsConstructor(access = lombok.AccessLevel.PRIVATE)
public final class MetronomeEnumeratorState {

  private final boolean assigned;
  @Nullable private final MetronomeSplit pendingSplit;

  public static MetronomeEnumeratorState of(boolean assigned) {
    return assigned ? assigned() : unassigned();
  }

  public static MetronomeEnumeratorState assigned() {
    return new MetronomeEnumeratorState(true, null);
  }

  public static MetronomeEnumeratorState unassigned() {
    return new MetronomeEnumeratorState(false, null);
  }

  public static MetronomeEnumeratorState pending(MetronomeSplit split) {
    return new MetronomeEnumeratorState(false, split);
  }
}
