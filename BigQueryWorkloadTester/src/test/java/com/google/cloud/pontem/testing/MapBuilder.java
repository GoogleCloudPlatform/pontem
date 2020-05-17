/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.pontem.testing;

import com.google.common.collect.ImmutableMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MapBuilder<K, V> {

  private static <K, V> Map<K, V> getMapFromLists(List<K> keys, List<V> values) {
    if (keys.size() != values.size()) {
      throw new IllegalArgumentException("Key and Value sizes don't match!");
    }

    // Ensure Map load factor is under 75%
    int size = (int) Math.ceil(keys.size() * 1.4);
    Map<K, V> map = new LinkedHashMap<>(size);

    for (int i = 0; i < keys.size(); i++) {
      map.put(keys.get(i), values.get(i));
    }

    return map;
  }

  public static <K, V> Map<K, V> getImmutableMapFromLists(List<K> keys, List<V> values) {
    return ImmutableMap.copyOf(getMapFromLists(keys, values));
  }
}
