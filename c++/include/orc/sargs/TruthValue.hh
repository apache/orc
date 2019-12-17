/**
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

#ifndef ORC_TRUTHVALUE_HH
#define ORC_TRUTHVALUE_HH

namespace orc {

  enum class TruthValue {
    YES, NO, IS_NULL, YES_NULL, NO_NULL, YES_NO, YES_NO_NULL
  };

  // Compute logical or between the two values.
  TruthValue operator||(TruthValue left, TruthValue right);

  // Compute logical AND between the two values.
  TruthValue operator&&(TruthValue left, TruthValue right);

  // Compute logical NOT for one value.
  TruthValue operator!(TruthValue val);

  // Do we need to read the data based on the TruthValue?
  bool isNeeded(TruthValue val);

} // namespace orc

#endif //ORC_TRUTHVALUE_HH
