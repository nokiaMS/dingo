/*
 * Copyright 2021 DataCanvas
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

package io.dingodb.calcite.stats;

/**
 * States for statistics analyze tasks.
 */
public enum StatsTaskState {
    /** Initial state before analysis is scheduled. */
    INIT("INIT"),
    /** Waiting to be picked up by the analyzer. */
    PENDING("PENDING"),
    /** Analysis is currently running. */
    RUNNING("RUNNING"),
    /** Analysis completed successfully. */
    SUCCESS("SUCCESS"),
    /** Analysis completed with failure. */
    FAIL("FAIL");

    /**
     * Creates a state enum with the provided string value.
     *
     * @param state state string
     */
    StatsTaskState(String state) {
        this.state = state;
    }

    /** String value persisted for this state. */
    private String state;

    /**
     * Returns the string representation of the state.
     *
     * @return state string
     */
    public String getState() {
        return state;
    }
}
