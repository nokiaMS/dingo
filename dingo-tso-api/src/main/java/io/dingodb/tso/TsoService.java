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

package io.dingodb.tso;

/**
 * TsoService接口定义了一个tso服务需要具备的接口。
 */
public interface TsoService {

    static TsoService getDefault() {
        return TsoServiceProvider.getDefault().get();
    }

    long tso();

    long tso(long timestamp);

    long timestamp();

    long timestamp(long tso);

}
