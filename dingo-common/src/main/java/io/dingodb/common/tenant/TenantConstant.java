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

package io.dingodb.common.tenant;

/**
 * 设置租户id TENANT_ID 常量值。
 */
public class TenantConstant {

    /**
     * 租户id值。
     */
    public static long TENANT_ID;

    /**
     * 构造函数
     * @param tenantId  租户id。
     */
    public static void tenant(long tenantId) {
        TENANT_ID = tenantId;
    }
}
