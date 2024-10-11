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

package io.dingodb.meta;

/**
 * schema同步服务接口。
 */
public interface SchemaSyncerService {
    static SchemaSyncerService root() {
        return SchemaSyncerServiceProvider.getDefault().root();
    }

    void updateSelfVersion(long startTs, long jobId, long schemaVersion);

    /**
     * owner负责确保所有节点都同步到了最新的版本。
     * @param jobId
     * @param latestVer
     * @param reorg
     * @return
     */
    String ownerCheckAllVersions(long jobId, long latestVer, boolean reorg);

    /**
     * owner更新全局globalSchemaVer版本号。
     * @param version
     */
    void ownerUpdateGlobalVersion(long version);

    void removeSelfVersionPath();

    void ownerUpdateExpVersion(long version);

}
