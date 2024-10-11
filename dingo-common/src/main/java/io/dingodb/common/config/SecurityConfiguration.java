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

package io.dingodb.common.config;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import io.dingodb.common.util.Optional;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * 配置文件中security段配置。
 */
@Getter
@Setter
@ToString
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class SecurityConfiguration {
    //cipher段配置。
    private CipherConfiguration cipher;

    //ldap段配置。
    private LdapConfiguration ldap;

    //verify字段。
    private boolean verify = true;

    //auth字段。
    private boolean auth = true;

    /**
     * 获得配置文件中的security.auth字段，如果不配置默认为true。
     * @return
     */
    public static boolean isAuth() {
        return Optional.ofNullable(DingoConfiguration.instance())
            .map(DingoConfiguration::getSecurity)
            .map(s -> s.auth)
            .orElse(true);
    }

    /**
     * 获得配置文件中的verify字段，如果没有设置，默认值为true。
     * @return
     */
    public static boolean isVerify() {
        return Optional.ofNullable(DingoConfiguration.instance())
            .map(DingoConfiguration::getSecurity)
            .map(s -> s.verify)
            .orElse(true);
    }

    /**
     * 获得配置文件中的security.cipher段。
     * @return
     */
    public static CipherConfiguration cipher() {
        return Optional.ofNullable(DingoConfiguration.instance())
            .map(DingoConfiguration::getSecurity)
            .map(s -> s.cipher)
            .orNull();
    }

    /**
     * 获得配置文件中的security.ldap段。
     * @return
     */
    public static LdapConfiguration ldap() {
        return Optional.ofNullable(DingoConfiguration.instance())
            .map(DingoConfiguration::getSecurity)
            .map(s -> s.ldap)
            .orNull();
    }

}
