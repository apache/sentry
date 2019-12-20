/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.tests.e2e.hive;


import java.util.Arrays;
import java.util.Collection;
import org.apache.sentry.core.model.db.DBModelAction;
import org.junit.BeforeClass;
import org.junit.runners.Parameterized;

public class TestShowMetadataPrivilegesOnSelectOnly extends TestShowMetadataPrivileges {

    @Parameterized.Parameters
    public static Collection describePrivilegesOnSelect() {
        return Arrays.asList(new Object[][] {
            { null,                  NOT_ALLOWED }, // Means no privileges
            { DBModelAction.ALL,     ALLOWED },
            { DBModelAction.CREATE,  NOT_ALLOWED },
            { DBModelAction.SELECT,  ALLOWED },
            { DBModelAction.INSERT,  NOT_ALLOWED },
            { DBModelAction.ALTER,   NOT_ALLOWED },
            { DBModelAction.DROP,    NOT_ALLOWED },
            { DBModelAction.INDEX,   NOT_ALLOWED },
            { DBModelAction.LOCK,    NOT_ALLOWED },
        });
    }

    @BeforeClass
    public static void setupSelectOnlyTestStaticConfiguration() throws Exception {
        showDbOnSelectOnly = true;
        showTableOnSelectOnly = true;
        setupTestStaticConfiguration();
    }

    public TestShowMetadataPrivilegesOnSelectOnly(DBModelAction action, boolean allowed) {
        super(action, allowed);
    }

}
