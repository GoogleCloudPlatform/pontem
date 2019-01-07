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
package com.google.cloud.pontem.auth;

import java.io.IOException;
import java.net.URL;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BigQueryCredentialManager}. */
@RunWith(JUnit4.class)
public class BigQueryCredentialManagerTest {

  BigQueryCredentialManager bigQueryCredentialManager;

  @Before
  public void setUp() {
    bigQueryCredentialManager = new BigQueryCredentialManager();
  }

  @Test(expected = IOException.class)
  public void attemptToReadFromWrongPath() throws Exception {
    Optional<String> fakeCredentialsFile = Optional.of("/fake/path/");

    bigQueryCredentialManager.getCredentialsFromFile(fakeCredentialsFile);
  }

  @Test(expected = IOException.class)
  public void atttemptToReadFromInvalidFile() throws Exception {
    URL resource = getClass().getClassLoader().getResource("fakecreds.json");
    Optional<String> fakeCredentialsFile = Optional.of(resource.getPath());

    bigQueryCredentialManager.getCredentialsFromFile(fakeCredentialsFile);
  }
}
