/*
 * Copyright (C) 2018 MemVerge Inc.
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
package com.memverge.splash.shared;

import com.memverge.splash.ShuffleFile;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import lombok.val;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SharedFSShuffleFile implements ShuffleFile {

  private static final Logger log = LoggerFactory
      .getLogger(SharedFSShuffleFile.class);

  protected File file;

  SharedFSShuffleFile(String pathname) {
    file = new File(pathname);
  }

  @Override
  public long getSize() {
    return file.length();
  }

  @Override
  public boolean delete() {
    log.debug("delete file {}", getPath());
    boolean success = true;
    try {
      FileUtils.forceDelete(file);
    } catch (FileNotFoundException e) {
      log.info("file to delete {} not found", file.getAbsolutePath());
    } catch (IOException e) {
      log.error("delete {} failed.", getPath(), e);
      success = false;
    }
    return success;
  }

  @Override
  public boolean exists() {
    return file.exists();
  }

  @Override
  public String getPath() {
    return file.getAbsolutePath();
  }

  @Override
  public InputStream makeInputStream() {
    InputStream ret;
    try {
      ret = new FileInputStream(file);
      log.debug("create input stream for {}.", getPath());
    } catch (FileNotFoundException e) {
      val msg = String.format("Create input stream failed for %s.", getPath());
      throw new IllegalArgumentException(msg, e);
    }
    return ret;
  }

  protected File getFile() {
    return file;
  }

  void rename(String tgtId) throws IOException {
    val tgtFile = new File(tgtId);
    val parent = tgtFile.getParentFile();
    if (!parent.exists() && !parent.mkdirs()) {
      if (!parent.exists()) {
        val msg = String.format("create parent folder %s failed",
            parent.getAbsolutePath());
        throw new IOException(msg);
      }
    }
    boolean success = file.renameTo(tgtFile);
    if (success) {
      log.debug("rename {} to {}.", getPath(), tgtId);
    } else {
      val msg = String.format("rename file to %s failed.", tgtId);
      throw new IOException(msg);
    }
  }
}
