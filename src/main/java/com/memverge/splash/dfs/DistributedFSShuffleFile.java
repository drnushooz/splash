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
package com.memverge.splash.dfs;

import com.memverge.splash.ShuffleFile;
import lombok.val;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Optional;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;

public class DistributedFSShuffleFile implements ShuffleFile {

  private static final Logger log = LoggerFactory.getLogger(DistributedFSShuffleFile.class);
  private static final URI shuffleFileSystem;
  private static final FileContext fileContext;

  static {
    val hadoopConf = FileContextManager.getHadoopConf();
    shuffleFileSystem = URI.create(hadoopConf.get(FS_DEFAULT_NAME_KEY));
    fileContext = FileContextManager.getOrCreate();
    log.info("file system for dfs shuffle {}", shuffleFileSystem);
  }

  protected Path underlyingFilePath;

  DistributedFSShuffleFile(String path) {
    this.underlyingFilePath = new Path(overlapConcat(shuffleFileSystem.toString(), path));
  }

  @Override
  public long getSize() {
    long len;
    try {
      len = fileContext.getFileStatus(underlyingFilePath).getLen();
    } catch (IOException ioe) {
      val msg = String.format("get size failed for %s.", getPath());
      throw new IllegalArgumentException(msg, ioe);
    }
    return len;
  }

  @Override
  public boolean delete() {
    log.debug("delete file {}", getPath());
    boolean success = true;
    try {
      success = fileContext.delete(underlyingFilePath, true);
    } catch (FileNotFoundException e) {
      log.info("file to delete {} not found", getPath());
    } catch (IOException e) {
      log.error("delete {} failed.", getPath(), e);
      success = false;
    }
    return success;
  }

  @Override
  public boolean exists() throws IOException {
    return fileContext.util().exists(underlyingFilePath);
  }

  @Override
  public String getPath() {
    return underlyingFilePath.toString();
  }

  @Override
  public InputStream makeInputStream() {
    InputStream distFileStream;
    try {
      distFileStream = fileContext.open(underlyingFilePath);
      log.debug("create input stream for {}.", getPath());
    } catch (IOException e) {
      val msg = String.format("Create input stream failed for %s.", getPath());
      throw new IllegalArgumentException(msg, e);
    }
    return distFileStream;
  }

  void rename(String tgtId) throws IOException {
    val nonOverlappingDest = new Path(overlapConcat(getPath(), tgtId));
    val destParent = nonOverlappingDest.getParent();
    if (!fileContext.util().exists(destParent)) {
      fileContext.mkdir(destParent, null, true);
      if (!fileContext.util().exists(destParent)) {
        val msg = String.format("create parent folder %s failed", destParent);
        throw new IOException(msg);
      }
    }
    fileContext.rename(underlyingFilePath, nonOverlappingDest, Options.Rename.OVERWRITE);
    if (fileContext.util().exists(nonOverlappingDest)) {
      log.debug("rename {} to {}.", getPath(), tgtId);
    } else {
      val msg = String.format("rename file to %s failed.", tgtId);
      throw new IOException(msg);
    }
  }

  protected FileContext getFileContext() {
    return fileContext;
  }

  private String overlapConcat(String path1, String path2) {
    val path1Scheme = Optional.ofNullable(URI.create(path1).getScheme()).orElse("");
    String concatPath;
    if (path1Scheme.isEmpty() || "file".equals(path1Scheme)) {
      concatPath = path2;
    } else {
      val path1NoAuthority = path1.replaceFirst(path1Scheme + "://", path1Scheme + ":/");
      val replaceRegex = String.format("%s|%s", path1, path1NoAuthority);
      path2 = URI.create(path2.replaceFirst(replaceRegex, "")).getPath();

      if (path1.endsWith("/")) {
        path1 = path1.substring(0, path1.length() - 1);
      }

      concatPath = path1.concat(path2);
    }

    return concatPath;
  }
}
