/*
 * Copyright 2014 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.rackspacecloud.blueflood.outputs.cloudfiles;

import com.rackspacecloud.blueflood.eventemitter.RollupEvent;
import com.rackspacecloud.blueflood.outputs.serializers.RollupEventSerializer;
import com.rackspacecloud.blueflood.service.CloudfilesConfig;
import com.rackspacecloud.blueflood.service.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;

// Not thread-safe
public class RollupFile implements Comparable {
    private static final Logger log = LoggerFactory.getLogger(RollupFile.class);

    private final File file;
    private FileOutputStream outputStream;
    private long timestamp;
    private RollupEventSerializer serializer = new RollupEventSerializer();
    public static final FileFilter fileFilter = new RollupFileFilter();

    public RollupFile(File file) {
        this.file = file;
        this.timestamp = parseTimestamp(file.getName());
    }

    /**
     * Retrieve the timestamp associated with this file.
     *
     * @return The timestamp associated with the file.
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Get the name of the buffer file.
     *
     * @return The name of the buffer file.
     */
    public String getName() {
        return file.getName();
    }

    /**
     * Get the remote file name.
     *
     * @return The path to the remote file.
     */
    public String getRemoteName() {
        Date time = new Date(timestamp);
        String formattedTime = new SimpleDateFormat("yyyyMMdd_").format(time);
        return formattedTime + System.currentTimeMillis() + "_" + Configuration.getInstance().getStringProperty(CloudfilesConfig.CLOUDFILES_HOST_UNIQUE_IDENTIFIER);
    }

    /**
     * Get the age of the rollup file in milliseconds.
     *
     * @return The age of the rollup file in milliseconds.
     */
    public long getAge() {
        return System.currentTimeMillis() - timestamp;
    }

    /**
     * Get the size of the rollup file in bytes.
     *
     * @return The size of the rollup file in bytes.
     * @throws IOException
     */
    public long getSize() throws IOException {
        ensureOpen();
        return outputStream.getChannel().size();
    }

    /**
     * Retrieve an InputStream which reads from this rollup file.
     *
     * @return An InputStream of data from the file.
     * @throws FileNotFoundException
     */
    public InputStream asReadStream() throws FileNotFoundException {
        return new FileInputStream(file);
    }

    /**
     * Serialize a Rollup Event and append it to the file.
     *
     * @param rollup The rollup to append.
     * @throws IOException
     */
    public void append(RollupEvent rollup) throws IOException {
        ensureOpen();
        outputStream.write(serializer.toBytes(rollup));
        outputStream.write('\n');
        outputStream.flush();
    }

    public void close() throws IOException {
        if (outputStream != null) {
            outputStream.close();
            outputStream = null;
        }
    }

    public void delete() throws FileNotFoundException, IllegalAccessException {
        if (!file.delete()) {
            if (!file.exists()) {
                throw new FileNotFoundException("File does not exist");
            }
            throw new IllegalAccessException("File exists but could not be deleted. File is probably locked.");
        }
    }

    public int compareTo(Object other) {
        return new Long(getTimestamp()).compareTo(((RollupFile) other).getTimestamp());
    }


    private void ensureOpen() throws FileNotFoundException {
        if (outputStream == null) {
            log.info("opening buffer file for writing: {}", file.getName());
            outputStream = new FileOutputStream(file, true);
        }
    }

    /**
     * Parse the timestamp from a filename.
     *
     * @param fileName The file name to parse.
     * @return The timestamp contained in the file name.
     * @throws NumberFormatException
     */
    private static long parseTimestamp(String fileName) throws NumberFormatException {
        String numberPart = fileName.substring(0, fileName.length() - 5);
        return Long.parseLong(numberPart);
    }

    /**
     * Build a new RollupFile using the current time as the timestamp. The file won't be created until it is actually
     * written to for the first time.
     *
     * @param bufferDir The directory in which to create the file.
     * @return The new RollupFile.
     */
    public static RollupFile buildRollupFile(File bufferDir) {
        return new RollupFile(new File(bufferDir, System.currentTimeMillis() + ".json"));
    }

    private static class RollupFileFilter implements FileFilter {
        public boolean accept(File f) {
            String fileName = f.getName();

            if (!f.isFile()) {
                return false;
            }

            if (!fileName.endsWith(".json")) {
                log.info("skipping non-JSON file: {}", fileName);
                return false;
            }

            try {
                parseTimestamp(fileName);
            } catch (NumberFormatException e) {
                log.info("skipping malformatted filename: {}", fileName);
                return false;
            }
            return true;
        }
    }
}
