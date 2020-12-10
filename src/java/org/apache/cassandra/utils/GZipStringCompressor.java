/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GZipStringCompressor
{

    public static String compress(String str) throws IOException
    {
        if (str == null || str.length() == 0) {
            return str;
        }

        try (ByteArrayOutputStream out = new ByteArrayOutputStream())
        {
            try (GZIPOutputStream gzip = new GZIPOutputStream(out))
            {
                gzip.write(str.getBytes());
            }

            return out.toString("ISO-8859-1");
        }
    }

    public static String uncompressIfGZipped(String str) throws IOException
    {
        if (str == null || str.length() == 0) {
            return str;
        }

        if (isGZipped(str))
        {
            try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                 ByteArrayInputStream in = new ByteArrayInputStream(str.getBytes("ISO-8859-1")))
            {

                try (GZIPInputStream gzip = new GZIPInputStream(in))
                {
                    copy(gzip, out);
                }

                return out.toString(Charset.defaultCharset().name());
            }
        } else {
            return str;
        }
    }

    public static boolean isGZipped(String str) throws IOException
    {
        if (str == null || str.getBytes("ISO-8859-1").length < 10)
        {
            return false;
        }

        byte[] bytes = str.getBytes("ISO-8859-1");
        final byte first = bytes[0];
        final byte second =bytes[1];
        // GZip uses a 10 bytes header with the first 2 bytes equals to 1f8b
        return ((first & 0xFF) == 0x1F) && ((second & 0xFF) == 0x8B);
    }

    private static long copy(InputStream input, OutputStream output) throws IOException
    {
        long count;
        int n;
        byte[] buffer = new byte[4096];
        for(count = 0L; -1 != (n = input.read(buffer)); count += (long)n)
        {
            output.write(buffer, 0, n);
        }
        output.flush();
        return count;
    }
}
