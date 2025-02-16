package org.moriano.locopostgres;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A set of utility methods to deal with arrays of bytes.
 *
 * In normal conditions one would use an existing Library. However LocoPostgres is made in a way so that
 * I do not depend on external libraries, this is a technical decision made just for fun :)
 */
public class ByteUtil {

    public static int getInt16(byte[] bytes) {
        if (bytes.length != 2) {
            throw new RuntimeException("I need exactly 2 bytes to perform an int16 conversion, you gave me " + bytes);
        }
        return ByteBuffer.wrap(new byte[]{0, 0, bytes[0], bytes[1]}).getInt();
    }

    /**
     * Gets an Int32 from a byte array
     * @param bytes
     * @return
     */
    public static int getInt32(byte[] bytes) {
        if (bytes.length != 4) {
            throw new RuntimeException("I need exactly 4 bytes to perform an int32 conversion, you gave me " + bytes);
        }
        return ByteBuffer.wrap(bytes).getInt();
    }


    /**
     * Returns a 0x00 terminated string. This is useful as the postgres protocol terminates strings with the
     * zero byte
     * @param value
     * @return
     */
    public static byte[] getZeroByteTerminatedString(String value) {
        if (value == null) {
            return new byte[]{0x00}; // A zero byte represents an empty string
        }
        return ByteUtil.concat(value.getBytes(StandardCharsets.UTF_8), new byte[]{0x00});
    }


    public static Character asChar(byte raw) {
        return new String(new byte[]{raw}, StandardCharsets.UTF_8).charAt(0);
    }

    /**
     * Extract strings from this array of bytes. Strings are terminated by byte 0x00 as per the postgres protocol
     * @param raw
     * @return
     */
    public static List<String> asStrings(byte[] raw) {
        int startStringIdx = 0;
        List<String> result = new ArrayList<>();
        for(int i = 0; i<raw.length; i++) {
            if (raw[i] == 0x00) {
                result.add(new String(Arrays.copyOfRange(raw, startStringIdx,i), StandardCharsets.UTF_8));
                startStringIdx = i+1;
            }
        }
        return result;
    }

    public static char[] asCharArray(byte[] raw) {
        return new String(raw, StandardCharsets.UTF_8).toCharArray();
    }

    /**
     * Provides a very human readable representation of this array of bytes.
     * @param raws
     * @return
     */
    public static String prettyPrint(byte[] raws) {
        StringBuilder result = new StringBuilder("" +
                "       +--------------------------------------------------+----------------------------------+------------------+\n" +
                "       | 00 01 02 03 04 05 06 07 08 09 0A 0B C0 D0 E0 0F  | 0 1 2 3 4 5 6 7 8 9 A B C D E F  |      Ascii       |\n" +
                "+------+--------------------------------------------------+----------------------------------+------------------+\n");
        StringBuilder preffix = new StringBuilder();
        StringBuilder suffix = new StringBuilder();

        int columnCount = 1;
        int rowCount = 0;
        for (byte raw : raws) {

            String niceByte = String.format("%02X ", raw);
            String niceString = new String(new byte[]{raw}, StandardCharsets.US_ASCII);
            if (!(raw >=0x21 && raw <= 0x7E)) {
                /*
                As per the ascii table.

                Characters that are not printable are assigned a dot, so that the printing of the array
                of bytes makes sense to the human eye
                 */
                niceString = ".";
            }

            preffix.append(niceByte);
            suffix.append(niceString + " ");
            if (columnCount >= 16 && columnCount % 16 == 0) {
                result.append("| " + String.format("%04X", rowCount) + " | ");
                result.append(preffix);
                result.append(" | ");
                result.append(suffix);
                result.append(" | ");
                result.append(suffix.toString().replace(" ", ""));
                result.append(" |\n");

                preffix = new StringBuilder();
                suffix = new StringBuilder();
                rowCount++;
            }
            columnCount++;
        }

        if (preffix.length() > 0) {
            result.append("| " + String.format("%04X", rowCount) + " | ");
            result.append(preffix);
            int expectedSize = 16 * 3;
            int missingGaps = expectedSize - preffix.length();
            for (int i = 0; i < missingGaps; i++) {
                result.append(" ");
            }
            result.append(" | ");
            result.append(suffix);

            expectedSize = (16 * 2);
            missingGaps = expectedSize - suffix.length();
            for (int i = 0; i < missingGaps; i++) {
                result.append(" ");
            }

            result.append(" | ");
            result.append(suffix.toString().replace(" ", ""));
            expectedSize = 16;
            missingGaps = expectedSize - suffix.toString().replace(" ", "").length();
            for (int i = 0; i < missingGaps; i++) {
                result.append(" ");
            }
            result.append(" | \n");
        }
        result.append("+------+--------------------------------------------------+----------------------------------+------------------+\n");

        return result.toString();
    }

    public static byte[] asBytes(int int32) {
        byte[] result = ByteBuffer.allocate(4).putInt(int32).array();
        return result;
    }

    public static byte[] asBytesInt16(int int16) {
        byte[] result = ByteBuffer.allocate(4).putInt(int16).array();
        return new byte[]{result[2], result[3]};
    }

    public static byte[] asBytes(String string) {
        byte[] result = string.getBytes(StandardCharsets.UTF_8);
        return result;
    }

    /**
     * Concatenates a bunch of array bytes into one.
     *
     * In normal conditions you will just use a library for this, but LocoPostgres is done in a way in which
     * we do not use external libraries (this is not practical, of course, it is just the way I decided to do
     * things :)
     * @param someBytes
     * @return
     */
    public static byte[] concat(byte[]... someBytes) {
        ByteArrayOutputStream outputStream;
        byte[] result = null;
        try {
            outputStream = new ByteArrayOutputStream();
            for (byte[] someByte : someBytes) {
                outputStream.write(someByte);
            }
            result = outputStream.toByteArray();

            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
}
