package org.moriano.locopostgres;

import java.util.Arrays;

/**
 * An object representation of the COMMAND_COMPLETE packet.
 *
 * Essentially the packet will return a string that contains a 'tag' which is normally the type of
 * query executed (SELECT, INSERT, etc)
 *
 * Depending on the command, different info will follow, specifically
 *
 * - INSERT will contain
 *      - INSERT tag
 *      - oid (object id)
 *      - rows (the number of rows inserted)
 * - DELETE contains the DELETE tag and then number of rows deleted
 * - UPDATE contains UPDATE tag and number of rows updated
 * - SELECT or CREATE TABLE AS contains the SELECT tag and number of rows retrieved
 * - MOVE contains MOVE tag and number of rows the cursor's position has changed by
 * - FETCH contains FETCH tag and number of rows retrieved by the cursor
 * - COPY contains the COPY tag and number of rows copied
 */
public final class CommandComplete {

    private final String rawData;
    private final int insertedRows;
    private final int deletedRows;
    private final int updatedRows;
    private final int retrievedRows;
    private final int movedRows;
    private final int fetchedRows;
    private final int copiedRows;

    private CommandComplete(Packet packet) {
        /*
        We are only interested om the contents of the packet, not its type nor its size. So we skip the type byte
        and the int32 indicating its size
         */
        this.rawData = new String(Arrays.copyOfRange(packet.getPacketContents(), 5, packet.getPacketContents().length-1));
        this.insertedRows = this.rawData.contains("INSERT") ? Integer.valueOf(this.rawData.substring("INSERT".length()+1)) : 0;
        this.deletedRows = this.rawData.contains("DELETE") ? Integer.valueOf(this.rawData.substring("DELETE".length()+1)) : 0;
        this.updatedRows = this.rawData.contains("UPDATE") ? Integer.valueOf(this.rawData.substring("UPDATE".length()+1)) : 0;
        if (this.rawData.contains("SELECT") || this.rawData.contains("CREATE TABLE AS")) {
            if (this.rawData.contains("SELECT")) {
                this.retrievedRows = Integer.valueOf(this.rawData.substring("SELECT".length()+1));
            } else {
                this.retrievedRows = Integer.valueOf(this.rawData.substring("CREATE TABLE AS".length()+1));
            }
        } else {
            this.retrievedRows = 0;
        }

        this.movedRows = this.rawData.contains("MOVE") ? Integer.valueOf(this.rawData.substring("MOVE".length()+1)) : 0;
        this.fetchedRows = this.rawData.contains("FETCH") ? Integer.valueOf(this.rawData.substring("FETCH".length()+1)) : 0;
        this.copiedRows = this.rawData.contains("COPY") ? Integer.valueOf(this.rawData.substring("COPY".length()+1)) : 0;
    }

    public static CommandComplete fromCommandCompletePacket(Packet packet) {
        if (packet.getPacketType() != PacketType.BACKEND_COMMAND_COMPLETE) {
            throw new RuntimeException("Ouch! you tried to created a command complete from the incorrect type " +
                    "of packet! packet was " + packet);
        }
        return new CommandComplete(packet);
    }

    public int getInsertedRows() {
        return insertedRows;
    }

    public int getDeletedRows() {
        return deletedRows;
    }

    public int getUpdatedRows() {
        return updatedRows;
    }

    public int getRetrievedRows() {
        return retrievedRows;
    }

    public int getMovedRows() {
        return movedRows;
    }

    public int getFetchedRows() {
        return fetchedRows;
    }

    public int getCopiedRows() {
        return copiedRows;
    }
}
