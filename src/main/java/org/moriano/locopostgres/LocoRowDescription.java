package org.moriano.locopostgres;

import java.util.List;

public class LocoRowDescription {

    private Packet rowDescription;
    private int numberOfFields;
    private List<LocoField> fields;

    public LocoRowDescription(Packet rowDescription) {

        this.rowDescription = rowDescription;

        byte[] raws = this.rowDescription.getPacketContents();
        this.numberOfFields = ByteUtil.getInt16(new byte[]{raws[5], raws[6]});
        this.fields = LocoField.fromPacket(rowDescription, numberOfFields);
    }

    public LocoField getFieldByName(String name) {
        return this.fields.stream().filter(t -> t.getName().equals(name)).findFirst().get();
    }

    public LocoField getFieldByPosition(int position) {
        return this.fields.get(position);
    }

    public int getNumberOfFields() {
        return numberOfFields;
    }

    public List<LocoField> getFields() {
        return fields;
    }
}
