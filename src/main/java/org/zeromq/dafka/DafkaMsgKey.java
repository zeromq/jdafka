package org.zeromq.dafka;

import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.nio.ByteBuffer;

public class DafkaMsgKey implements Comparable<DafkaMsgKey>
{
    private static int MAX_HEAD_KEY_SIZE = 1 + 256 + 256;

    private ByteBuffer buffer = ByteBuffer.allocate(MAX_HEAD_KEY_SIZE);

    private String subject;
    private String address;
    private long   sequence;

    public void setKey(String subject, String address, long sequence)
    {
        this.subject = subject;
        this.address = address;
        this.sequence = sequence;

        buffer.clear();
        buffer.putChar('M');
        buffer.putInt(subject.length()).put(subject.getBytes());
        buffer.putInt(address.length()).put(address.getBytes());
        buffer.putLong(sequence);
        buffer.flip();
    }

    // Encode head key as bytes for leveldb
    public byte[] encode()
    {
        byte[] headBytes = new byte[buffer.remaining()];
        buffer.get(headBytes);
        buffer.rewind();
        return headBytes;
    }

    public boolean decode(byte[] buffer)
    {
        int size = buffer.length;
        ByteBuffer byteBuf = ByteBuffer.wrap(buffer);

        if (size < 1 + 2 + 8) {
            return false;
        }

        if (byteBuf.getChar() != 'M') {
            return false;
        }

        int subjectSize = byteBuf.getInt();
        byte[] subjectBytes = new byte[subjectSize];
        byteBuf.get(subjectBytes);
        this.subject = new String(subjectBytes);

        int addressSize = byteBuf.getInt();
        byte[] addressBytes = new byte[addressSize];
        byteBuf.get(addressBytes);
        this.address = new String(addressBytes);

        this.sequence = byteBuf.getLong();

        return true;
    }

    public String getSubject()
    {
        return subject;
    }

    public String getAddress()
    {
        return address;
    }

    public long getSequence()
    {
        return sequence;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DafkaMsgKey that = (DafkaMsgKey) o;

        return new EqualsBuilder().append(subject, that.subject).append(address, that.address).isEquals();
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder(17, 37).append(subject).append(address).toHashCode();
    }

    @Override
    public int compareTo(DafkaMsgKey other)
    {
        return new CompareToBuilder()
                .append(this.subject, other.subject)
                .append(this.address, other.address)
                .append(this.sequence, other.sequence)
                .build();
    }
}
