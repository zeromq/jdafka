package org.zeromq.dafka;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.nio.ByteBuffer;

public class DafkaHeadKey
{
    private static int MAX_HEAD_KEY_SIZE = 1 + 256 + 256;

    private ByteBuffer buffer = ByteBuffer.allocate(MAX_HEAD_KEY_SIZE);

    private String subject;
    private String address;

    public void set(String subject, String address)
    {
        this.subject = subject;
        this.address = address;

        buffer.clear();
        buffer.putChar('H');
        buffer.putInt(subject.length()).put(subject.getBytes());
        buffer.putInt(address.length()).put(address.getBytes());
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

        if (byteBuf.getChar() != 'H') {
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

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DafkaHeadKey that = (DafkaHeadKey) o;

        return new EqualsBuilder().append(subject, that.subject).append(address, that.address).isEquals();
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder(17, 37).append(subject).append(address).toHashCode();
    }
}
