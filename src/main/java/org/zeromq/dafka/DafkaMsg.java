package org.zeromq.dafka;

public class DafkaMsg
{
    private String topic;
    private String partition;
    private byte[] data;

    public DafkaMsg(String subject, String address, byte[] data)
    {
        this.topic = subject;
        this.partition = address;
        this.data = data;
    }

    public String getTopic()
    {
        return topic;
    }

    public String getPartition()
    {
        return partition;
    }

    public byte[] getData()
    {
        return data;
    }
}
