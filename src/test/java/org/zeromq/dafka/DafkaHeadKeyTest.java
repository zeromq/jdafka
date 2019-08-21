package org.zeromq.dafka;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

@RunWith(JUnit4.class)
public class DafkaHeadKeyTest
{

    @Test
    public void encodeDecode()
    {
        DafkaHeadKey headKey = new DafkaHeadKey();
        headKey.set("abc", "def");
        assertThat(headKey.getSubject(), equalTo("abc"));
        assertThat(headKey.getAddress(), equalTo("def"));

        final byte[] encode = headKey.encode();

        headKey = new DafkaHeadKey();
        headKey.decode(encode);

        assertThat(headKey.getSubject(), equalTo("abc"));
        assertThat(headKey.getAddress(), equalTo("def"));
    }
}
