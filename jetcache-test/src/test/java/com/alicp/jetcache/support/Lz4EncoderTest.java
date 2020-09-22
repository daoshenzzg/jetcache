package com.alicp.jetcache.support;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Created on 2016/10/8.
 *
 * @author <a href="mailto:areyouok@gmail.com">huangli</a>
 */
public class Lz4EncoderTest extends AbstractEncoderTest {
    @Test
    public void test() {
        encoder = Lz4ValueEncoder.INSTANCE;
        decoder = Lz4ValueDecoder.INSTANCE;
        baseTest();
    }

    @Test
    public void compoundTest() {
        encoder = (p) -> Lz4ValueEncoder.INSTANCE.apply(Lz4ValueEncoder.INSTANCE.apply(p));
        decoder = (p) -> Lz4ValueDecoder.INSTANCE.apply((byte[]) Lz4ValueDecoder.INSTANCE.apply(p));
        baseTest();

        encoder = (p) -> Lz4ValueEncoder.INSTANCE.apply(JavaValueEncoder.INSTANCE.apply(p));
        decoder = (p) -> JavaValueDecoder.INSTANCE.apply((byte[]) Lz4ValueDecoder.INSTANCE.apply(p));
        baseTest();
    }

    @Test
    public void compatibleTest() {
        encoder = Lz4ValueEncoder.INSTANCE;
        decoder = Lz4ValueDecoder.INSTANCE;
        baseTest();
    }

    @Test
    public void errorTest() {
        encoder = Lz4ValueEncoder.INSTANCE;
        decoder = Lz4ValueDecoder.INSTANCE;
        byte[] bytes = encoder.apply("12345");
        bytes[0] = 0;
        assertThrows(CacheEncodeException.class, () -> decoder.apply(bytes));
        ((AbstractValueEncoder)encoder).writeHeader(bytes, JavaValueEncoder.IDENTITY_NUMBER);
        assertThrows(CacheEncodeException.class, () -> decoder.apply(bytes));

        encoder = Lz4ValueEncoder.INSTANCE;
        decoder = new Lz4ValueDecoder();
        assertThrows(CacheEncodeException.class, () -> decoder.apply(bytes));

        encoder = new Lz4ValueEncoder();
        decoder = Lz4ValueDecoder.INSTANCE;
        assertThrows(CacheEncodeException.class, () -> decoder.apply(bytes));
    }

    @Test
    public void gcTest() {
        encoder = Lz4ValueEncoder.INSTANCE;
        decoder = Lz4ValueDecoder.INSTANCE;
        super.gcTest();
    }

}
