package com.alicp.jetcache.support;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import java.io.ByteArrayOutputStream;
import java.lang.ref.WeakReference;

/**
 * Created on 2016/10/4.
 *
 * @author <a href="mailto:areyouok@gmail.com">huangli</a>
 */
public class Lz4ValueEncoder extends AbstractValueEncoder {

    public static final Lz4ValueEncoder INSTANCE = new Lz4ValueEncoder();

    private static int INIT_BUFFER_SIZE = 512;

    static ThreadLocal<Object[]> kryoThreadLocal = ThreadLocal.withInitial(() -> {
        Kryo kryo = new Kryo();
        kryo.setDefaultSerializer(CompatibleFieldSerializer.class);

        byte[] buffer = new byte[INIT_BUFFER_SIZE];

        WeakReference<byte[]> ref = new WeakReference<>(buffer);
        return new Object[]{kryo, ref};
    });

    public Lz4ValueEncoder() {
        super(false);
    }

    @Override
    public byte[] apply(Object value) {
        try {
            Object[] kryoAndBuffer = kryoThreadLocal.get();
            Kryo kryo = (Kryo) kryoAndBuffer[0];
            WeakReference<byte[]> ref = (WeakReference<byte[]>) kryoAndBuffer[1];
            byte[] buffer = ref.get();
            if (buffer == null) {
                buffer = new byte[INIT_BUFFER_SIZE];
            }
            Output output = new Output(buffer, -1);
            try {
                kryo.writeClassAndObject(output, value);
                byte[] bytes = output.toBytes();
                return this.compress(bytes);
            } finally {
                //reuse buffer if possible
                if (ref.get() == null || buffer != output.getBuffer()) {
                    ref = new WeakReference<>(output.getBuffer());
                    kryoAndBuffer[1] = ref;
                }
            }
        } catch (Exception e) {
            StringBuilder sb = new StringBuilder("Kryo Encode error. ");
            sb.append("msg=").append(e.getMessage());
            throw new CacheEncodeException(sb.toString(), e);
        }
    }

    protected byte[] compress(byte[] data) {
        LZ4Factory factory = LZ4Factory.fastestInstance();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        LZ4Compressor compressor = factory.fastCompressor();
        try (LZ4BlockOutputStream out = new LZ4BlockOutputStream(bos, 2048, compressor)) {
            out.write(data);
        } catch (Exception e) {
            StringBuilder sb = new StringBuilder("LZ4 compress error. ");
            sb.append("msg=").append(e.getMessage());
            throw new CacheEncodeException(sb.toString(), e);
        }
        return bos.toByteArray();
    }

}
