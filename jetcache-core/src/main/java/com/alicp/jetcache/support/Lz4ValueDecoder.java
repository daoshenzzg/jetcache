package com.alicp.jetcache.support;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/**
 * Created on 2016/10/4.
 *
 * @author <a href="mailto:areyouok@gmail.com">huangli</a>
 */
public class Lz4ValueDecoder extends AbstractValueDecoder {

    public static final Lz4ValueDecoder INSTANCE = new Lz4ValueDecoder();

    public Lz4ValueDecoder() {
        super(false);
    }

    @Override
    public Object doApply(byte[] buffer) {
        buffer = this.uncompress(buffer);

        ByteArrayInputStream in = new ByteArrayInputStream(buffer);
        Input input = new Input(in);
        Kryo kryo = (Kryo) KryoValueEncoder.kryoThreadLocal.get()[0];
        kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
        return kryo.readClassAndObject(input);
    }

    protected byte[] uncompress(byte[] data) {
        LZ4Factory factory = LZ4Factory.fastestInstance();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        LZ4FastDecompressor decompressor = factory.fastDecompressor();
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        try (LZ4BlockInputStream in = new LZ4BlockInputStream(bis, decompressor)) {
            int count;
            final byte[] buffer = new byte[1024];
            while ((count = in.read(buffer)) != -1) {
                bos.write(buffer, 0, count);
            }
        } catch (Exception e) {
            StringBuilder sb = new StringBuilder("LZ4 uncompress error. ");
            sb.append("msg=").append(e.getMessage());
            throw new CacheEncodeException(sb.toString(), e);
        }
        return bos.toByteArray();
    }
}
