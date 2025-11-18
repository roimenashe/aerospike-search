package io.github.roimenashe.util;

import io.github.roimenashe.model.SimilarityFunction;
import org.apache.lucene.index.VectorSimilarityFunction;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class VectorUtil {

    public static String getUniqueVectorIndexName(String namespace, String set, SimilarityFunction similarityFunction) {
        return namespace + ":" + set + ":" + similarityFunction.name();
    }

    public static byte[] floatsToBytes(float[] floats) {
        ByteBuffer buffer = ByteBuffer.allocate(floats.length * 4);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        for (float f : floats) {
            buffer.putFloat(f);
        }
        return buffer.array();
    }

    public static float[] bytesToFloats(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        float[] floats = new float[bytes.length / 4];
        for (int i = 0; i < floats.length; i++) {
            floats[i] = buffer.getFloat();
        }
        return floats;
    }

    public static VectorSimilarityFunction getVectorSimilarityFunction(SimilarityFunction similarityFunction) {
        return switch (similarityFunction) {
            case DOT_PRODUCT -> VectorSimilarityFunction.DOT_PRODUCT;
            case COSINE -> VectorSimilarityFunction.COSINE;
            case EUCLIDEAN -> VectorSimilarityFunction.EUCLIDEAN;
            default -> throw new IllegalArgumentException("Unsupported similarity function: " + similarityFunction);
        };
    }
}
