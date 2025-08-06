package converters;

import java.io.*;

public record TrataConteudoMensagem() {

    public byte[] converterParaBytes(Object data) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); ObjectOutput out = new ObjectOutputStream(bos)) {
            out.writeObject(data);
            return bos.toByteArray();
        }
    }

    public Object converterParaObjetos(byte[] messageBytes) throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(messageBytes); ObjectInput in = new ObjectInputStream(bis)) {
            return in.readObject();
        }
    }
}
