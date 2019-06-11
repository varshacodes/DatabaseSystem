package dubstep;

import net.sf.jsqlparser.expression.PrimitiveValue;
import java.io.IOException;

public interface DataWriter
{
    void writeData(String data)throws IOException;
    void writeData(PrimitiveValue data)throws IOException,PrimitiveValue.InvalidPrimitive;
    void close() throws IOException;
}
