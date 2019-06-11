package dubstep;

import net.sf.jsqlparser.expression.PrimitiveValue;
import java.io.IOException;

public interface DataReader
{
    PrimitiveValue readData() throws IOException;
    void reset() throws IOException;
    void close() throws IOException, ClassNotFoundException ;
}
