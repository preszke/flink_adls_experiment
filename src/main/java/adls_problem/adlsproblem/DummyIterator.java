package adls_problem.adlsproblem;

import java.io.Serializable;
import java.util.Iterator;

public class DummyIterator implements Iterator<DummyBean>, Serializable {

    private Long state = 0L;

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public DummyBean next() {
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        state = state + 1;
        return new DummyBean(state, (int) (state % 10));
    }
}
