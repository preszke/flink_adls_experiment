package adls_problem.adlsproblem;

import java.io.Serializable;

public class DummyBean implements Serializable {
    private Long val;
    private Integer mod;

    public Long getVal() {
        return val;
    }

    public void setVal(Long val) {
        this.val = val;
    }

    public Integer getMod() {
        return mod;
    }

    public void setMod(Integer mod) {
        this.mod = mod;
    }


    public DummyBean() {
    }

    public DummyBean(Long val, Integer mod) {
        this.val = val;
        this.mod = mod;
    }

    @Override
    public String toString() {
        return "Dummy{" +
                "val=" + val +
                ", mod=" + mod +
                '}';
    }
}
