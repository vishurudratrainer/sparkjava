package dataframe;

public class Author {
    @Override
    public String toString() {
        return "Author{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", external=" + external +
                '}';
    }

    private Integer id;
    private String name;


    public Extra getExternal() {
        return external;
    }

    public void setExternal(Extra external) {
        this.external = external;
    }

    private Extra external;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
