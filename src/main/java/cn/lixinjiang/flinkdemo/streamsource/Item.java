package cn.lixinjiang.flinkdemo.streamsource;

/**
 * @Author lxj
 */
public class Item {
    private String name;
    private Integer id;

    public Item() {
    }

    public Item(String name, Integer id) {
        this.name = name;
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "Item{" + "name='" + name + "'" + ",id=" + id + "}";
    }
}
