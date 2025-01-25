package cn.wyber.sparksql.beans;

import java.util.Objects;

public class JavaDoitStudent {
    private int id;
    private String name;
    private String gender;

    public JavaDoitStudent() {
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    @Override
    public String toString() {
        return "cn.wyber.sparksql.beans.JavaDoitStudent{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", gender='" + gender + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JavaDoitStudent that = (JavaDoitStudent) o;
        return id == that.id && Objects.equals(name, that.name) && Objects.equals(gender, that.gender);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, gender);
    }
}
