import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author bocai
 */
@Getter
@Setter
@ToString
@AllArgsConstructor
public class Person {
    private String name;
    private int age;
    private String address;

    @Override
    public String toString() {
        return "Person{" +
                "name=" + name +
                ", age=" + age +
                ", address=" + address +
                '}';
    }
}
