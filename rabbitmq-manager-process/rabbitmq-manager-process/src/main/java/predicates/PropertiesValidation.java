package predicates;

import java.util.function.Predicate;

public interface PropertiesValidation {
    Predicate<String> fillValue = value -> value != null && value.length() > 0;
}
