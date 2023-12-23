package file.engine.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.function.Supplier;

@Data
@AllArgsConstructor
public class SearchInfoEntity {

    private Supplier<String> searchText;

    private Supplier<String[]> searchCase;

    private Supplier<String[]> keywords;

    private int maxResultNum;
}
