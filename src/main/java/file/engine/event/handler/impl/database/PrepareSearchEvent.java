package file.engine.event.handler.impl.database;

import file.engine.entity.SearchInfoEntity;
import lombok.NonNull;

import java.util.function.Supplier;

public class PrepareSearchEvent extends StartSearchEvent {
    public PrepareSearchEvent(@NonNull Supplier<String> searchText, @NonNull Supplier<String[]> searchCase, @NonNull Supplier<String[]> keywords) {
        super(searchText, searchCase, keywords);
    }

    public PrepareSearchEvent(SearchInfoEntity searchInfoEntity) {
        super(searchInfoEntity.getSearchText(), searchInfoEntity.getSearchCase(), searchInfoEntity.getKeywords());
    }
}
