package file.engine.event.handler.impl.database;

import file.engine.entity.SearchInfoEntity;
import file.engine.event.handler.Event;
import lombok.NonNull;

import java.util.function.Supplier;

public class StartSearchEvent extends Event {

    public final Supplier<String[]> searchCase;
    public final Supplier<String> searchText;
    public final Supplier<String[]> keywords;
    public final int maxResultNum;

    public StartSearchEvent(@NonNull Supplier<String> searchText, @NonNull Supplier<String[]> searchCase, @NonNull Supplier<String[]> keywords) {
        this.searchCase = searchCase;
        this.searchText = searchText;
        this.keywords = keywords;
        this.maxResultNum = 200;
    }

    public StartSearchEvent(SearchInfoEntity searchInfoEntity) {
        this.searchCase = searchInfoEntity.getSearchCase();
        this.searchText = searchInfoEntity.getSearchText();
        this.keywords = searchInfoEntity.getKeywords();
        this.maxResultNum = searchInfoEntity.getMaxResultNum();
    }
}
