package file.engine.controller;

import com.google.gson.Gson;
import file.engine.annotation.EventListener;
import file.engine.configs.AllConfigs;
import file.engine.configs.ConfigEntity;
import file.engine.configs.Constants;
import file.engine.dllInterface.PathMatcher;
import file.engine.dllInterface.gpu.GPUAccelerator;
import file.engine.entity.SearchInfoEntity;
import file.engine.event.handler.Event;
import file.engine.event.handler.EventManagement;
import file.engine.event.handler.impl.BootSystemEvent;
import file.engine.event.handler.impl.configs.SetConfigsEvent;
import file.engine.event.handler.impl.database.*;
import file.engine.event.handler.impl.stop.CloseEvent;
import file.engine.services.DatabaseService;
import file.engine.utils.RegexUtil;
import file.engine.utils.ThreadPoolUtil;
import file.engine.utils.gson.GsonUtil;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
public class Core {

    private static volatile DatabaseService.SearchTask currentSearchTask;
    private static DatagramSocket server;

    @EventListener(listenClass = BootSystemEvent.class)
    private static void startServer(Event event) {
        ThreadPoolUtil.getInstance().executeTask(() -> {
            var allConfigs = AllConfigs.getInstance();
            var urlMap = getUrlMap();
            try {
                server = new DatagramSocket(allConfigs.getConfigEntity().getPort(), InetAddress.getLocalHost());
            } catch (SocketException | UnknownHostException e) {
                throw new RuntimeException(e);
            }
            String reqUrl;
            while (!server.isClosed()) {
                try {
                    byte[] bytes = new byte[8192];
                    DatagramPacket receivePacket = new DatagramPacket(bytes, bytes.length);
                    server.receive(receivePacket);
                    reqUrl = new String(receivePacket.getData(), receivePacket.getOffset(),
                            receivePacket.getLength(), StandardCharsets.UTF_8);
                    int blankPos = reqUrl.indexOf(' ');
                    int paramPos = reqUrl.indexOf('?');
                    //无请求方式
                    if (blankPos != -1) {
                        String type = reqUrl.substring(0, blankPos);
                        String url;
                        Map<String, String> params;
                        if (paramPos != -1) {
                            url = reqUrl.substring(blankPos + 1, paramPos);
                            params = getParameter(reqUrl.substring(paramPos + 1));
                        } else {
                            url = reqUrl.substring(blankPos + 1);
                            params = Collections.emptyMap();
                        }
                        var map = urlMap.get(type);
                        //不支持的请求
                        if (map != null) {
                            var function = map.get(url);
                            //未找到对应路径
                            if (function != null) {
                                String result = function.apply(params);
                                byte[] resultBytes = result.getBytes(StandardCharsets.UTF_8);
                                sendResult(receivePacket, resultBytes);
                            } else {
                                sendResult(receivePacket, "not found".getBytes(StandardCharsets.UTF_8));
                            }
                        } else {
                            sendResult(receivePacket, "method not supported".getBytes(StandardCharsets.UTF_8));
                        }
                    } else {
                        sendResult(receivePacket, "method not found".getBytes(StandardCharsets.UTF_8));
                    }
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        });
    }

    @SneakyThrows
    @EventListener(listenClass = CloseEvent.class)
    private static void close(Event event) {
        if (server != null) {
            server.close();
        }
    }

    private static void sendResult(DatagramPacket receivePacket, byte[] bytes) throws IOException {
        InetAddress address = receivePacket.getAddress();
        int port = receivePacket.getPort();
        if (bytes.length == 0) {
            bytes = "success".getBytes(StandardCharsets.UTF_8);
        }
        byte[] lenBytes = int2byteArray(bytes.length);
        server.send(new DatagramPacket(lenBytes, lenBytes.length, address, port));
        server.send(new DatagramPacket(bytes, bytes.length, address, port));
    }

    private static byte[] int2byteArray(int val) {
        byte[] lenBytes = new byte[4];
        lenBytes[3] = (byte) (val & 0xFF);
        lenBytes[2] = (byte) (val >> 8 & 0xFF);
        lenBytes[1] = (byte) (val >> 16 & 0xFF);
        lenBytes[0] = (byte) (val >> 24 & 0xFF);
        return lenBytes;
    }

    private static ConcurrentHashMap<String, ConcurrentHashMap<String, Function<Map<String, String>, String>>> getUrlMap() {
        var urlMap = new ConcurrentHashMap<String, ConcurrentHashMap<String, Function<Map<String, String>, String>>>();
        var getMap = generateGetMap();
        var postMap = generatePostMap();
        var putMap = generatePutMap();
        var deleteMap = generateDeleteMap();
        urlMap.put("GET", getMap);
        urlMap.put("POST", postMap);
        urlMap.put("PUT", putMap);
        urlMap.put("DELETE", deleteMap);
        return urlMap;
    }

    private static ConcurrentHashMap<String, Function<Map<String, String>, String>> generateDeleteMap() {
        EventManagement eventManager = EventManagement.getInstance();
        var deleteMap = new ConcurrentHashMap<String, Function<Map<String, String>, String>>();
        deleteMap.put("/search", param -> {
            eventManager.putEvent(new StopSearchEvent());
            return "";
        });
        deleteMap.put("/cache", param -> {
            eventManager.putEvent(new DeleteFromCacheEvent(param.get("path")));
            return "";
        });
        deleteMap.put("/suffixPriority", param -> {
            eventManager.putEvent(new DeleteFromSuffixPriorityMapEvent(param.get("suffix")));
            return "";
        });
        deleteMap.put("/clearSuffixPriority", param -> {
            eventManager.putEvent(new ClearSuffixPriorityMapEvent());
            return "";
        });
        deleteMap.put("/closeConnections", param -> {
            PathMatcher.INSTANCE.closeConnections();
            return "";
        });
        return deleteMap;
    }

    private static ConcurrentHashMap<String, Function<Map<String, String>, String>> generatePutMap() {
        EventManagement eventManager = EventManagement.getInstance();
        var putMap = new ConcurrentHashMap<String, Function<Map<String, String>, String>>();
        putMap.put("/suffixPriority", param -> {
            eventManager.putEvent(new UpdateSuffixPriorityEvent(
                    param.get("oldSuffix"), param.get("newSuffix"), Integer.parseInt(Objects.requireNonNull(param.get("priority")))
            ));
            return "";
        });
        return putMap;
    }

    private static ConcurrentHashMap<String, Function<Map<String, String>, String>> generatePostMap() {
        Gson gson = GsonUtil.INSTANCE.getGson();
        EventManagement eventManager = EventManagement.getInstance();
        var postMap = new ConcurrentHashMap<String, Function<Map<String, String>, String>>();
        postMap.put("/config", param -> {
            eventManager.putEvent(new SetConfigsEvent(gson.fromJson(URLDecoder.decode(param.get("config"), StandardCharsets.UTF_8), ConfigEntity.class)));
            return "";
        });
        postMap.put("/close", param -> {
            eventManager.putEvent(new CloseEvent());
            return "";
        });
        postMap.put("/flushFileChanges", param -> {
            eventManager.putEvent(new FlushFileChangesEvent());
            return "";
        });
        postMap.put("/optimize", param -> {
            eventManager.putEvent(new OptimizeDatabaseEvent());
            return "";
        });
        postMap.put("/search", param -> {
            final DatabaseService.SearchTask[] searchTask = new DatabaseService.SearchTask[1];
            StartSearchEvent startSearchEvent = new StartSearchEvent(
                    generateSearchKeywordsAndSearchCase(Objects.requireNonNull(param.get("searchText")),
                            Integer.parseInt(Objects.requireNonNull(param.get("maxResultNum"))))
            );
            eventManager.putEvent(startSearchEvent);
            eventManager.waitForEvent(startSearchEvent);
            startSearchEvent.getReturnValue().ifPresent(o -> searchTask[0] = (DatabaseService.SearchTask) o);
            final long startTime = System.currentTimeMillis();
            while (!searchTask[0].isSearchDone() && System.currentTimeMillis() - startTime < Constants.MAX_TASK_EXIST_TIME) {
                try {
                    TimeUnit.MILLISECONDS.sleep(50);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            LinkedHashSet<String> ret = new LinkedHashSet<>();
            ret.addAll(searchTask[0].getCacheAndPriorityResults());
            ret.addAll(searchTask[0].getTempResults());
            return gson.toJson(ret);
        });
        postMap.put("/prepareSearch", param -> {
            PrepareSearchEvent prepareSearchEvent = new PrepareSearchEvent(
                    generateSearchKeywordsAndSearchCase(Objects.requireNonNull(param.get("searchText")),
                            Integer.parseInt(Objects.requireNonNull(param.get("maxResultNum"))))
            );
            eventManager.putEvent(prepareSearchEvent);
            eventManager.waitForEvent(prepareSearchEvent);
            prepareSearchEvent.getReturnValue().ifPresent(o -> currentSearchTask = (DatabaseService.SearchTask) o);
            return currentSearchTask.getUuid().toString();
        });
        postMap.put("/searchAsync", param -> {
            StartSearchEvent startSearchEvent = new StartSearchEvent(
                    generateSearchKeywordsAndSearchCase(Objects.requireNonNull(param.get("searchText")),
                            Integer.parseInt(Objects.requireNonNull(param.get("maxResultNum"))))
            );
            eventManager.putEvent(startSearchEvent);
            eventManager.waitForEvent(startSearchEvent);
            startSearchEvent.getReturnValue().ifPresent(o -> currentSearchTask = (DatabaseService.SearchTask) o);
            return currentSearchTask.getUuid().toString();
        });
        postMap.put("/cache", param -> {
            eventManager.putEvent(new AddToCacheEvent(param.get("path")));
            return "";
        });
        postMap.put("/update", param -> {
            eventManager.putEvent(new UpdateDatabaseEvent(Boolean.parseBoolean(param.get("isDropPrevious"))));
            return "";
        });
        postMap.put("/suffixPriority", param -> {
            eventManager.putEvent(new AddToSuffixPriorityMapEvent(
                    param.get("suffix"), Integer.parseInt(Objects.requireNonNull(param.get("priority")))
            ));
            return "";
        });
        return postMap;
    }

    private static ConcurrentHashMap<String, Function<Map<String, String>, String>> generateGetMap() {
        Gson gson = GsonUtil.INSTANCE.getGson();
        var getMap = new ConcurrentHashMap<String, Function<Map<String, String>, String>>();
        getMap.put("/config", param -> gson.toJson(AllConfigs.getInstance().getConfigEntity()));
        getMap.put("/gpu", param -> gson.toJson(GPUAccelerator.INSTANCE.getDevices()));
        getMap.put("/status", param -> DatabaseService.getInstance().getStatus().toString());
        getMap.put("/frequentResult", param -> gson.toJson(DatabaseService.getInstance().getFrequentlyUsedCaches(Integer.parseInt(Objects.requireNonNull(param.get("num"))))));
        getMap.put("/cacheResult", param -> gson.toJson(getSearchCacheResults(Integer.parseInt(Objects.requireNonNull(param.get("startIndex"))))));
        getMap.put("/result", param -> gson.toJson(getSearchResults(Integer.parseInt(Objects.requireNonNull(param.get("startIndex"))))));
        getMap.put("/cache", param -> gson.toJson(DatabaseService.getInstance().getCache()));
        getMap.put("/suffixPriority", param -> gson.toJson(DatabaseService.getInstance().getPriorityMap()));
        return getMap;
    }

    private static HashMap<String, String> getParameter(String contents) {
        HashMap<String, String> map = new HashMap<>();
        String[] keyValues = contents.split("&");
        for (String keyValue : keyValues) {
            int i = keyValue.indexOf("=");
            String key = keyValue.substring(0, i);
            String value = keyValue.substring(i + 1);
            map.put(key, value);
        }
        return map;
    }

    private static HashMap<String, Object> getSearchResults(int startIndex) {
        HashMap<String, Object> retWrapper = new HashMap<>();
        if (currentSearchTask != null) {
            var tempResults = currentSearchTask.getTempResults();
            genSearchResultMap(startIndex, retWrapper, tempResults);
        }
        return retWrapper;
    }

    private static HashMap<String, Object> getSearchCacheResults(int startIndex) {
        HashMap<String, Object> ret = new HashMap<>();
        if (currentSearchTask != null) {
            var cacheAndPriorityResults = currentSearchTask.getCacheAndPriorityResults();
            genSearchResultMap(startIndex, ret, cacheAndPriorityResults);
        }
        return ret;
    }

    private static void genSearchResultMap(int startIndex, HashMap<String, Object> retWrapper, ConcurrentLinkedQueue<String> tempResults) {
        retWrapper.put("uuid", currentSearchTask.getUuid().toString());
        Iterator<String> iterator = tempResults.iterator();
        for (int i = 0; i < startIndex; i++) {
            if (iterator.hasNext()) {
                iterator.next();
            } else {
                startIndex = i;
                break;
            }
        }
        ArrayList<String> list = new ArrayList<>();
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        retWrapper.put("data", list);
        retWrapper.put("nextIndex", list.size() + startIndex);
        retWrapper.put("isDone", currentSearchTask.isSearchDone());
    }

    /**
     * 根据用户输入设置搜索关键字
     */
    private static SearchInfoEntity generateSearchKeywordsAndSearchCase(String searchBarText, int maxResultNum) {
        String searchText;
        String[] searchCase;
        String[] keywords;
        if (!searchBarText.isEmpty()) {
            final int i = searchBarText.lastIndexOf('|');
            if (i == -1) {
                searchCase = null;
                searchText = searchBarText;
            } else {
                searchText = searchBarText.substring(0, i);
                var searchCaseStr = searchBarText.substring(i + 1);
                if (!searchCaseStr.isEmpty()) {
                    String[] tmpSearchCase = RegexUtil.semicolon.split(searchCaseStr);
                    searchCase = new String[tmpSearchCase.length];
                    for (int j = 0; j < tmpSearchCase.length; j++) {
                        searchCase[j] = tmpSearchCase[j].trim();
                    }
                } else {
                    searchCase = null;
                }
            }
            keywords = RegexUtil.semicolon.split(searchText);
        } else {
            keywords = null;
            searchCase = null;
            searchText = "";
        }
        return new SearchInfoEntity(() -> searchText, () -> searchCase, () -> keywords, maxResultNum);
    }
}
