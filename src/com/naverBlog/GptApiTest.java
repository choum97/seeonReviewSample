package com.seeon.naverBlog;

import com.seeon.common.util.ConnectionFactory;
import com.seeon.common.util.DBType;
import com.seeon.common.util.SeeonConfig;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class GptApiTest {
    private static SeeonConfig config;
    private static final Logger log = Logger.getLogger("SearchBlog");
    private static Connection conn;

    public GptApiTest() {
        conn = null;
        config = SeeonConfig.getInstance();
    }

    public static void main(String[] args) {
        GptApiTest searchBlog = new GptApiTest();

        int testPid0 = 992492;
        int testPid4 = 377;

        //에러 발생 케이스 -> \ 및 " 제거
        int testPid1 = 56628;
        int testPid2 = 3000;
        searchBlog.gtpTarget(testPid2);


    }

    // 블로그 리뷰 데이터 insert, update
    private void gtpTarget(int pid){
        dbConnection();

        while (true){
            try {
                //네이버 블로그 검색 대상 목록
                List<Map<String,Object>> reqApiPidList = getTargetList(pid);


                for (int i = 0; i < reqApiPidList.size(); i++) {
                    //주소데이터 설정
                    String addr  = addressFilter(String.valueOf(reqApiPidList.get(i).get("addr")));
                    String apiQuery  = String.valueOf(reqApiPidList.get(i).get("pname")) + " " + addr;

                    String mainPid = String.valueOf(reqApiPidList.get(i).get("pid"));

                    //naverAPi
                    blogReviewUpdate(mainPid, apiQuery);
                }

            }catch (Exception e){
                log.error(e);
                break;
            }

        }
        if (conn != null) dbClose();
    }

    //Api Start
    public void blogReviewUpdate(String tmpPid, String textSearch) throws Exception {
        int pid = Integer.parseInt(tmpPid);

        ////네이버 클라이언트 정보
        String clientId = config.get("com.naver.search.client.id");
        String clientSecret = config.get("com.naver.search.client.secret");
        String text = URLEncoder.encode(textSearch, "UTF-8");
        String apiURL = "https://openapi.naver.com/v1/search/blog.json?query=" + text + "&start=1&display=100";

        //저장 index, keyword, gtp사용 토큰
        List<Long> savePidBlogList = new ArrayList<>();
        Set<String> saveKeywordList = new HashSet<>();
        Long totalToken = 0L;

        try {
            log.info("==================================================== naverBlogSearch Start : " + textSearch);
            URL url = new URL(apiURL);
            HttpURLConnection con = (HttpURLConnection)url.openConnection();
            con.setRequestMethod("GET");
            con.setRequestProperty("X-Naver-Client-Id", clientId);
            con.setRequestProperty("X-Naver-Client-Secret", clientSecret);
            int responseCode = con.getResponseCode();
            BufferedReader br;
            if(responseCode == 200) { // 정상 호출
                br = new BufferedReader(new InputStreamReader(con.getInputStream()));

                String inputLine;
                StringBuffer response = new StringBuffer();
                while ((inputLine = br.readLine()) != null) {
                    response.append(inputLine);
                }
                br.close();

                // 이모티콘 필터
                String jsonString = response.toString();
                Pattern emoticons = Pattern.compile("[\\uD83C-\\uDBFF\\uDC00-\\uDFFF]+"); // 이모티콘
                Matcher emoticonsMatcher = emoticons.matcher(jsonString);
                jsonString = emoticonsMatcher.replaceAll("");

                JSONParser parser = new JSONParser();
                JSONObject obj = (JSONObject) parser.parse(jsonString);
                JSONArray items = (JSONArray) obj.get("items");

                List<String> mainGroupItems = groupItems(textSearch, items, 10);
                //log.info(mainGroupItems.toString());

                log.info("mainGroupItems Cnt " + mainGroupItems.size());
                for (int i = 0; i < mainGroupItems.size(); i++) {
                    log.info("subGroup start : " + i);

                    Map<String, Object> chatGptReult = null;
                    try {
                        chatGptReult = chatGptApi(pid, textSearch, mainGroupItems.get(i).toString());
                    } catch (Exception e) {
                        log.error("chatGptReult error : "  + e);
                    }
                    List<Long> responseGtpApiPid = (List<Long>) chatGptReult.get("indexList");
                    Set<String> responseGtpApiKeyword = (Set<String>) chatGptReult.get("keywordList");
                    Long responseToken = (Long) chatGptReult.get("totalToken");

                    if (responseToken != null) {
                        totalToken += responseToken;
                    }

                    if (!responseGtpApiPid.isEmpty()) {
                        savePidBlogList.addAll(responseGtpApiPid);
                    }

                    if (!responseGtpApiKeyword.isEmpty()){
                        saveKeywordList.addAll(responseGtpApiKeyword);
                    }
                }

            }else {
                log.info("Naver Blog Search Api 실패");
            }
        } catch (Exception e) {
            log.error("blogReviewUpdate Fail : " + e);
        }
    }


    public Map<String, Object> chatGptApi(int pid, String query, String jsonToStringList) throws Exception {
        long beforeTime = System.currentTimeMillis();
        Map<String, Object> resutMap = new HashMap<>();
        List<Long> resultGptApiIndexList = new ArrayList<>();
        Set<String> resultGptApiKeywordList = new HashSet<>();
        Long totalToken = 0L;

        try {
            log.info("==================================================== chatGptApi Start");

            // Gpt API URL
            URL url = new URL("https://api.openai.com/v1/chat/completions");

            // 연결 설정
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Authorization", "Bearer api 값");

            // gpt 요청 프롬포트
            String rContent = SeeonConfig.getInstance().get("com.seeon.chatGptApi.send.text");
            rContent = rContent.replaceAll("\\{0\\}", query);

            // \ 및 " 제거
            String reqReplaceText = jsonToStringList.replaceAll("\"", "'").replaceAll("\\\\", "");

            // 요청 본문 데이터
            String requestData = "{ \"model\": \"gpt-3.5-turbo-1106\", \"response_format\": { \"type\": \"json_object\" }, \"messages\": [ " +
                                 "{ \"role\": \"system\", \"content\": \"" + rContent + "\" }, " +
                                 "{ \"role\": \"user\", \"content\": \"" + reqReplaceText + "\" } ] }";
            log.info("requestData:" + requestData);

            // 요청 전송
            connection.setDoOutput(true);
            try (OutputStream os = connection.getOutputStream()) {
                byte[] input = requestData.getBytes("utf-8");
                os.write(input, 0, input.length);
            } catch (Exception e) {
                log.error("chatGpt OutputStream error:" + e);
            }

            // 응답 읽기
            try (BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream(), "utf-8"))) {
                StringBuilder gtpResponse = new StringBuilder();
                String responseLine;
                while ((responseLine = br.readLine()) != null) {
                    gtpResponse.append(responseLine.trim());
                }

                //응답데이터 확인시
                log.info("gptResponse :" + gtpResponse.toString());

                JSONArray gptItems = null;
                JSONParser gtpParser = new JSONParser();

                // 사용 토큰
                try {
                    JSONObject gptObj = (JSONObject) gtpParser.parse(gtpResponse.toString());

                    gptItems = (JSONArray) gptObj.get("choices");
                    JSONObject usageObject = (JSONObject) gptObj.get("usage");
                    Long useTokens = (Long) usageObject.get("total_tokens");

                    totalToken = useTokens;

                }catch (Exception e){
                    log.error("Gpt JSONObject Parser Error : " + e);
                }

                // 응답 index, keyword
                try {
                    if (gptItems != null && !gptItems.isEmpty()) {
                        for (Object item : gptItems) {
                            JSONObject element = (JSONObject) item;
                            JSONObject message = (JSONObject) element.get("message");
                            String content = (String) message.get("content");
                            JSONObject contentJson = (JSONObject) gtpParser.parse(content);
                            log.info("gpt item response : " + contentJson.toString());

                            try {

                                JSONArray indicesArray = (JSONArray) contentJson.get("INDICES");
                                if (!indicesArray.isEmpty()){
                                    List<Long> indicesList = jsonArrayToLongList(indicesArray);
                                    resultGptApiIndexList.addAll(indicesList);
                                }

                            }catch (Exception e){
                                log.error("indicesArray Parser Error : "  + e);
                            }

                            try {
                                JSONArray keywordsArray = (JSONArray) contentJson.get("KEYWORDS");
                                if (!keywordsArray.isEmpty()){
                                    List<String> keywordsList = jsonArrayToStringList(keywordsArray);
                                    resultGptApiKeywordList.addAll(keywordsList);
                                }
                            }catch (Exception e){
                                log.error("keywordsArray Parser Error : "  + e);
                            }

                        }
                        resutMap.put("indexList" , resultGptApiIndexList);
                        resutMap.put("keywordList" , resultGptApiKeywordList);
                    }
                }catch (Exception e){
                    log.error("Gpt Json Parser Error : " + e +  e.getMessage());
                }

                resutMap.put("totalToken" , totalToken);
            } catch (Exception e) {
                log.error("chatGpt response Data error:" + e);

            }

        } catch (Exception e) {
            log.error("chatGptApi Fail:" + e);
        } finally {
            long afterTime = System.currentTimeMillis();
            long secDiffTime = (afterTime - beforeTime) / 1000;
            log.info("==================================================== chatGptApi() 소요(m) : " + secDiffTime + " / " + query);
        }

        return resutMap;
    }

    //Api End

    private boolean checkCntForGtpReviewTarget(int target1, int target2){
        if (target1 > target2){
            return true;
        }
        return false;
    }

    /**
     * groupSize 만큼 잘라서 List 반환
     * 블로그 검색 api 리턴값 chatGtpApi 호출전 최대 10개씩 자른 것
     */
    public static List<String> groupItems(String tagName, JSONArray items, int groupSize) {
        List<String> mainGoupList = new ArrayList<>();
        int itemCount = items.size();

        for (int i = 0; i < itemCount; i += groupSize) {
            int endIndex = Math.min(i + groupSize, itemCount);
            StringBuffer subGroupList = new StringBuffer();
            for (int j = i; j < endIndex; j++) {
                JSONObject element = new JSONObject();
                element = (JSONObject) items.get(j);
                //subGroupList.append("{  “tagName“:“" + tagName + "“, “INDEX“: “" + j + "“, “title“:“" + (String)element.get("title") + "“, “description“:“" + (String)element.get("description") + "“}");
                subGroupList.append("{  tagName:" + tagName + ", INDEX :" + j + ", title:" + (String)element.get("title") + ", description:" + (String)element.get("description") + "}");
            }

            mainGoupList.add(String.valueOf(subGroupList));
        }

        return mainGoupList;
    }

    //주소 정보 필터링
    private String addressFilter(String addr){
        String smallArea = "";
        char delText = 0;

        addr += " ";

        String[] keywords = {"동 ", "면 ", "읍 ", "가 "};

        for (String keyword : keywords) {
            int index = addr.indexOf(keyword);

            if (index != -1) {
                smallArea = addr.substring(0, index).trim();
                delText = addr.charAt(index);
                break;
            }
        }

        if (!smallArea.isEmpty()) {
            int lastSpaceIndex = smallArea.lastIndexOf(" ");
            if (lastSpaceIndex != -1) {
                smallArea = smallArea.substring(lastSpaceIndex + 1);
            }

            if (smallArea.length() < 2){
                smallArea += delText;
            }
        }else {
            smallArea = addr;
        }

        return smallArea;
    }

    private static List<Long> jsonArrayToLongList(JSONArray jsonArray) {
        List<Long> list = new ArrayList<>();
        for (Object obj : jsonArray) {
            list.add((Long) obj);
        }
        return list;
    }

    private static List<String> jsonArrayToStringList(JSONArray jsonArray) {
        List<String> list = new ArrayList<>();
        for (Object obj : jsonArray) {
            list.add((String) obj);
        }
        return list;
    }

    /*SQL  ----- */
    private List<Map<String,Object>> getTargetList(int pid) {
        List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();

        StringBuffer query = new StringBuffer();
        query.append("SELECT GBRT.PID, P.ADDR, P.PNAME");
        query.append("  FROM GPT_BLOG_REVIEW_TARGET GBRT ");
        query.append("       JOIN PLACE P ON P.PID = GBRT.PID");
        query.append(" WHERE P.PID = ?");
        query.append(" LIMIT 1 ");

        try (PreparedStatement pstmt = conn.prepareStatement(query.toString())) {

            pstmt.setInt(1, pid); // pid를 설정

            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> info = new HashMap<>();
                    info.put("pid", rs.getInt("PID"));
                    info.put("addr", rs.getString("ADDR"));
                    info.put("pname", rs.getString("PNAME"));
                    list.add(info);
                }
            }

        } catch (SQLException e) {
            log.error("getTargetList 오류: ", e);
        }

        return list;
    }

    private void dbConnection() {
        try {
            log.debug("Database connection opened");
            conn = ConnectionFactory.getInstance().getConnection(DBType.mysql, 1);
        } catch (SQLException e) {
            log.error("Error opening database connection: " + e);
        } catch (Exception e) {
            log.error("Run Time Error " + e);
        }
    }

    private void dbClose() {
        if (conn != null) {
            try {
                conn.close();
                conn = null;
                log.info("Database connection closed");
            } catch (SQLException e) {
                conn = null;
                log.error("Error closing database connection: " + e);
            }
        }
    }

    private void dbRollBack() {
        try {
            if (conn != null && !conn.isClosed()) {
                conn.rollback();
            }
        } catch (SQLException ex) {
            log.error("Rollback failed: ", ex);
        }
    }
}
