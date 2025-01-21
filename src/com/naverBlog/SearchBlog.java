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


public class SearchBlog {
    private static SeeonConfig config;
    private static final Logger log = Logger.getLogger("SearchBlog");
    private static Connection conn;

    public SearchBlog() {
        conn = null;
        config = SeeonConfig.getInstance();
    }

    private static String tablePlace = "PLACE";
    private static String tableGtpBlogReviewTarget = "GPT_BLOG_REVIEW_TARGET";
    private static String stopGptTarget = "GPT_BLOG_REVIEW_TARGET_ERROR";


    public static void main(String[] args) {
        //String runMode = StringHelper.NVL(System.getProperty("runMode"));
        //log.info("start RunMode : " + runMode);

        SearchBlog searchBlog = new SearchBlog();
        searchBlog.gtpTarget();



        /*
        String testAddr1 = "충청북도 진천군 진천읍 읍내리 471-179번지";
        String testAddr2 = "서울특별시 마포구 중동 355-2";
        String testAddr3 = "충청북도 진천군 진천읍";
        String testAddr4 = "충청남도 천안시 동남구 병천면 병천리 166-21";
        String testAddr5 = "인천시 강화군 내가면 외포리 582-24";
        String testAddr6 = "서울특별시  강남구  역삼동  814-6";
        String testAddrFilter1 = SearchBlog.addressFilter(testAddr1);
        String testAddrFilter2 = SearchBlog.addressFilter(testAddr2);
        String testAddrFilter3 = SearchBlog.addressFilter(testAddr3);
        String testAddrFilter4 = SearchBlog.addressFilter(testAddr4);
        String testAddrFilter5 = SearchBlog.addressFilter(testAddr5);
        String testAddrFilter6 = SearchBlog.addressFilter(testAddr6);
        log.info(testAddrFilter1);
        log.info(testAddrFilter2);
        log.info(testAddrFilter3);
        log.info(testAddrFilter4);
        log.info(testAddrFilter5);
        log.info(testAddrFilter6);
        */

    }

    /**
     * 블로그 리뷰 데이터 insert, update 실행
     */
    private void gtpTarget(){
        dbConnection();

        while (true){
            try {
                //네이버 Api 요청, Gtp 요청 에러 확인
                int stopCnt = getCntTargetTable(stopGptTarget);
                if (stopCnt > 10 ){
                    break;
                }

                //폐업된 장소 target 제거
                try {
                    int delTargetCnt = getPlaceCloseCnt();
                    if (delTargetCnt > 0){
                        deleteTargetFromPlaceClose();
                    }
                }catch (Exception e){
                    log.error("getPlaceCloseCnt : "  + e);
                }

                //네이버 블로그 검색 대상 목록
                List<Map<String,Object>> reqApiPidList = getTargetList();

                if (reqApiPidList.isEmpty()){
                    log.info("reqApiPidList Empty ");
                    int palceCnt = getCntTargetTable(tablePlace);
                    int targetCnt = getCntTargetTable(tableGtpBlogReviewTarget);
                    boolean excute = checkCntForGtpReviewTarget(palceCnt, targetCnt);
                    if (excute){    //블로그 리뷰 업데이트 대상 테이블, place 테이블 비교
                        int cntSetTarget = setTargetList();
                        log.info("대상 등록 수 : " + cntSetTarget);
                    }else {         //초기화
                        int checkBreak = resetGtpBlogReviewTarget();
                        if (checkBreak == 0){
                            break;
                        }
                    }
                }else {
                    for (int i = 0; i < reqApiPidList.size(); i++) {
                        //주소데이터 설정
                        String addr  = addressFilter(String.valueOf(reqApiPidList.get(i).get("addr")));
                        String apiQuery  = String.valueOf(reqApiPidList.get(i).get("pname")) + " " + addr;

                        String mainPid = String.valueOf(reqApiPidList.get(i).get("pid"));

                        //기존 블로그 리뷰 N처리
                        setUseYnForGtpBlogReview(mainPid, "N");

                        //naverAPi
                        blogReviewUpdate(mainPid, apiQuery);
                    }
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
        List<String> mainGroupItems = new ArrayList<>();
        Long totalToken = 0L;

        try {
            log.info("==================================================== naverBlogSearch Start : " + textSearch + " / pid : " + tmpPid);
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

                if (!items.isEmpty()){
                    mainGroupItems = groupItems(textSearch, items, 10);
                }

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

                //유효한 데이터 있는 경우
                if (!savePidBlogList.isEmpty()) {
                    try {
                        log.info("To be saved, blog Review Cnt : " + savePidBlogList.size());
                        for (Long getPid : savePidBlogList) {
                            JSONObject element = (JSONObject) items.get(Integer.parseInt(String.valueOf(getPid)));
                            String title = (String) element.get("title");
                            String description = (String) element.get("description");
                            String blog_link = (String) element.get("link");
                            String postdate = (String) element.get("postdate");

                            addGtpBlogReview(pid, title, description, blog_link, postdate);
                        }
                    }catch (Exception e){
                        log.info("savePidBlogList e : "  + e);
                    }

                    try {
                        log.info("To be saved, Keyword Cnt : " + saveKeywordList.size());
                        //기존 키워드 삭제
                        deletePlaceGtpKeyword(pid);

                        // 키워드 저장
                        for (String getKeyword : saveKeywordList) {
                            addGtpKeyword(pid, getKeyword);
                        }

                    }catch (Exception e){
                        log.info("Keyword delete or save error: "  + e);
                    }

                    // 기존 리뷰 삭제
                    deleteBlogRiew(pid, "N");

                    // 리뷰 데이터 업데이트한 pid 대상 Y처리
                    updateProcessEndForTarget(pid, "Y", textSearch.trim(), totalToken);
                }else {
                    // 기존 블로그 리뷰 원복
                    setUseYnForGtpBlogReview(tmpPid, "Y");

                    // 업데이트 대상 실패처리
                    updateProcessEndForTarget(pid, "F", textSearch.trim(), totalToken);
                }
            }else {
                log.info("Naver Blog Search Api 실패");
                updateProcessEndForTarget(pid, "F", textSearch.trim(), totalToken);
                addTargetError(pid, "NaverBlogSearch Api response fail", TargetErrorCode.NAVER.getValue());
            }
        } catch (Exception e) {
            log.error("blogReviewUpdate Fail : " + e);
            updateProcessEndForTarget(pid, "F", textSearch.trim(), totalToken);
            //addTargetError(pid, "blogReviewUpdate Fail : " + e , TargetErrorCode.ETC.getValue());
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
            connection.setRequestProperty("Authorization", "Bearer gpt 사용될 Authorization 값");

            // gpt 요청 프롬포트
            String rContent = SeeonConfig.getInstance().get("com.seeon.chatGptApi.send.text");
            rContent = rContent.replaceAll("\\{0\\}", query);

            // \ 및 " 제거
            String reqReplaceText = jsonToStringList.replaceAll("\"", "'").replaceAll("\\\\", "");

            // 요청 본문 데이터
            String requestData = "{ \"model\": \"gpt-3.5-turbo-1106\", \"response_format\": { \"type\": \"json_object\" }, \"messages\": [ " +
                                 "{ \"role\": \"system\", \"content\": \"" + rContent + "\" }, " +
                                 "{ \"role\": \"user\", \"content\": \"" + reqReplaceText + "\" } ] }";
            //log.info("requestData:" + requestData);

            // 요청 전송
            connection.setDoOutput(true);
            try (OutputStream os = connection.getOutputStream()) {
                byte[] input = requestData.getBytes("utf-8");
                os.write(input, 0, input.length);
            } catch (Exception e) {
                log.error("chatGpt OutputStream error:" + e);
                addTargetError(pid, "chatGpt OutputStream error ", TargetErrorCode.STOP_GPT.getValue());
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
                    addTargetError(pid, "Gpt JSONObject Error :" + e, TargetErrorCode.GPT.getValue());
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
                                addTargetError(pid, "Gpt Josn indicesArray Parser Error :" + e, TargetErrorCode.GPT.getValue());
                            }

                            try {
                                JSONArray keywordsArray = (JSONArray) contentJson.get("KEYWORDS");
                                if (!keywordsArray.isEmpty()){
                                    List<String> keywordsList = jsonArrayToStringList(keywordsArray);
                                    resultGptApiKeywordList.addAll(keywordsList);
                                }
                            }catch (Exception e){
                                log.error("keywordsArray Parser Error : "  + e);
                                addTargetError(pid, "Gpt Josn keywordsArray Parser Error :" + e, TargetErrorCode.GPT.getValue());
                            }

                        }
                        resutMap.put("indexList" , resultGptApiIndexList);
                        resutMap.put("keywordList" , resultGptApiKeywordList);
                    }
                }catch (Exception e){
                    log.error("Gpt Json Parser Error : " + e +  e.getMessage());
                    addTargetError(pid, "Gpt Json Parser Error :" + e, TargetErrorCode.GPT.getValue());
                }

                resutMap.put("totalToken" , totalToken);
            } catch (Exception e) {
                log.error("chatGpt response Data error:" + e);

                //제이슨 파싱 에러시에도 저장됨
                addTargetError(pid, "chatGpt response Data error " + e, TargetErrorCode.GPT.getValue());
            }

        } catch (Exception e) {
            log.error("chatGptApi Fail:" + e);
            addTargetError(pid, "chatGptApi Fail ", TargetErrorCode.GPT.getValue());
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
    private List<Map<String,Object>> getTargetList() {
        List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();

        StringBuffer query = new StringBuffer();
        query.append("SELECT GBRT.PID, P.ADDR, P.PNAME");
        query.append("  FROM GPT_BLOG_REVIEW_TARGET GBRT ");
        query.append("       JOIN PLACE P ON P.PID = GBRT.PID");
        query.append(" WHERE GBRT.GUBUN = 'N' ");
        query.append(" ORDER BY GBRT.ORD ");
        query.append(" LIMIT 1 ");

        try (PreparedStatement pstmt = conn.prepareStatement(query.toString());
             ResultSet rs = pstmt.executeQuery()) {

            while (rs.next()) {
                Map<String, Object> info = new HashMap<>();
                info.put("pid", rs.getInt("PID"));
                info.put("addr", rs.getString("ADDR"));
                info.put("pname", rs.getString("PNAME"));
                list.add(info);
            }
        } catch (SQLException e) {
            log.error("getTargetList 오류: ", e);
        }

        return list;
    }

    private int getCntTargetTable(String targetTable){
        int targetCnt = 0;
        try (PreparedStatement pstmt = conn.prepareStatement(buildQuery(targetTable));
             ResultSet rs = pstmt.executeQuery()) {

            while (rs.next()) {
                targetCnt = rs.getInt("CNT");
            }
        } catch (Exception e) {
            log.error("checkCntForGptReviewTarget error : ", e);
        }

        return targetCnt;
    }

    private String buildQuery(String targetTable) {
        StringBuilder query = new StringBuilder();
        if (targetTable.equals(tablePlace)) {
            query.append("SELECT COUNT(DISTINCT P.PID) CNT ");
            query.append("  FROM PLACE P ");
            query.append("       JOIN PLACE_DETAIL PD ON (PD.PID = P.PID AND PD.CLOSE_TYPE != 'C') ");
            query.append("       JOIN HOTPLACE H ON (H.PID = P.PID AND H.USE_YN = 'Y') ");
            query.append(" WHERE P.IS_KO = 'Y' AND P.ADDR IS NOT NULL ");
        } else if (targetTable.equals(tableGtpBlogReviewTarget)){
            query.append("SELECT COUNT(*) CNT FROM ");
            query.append(targetTable);
        } else if (targetTable.equals(stopGptTarget)){
            query.append("SELECT COUNT(*) CNT FROM ");
            query.append(targetTable);
            query.append(" WHERE ERROR_CODE IN ('E0002', 'E0004')");
            query.append("       AND REG_DT > DATE_ADD(NOW(),INTERVAL - 3 MINUTE)");
        }
        return query.toString();
    }

    private void setUseYnForGtpBlogReview(String targetPid, String useYn) throws Exception {
        StringBuffer query = new StringBuffer();
        int pid = Integer.parseInt(targetPid);
        query.append("UPDATE GPT_BLOG_REVIEW SET USE_YN = ? WHERE PID = ?");

        try (PreparedStatement pstmt = conn.prepareStatement(query.toString())) {
            conn.setAutoCommit(false);
            pstmt.setString(1, useYn);
            pstmt.setInt(2, pid);
            pstmt.executeUpdate();

            conn.commit();
        } catch (Exception e) {
            log.error("setUseYnForGptBlogReview sql :", e);
            dbRollBack();
        }finally {
            conn.setAutoCommit(true);
        }
    }

    private int resetGtpBlogReviewTarget() throws Exception {
        int cnt = 0;
        log.info("------ start reset GtpBlogReviewTarget");
        StringBuffer query = new StringBuffer();
        query.append("UPDATE GPT_BLOG_REVIEW_TARGET SET GUBUN = 'N' WHERE REG_DT < DATE_ADD(NOW(),INTERVAL - 3 MONTH)");

        try (PreparedStatement pstmt = conn.prepareStatement(query.toString())) {
            conn.setAutoCommit(false);
            cnt = pstmt.executeUpdate();

            conn.commit();
        } catch (Exception e) {
            log.error("resetGtpBlogReviewTarget sql :", e);
            dbRollBack();
        } finally {
            conn.setAutoCommit(true);
        }
        return cnt;
    }

    private int setTargetList() throws Exception{
        int plimit = Integer.parseInt(config.get("com.seeon.chatGptApi.place.target.limit"));
        int cnt = 0;
        StringBuffer query = new StringBuffer();
        query.append("INSERT INTO GPT_BLOG_REVIEW_TARGET(PID, REG_DT, API_QUERY, TOKEN_CNT, GUBUN, ORD)");
        query.append(" SELECT T.PID, NOW(), null, 0, 'N', T.P_ORD ");
        query.append("  FROM (SELECT DISTINCT P.PID, P.pname, IFNULL(H.BEST_ORD, 7) P_ORD");
        query.append("          FROM PLACE P");
        query.append("               JOIN PLACE_DETAIL PD ON (PD.PID = P.PID AND PD.CLOSE_TYPE != 'C')");
        query.append("               JOIN HOTPLACE H ON ( H.pid = P.pid AND H.USE_YN = 'Y') ");
        query.append("               LEFT OUTER JOIN GPT_BLOG_REVIEW_TARGET GBRT ON (GBRT.PID = P.PID) ");
        query.append("         WHERE GBRT.PID IS NULL AND P.IS_KO = 'Y' AND P.ADDR IS NOT NULL ) T ");
        query.append("         ORDER BY T.P_ORD");
        query.append("         LIMIT ?");

        log.info("-------- setTargetList start");
        try (PreparedStatement pstmt = conn.prepareStatement(query.toString())) {
            conn.setAutoCommit(false);
            pstmt.setInt(1, plimit);
            cnt = pstmt.executeUpdate();

            conn.commit();
        } catch (Exception e) {
            log.error("setTargetList sql :", e);
            dbRollBack();
        }finally {
            conn.setAutoCommit(true);
        }

        return cnt;
    }

    private int deleteTargetFromPlaceClose() throws Exception {
        int cnt = 0;
        StringBuffer query = new StringBuffer();
        query.append("DELETE FROM GPT_BLOG_REVIEW_TARGET ");
        query.append(" WHERE PID IN ( ");
        query.append("                SELECT PID");
        query.append("                  FROM ( SELECT GBRT.PID");
        query.append("                           FROM PLACE P");
        query.append("                                JOIN PLACE_DETAIL PD ON PD.PID = P.PID AND PD.CLOSE_TYPE = 'C'");
        query.append("                                JOIN GPT_BLOG_REVIEW_TARGET GBRT ON GBRT.PID = P.PID  ) AS T ");
        query.append("                )");

        log.info("-------- deleteTargetFromPlaceClose start");
        try (PreparedStatement pstmt = conn.prepareStatement(query.toString())) {
            conn.setAutoCommit(false);
            cnt = pstmt.executeUpdate();

            conn.commit();
        } catch (Exception e) {
            log.error("deletePlaceTarget :", e);
            dbRollBack();
        } finally {
            conn.setAutoCommit(true);
        }

        return cnt;
    }

    private int deleteBlogRiew(int pid, String useYn) throws Exception {
        int cnt = 0;
        String query = "DELETE FROM GPT_BLOG_REVIEW WHERE USE_YN = ? AND PID = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(query.toString())) {
            conn.setAutoCommit(false);
            pstmt.setString(1, useYn);
            pstmt.setInt(2, pid);
            cnt = pstmt.executeUpdate();

            conn.commit();
        } catch (Exception e) {
            log.error("deleteBlogRiew :", e);
            dbRollBack();
        } finally {
            conn.setAutoCommit(true);
        }

        return cnt;
    }

    private void deletePlaceGtpKeyword(int pid) throws Exception {
        String query = "DELETE FROM GPT_KEYWORD WHERE PID = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(query.toString())) {
            conn.setAutoCommit(false);
            pstmt.setInt(1, pid);
            pstmt.executeUpdate();

            conn.commit();
        } catch (Exception e) {
            log.error("deleteBlogRiew :", e);
            dbRollBack();
        } finally {
            conn.setAutoCommit(true);
        }
    }

    private int getPlaceCloseCnt() throws Exception {
        int cnt = 0;
        StringBuffer query = new StringBuffer();
        query.append("  SELECT COUNT(*) AS cnt");
        query.append("    FROM PLACE P ");
        query.append("         JOIN PLACE_DETAIL PD ON PD.PID = P.PID AND PD.CLOSE_TYPE = 'C'");
        query.append("         JOIN GPT_BLOG_REVIEW_TARGET GBRT ON GBRT.PID = P.PID");

        try (PreparedStatement pstmt = conn.prepareStatement(query.toString());
             ResultSet rs = pstmt.executeQuery()) {

            if (rs.next()) {
                cnt = rs.getInt("cnt");
            }

        } catch (SQLException e) {
            log.error("getPlaceCloseCnt SQL : " + e);
        }

        return cnt;
    }

    private void addTargetError(int pid, String content, String errorType) throws Exception {
        String query = "INSERT INTO GPT_BLOG_REVIEW_TARGET_ERROR(PID, ERROR_CODE, CONTENT, REG_DT) VALUES(?, ?, ?, NOW())";

        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            conn.setAutoCommit(false);

            pstmt.setInt(1, pid);
            pstmt.setString(2, errorType);
            pstmt.setString(3, content);
            pstmt.executeUpdate();

            conn.commit();
        } catch (Exception e) {
            log.error("addTargetError sql :", e);
        } finally {
            conn.setAutoCommit(true);
        }
    }

    private void updateProcessEndForTarget(int pid, String gubun, String apiQuery, Long tokenCnt) throws Exception {
        String query = "UPDATE GPT_BLOG_REVIEW_TARGET SET GUBUN = ?, API_QUERY = ?, TOKEN_CNT = ?,  REG_DT = NOW() WHERE PID = ?";
        int tmpTokenCnt = Integer.parseInt(String.valueOf(tokenCnt));

        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            conn.setAutoCommit(false);
            pstmt.setString(1, gubun);
            pstmt.setString(2, apiQuery);
            pstmt.setLong(3, tokenCnt);
            pstmt.setInt(4, pid);

            pstmt.executeUpdate();

            conn.commit();
        } catch (Exception e) {
            log.error("updateProcessEndForTarget sql :", e);
        } finally {
            conn.setAutoCommit(true);
        }
    }

    private void addGtpBlogReview(int pid, String title, String description, String link, String postdate) throws Exception {
        String query = "INSERT INTO GPT_BLOG_REVIEW(PID, TITLE, DESCRIPTION, BLOG_LINK, POSTDATE, USE_YN) VALUES(?, ?, ?, ?, ?, 'Y')";

        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            conn.setAutoCommit(false);

            pstmt.setInt(1, pid);
            pstmt.setString(2, title);
            pstmt.setString(3, description);
            pstmt.setString(4, link);
            pstmt.setString(5, postdate);
            pstmt.executeUpdate();

            conn.commit();
        } catch (Exception e) {
            log.error("addGtpBlogReview sql :", e);
        } finally {
            conn.setAutoCommit(true);
        }
    }

    private void addGtpKeyword(int pid, String keyword) throws Exception {
        String query = "INSERT INTO GPT_KEYWORD(PID, GPT_KEYWORD) VALUES(?, ?)";

        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            conn.setAutoCommit(false);

            pstmt.setInt(1, pid);
            pstmt.setString(2, keyword);
            pstmt.executeUpdate();

            conn.commit();
        } catch (Exception e) {
            log.error("addGtpKeyword sql :", e);
        } finally {
            conn.setAutoCommit(true);
        }
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
