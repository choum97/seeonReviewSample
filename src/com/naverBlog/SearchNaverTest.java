package com.seeon.naverBlog;

import com.seeon.common.util.SeeonConfig;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.Connection;


public class SearchNaverTest {
    private static SeeonConfig config;
    private static final Logger log = Logger.getLogger("SearchBlog");
    private static Connection conn;

    public SearchNaverTest() {
        conn = null;
        config = SeeonConfig.getInstance();
    }

    public static void main(String[] args) {
        SearchNaverTest searchBlog = new SearchNaverTest();
        String testText = "봉피양 판교점 삼평";
        String testText2 = "아건 대현";
        String testText3 = "온더보더 코엑스도심공항점 삼성";
        String testText4 = "계곡가든꽃게장군산본부점 개정";

        String testText5 = "에스프레소 퍼블릭 역삼";
        String testText6 = "온더보더 코엑스도심공항점 삼성";

        try {
            searchBlog.blogReviewUpdate(testText5);

        }catch (Exception e){
            log.error(e);
        }
    }

    public void blogReviewUpdate(String textSearch) throws Exception {
        ////네이버 클라이언트 정보
        String clientId = config.get("com.naver.search.client.id");
        String clientSecret = config.get("com.naver.search.client.secret");
        String text = URLEncoder.encode(textSearch, "UTF-8");
        String apiURL = "https://openapi.naver.com/v1/search/blog.json?query=" + text + "&start=1&display=100";

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

                String jsonString = response.toString();
                JSONParser parser = new JSONParser();
                JSONObject obj = (JSONObject) parser.parse(jsonString);
                JSONArray items = (JSONArray) obj.get("items");

                log.info(items.toString().replaceAll("\\{\"link", "\n\\{\"link"));

            }
        } catch (Exception e) {
            log.error("blogReviewUpdate Fail : " + e);
        }
    }

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

}
