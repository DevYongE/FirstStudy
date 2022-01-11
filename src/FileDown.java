import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileDown {
    public static Connection conn;
    public static Statement stmt;
    public static PreparedStatement pstmt;
    public static ResultSet rs;
    public static InputStream is= null;
    public static FileOutputStream fos = null;
    static String DBuser = "";
    static String DBpw = "";
    static String DBurl = "";

    static String OperType = "1"; //0: 운영 1: 개발

    public static void main(String[] args) {
        String TYPE1 =  args[0]; // 게시판 아이디?
        getConnection();
        List<Map<String, String>> resultList = new ArrayList<>();
        System.out.println("[ 첨부파일 다운로드를 시작 합니다. ]");
        resultList = getFileUrl();
        String serverPath = "scrapystore";
        String dbSavePath = "share_files/kms";
        System.out.println("[ "+resultList.size()+" 첨부파일 ROW 수 ]");

        for(int i=0; resultList.size() > i ;i++){
            String fileurl=resultList.get(i).get("FILE_URL");
            String filenm=resultList.get(i).get("FILE_NAME");

            String REG_DATE;

            Map<String, String> resultMap= resultList.get(i);
            setFile(resultMap);
            String[] FILENM_ARRAY = filenm.replaceAll("\r", "").replaceAll(" ", "\n").replaceAll("\r\n", "\n").split("\n");
            String[] FILEURL_ARRAY = fileurl.replaceAll("\r", "").replaceAll(" ", "\n").replaceAll("\r\n", "\n").split("\n");
            String site_code = resultList.get(i).get("SITE_CODE");
            for(int j=0; j < FILEURL_ARRAY.length;j++){
                try{
                    String originFile = getFileName(FILEURL_ARRAY[j], FILENM_ARRAY[j]);
                    String file_code = "KMSF"+resultMap.get("FILE_SEQ");
                    System.out.println(FILEURL_ARRAY[j]);
                    long CONTS_SEQ = Long.parseLong(resultMap.get("CONTS_SEQ"));
                    String SOURCENAME = resultMap.get("SOURCENAME");
                    String CRWL_URL = resultMap.get("CRWL_URL");
                    int SITE_CODE = Integer.parseInt(resultMap.get("SITE_CODE"));
                    REG_DATE = resultMap.get("REG_DATE");
                    int file_seq = Integer.parseInt(resultMap.get("FILE_SEQ"));
                    String tempServerPath = dbSavePath+"/"+SITE_CODE+"/";
                    String tempServerRealPath = serverPath+"\\"+SITE_CODE+"\\";
                    String FILEEX = "";		// 파일 확장자
                    //if(FILENM_TEMP.lastIndexOf(".") > 0)
                    if(originFile.length() - originFile.lastIndexOf(".") < 6)
                        FILEEX = "."+originFile.trim().substring(originFile.lastIndexOf(".")+1);
                    else{
                        try{
                            originFile = getFileName(FILEURL_ARRAY[i], FILENM_ARRAY[j]+"_"+i);
                        }catch (Exception e){
                            System.out.println(e.getMessage());
                        }
                        if(originFile.length() - originFile.lastIndexOf(".") < 6)
                            FILEEX = "."+originFile.trim().substring(originFile.lastIndexOf(".")+1);
                    }

                    String makeDr = mkdirs(tempServerRealPath);

                    String finalPath = "";
                    String finalRealPath = "";
                    if(originFile.contains("hwp") || originFile.contains("pdf") ){
                        finalPath = tempServerPath+file_code+"_"+Integer.toString(j+1)+FILEEX;
                        finalRealPath = tempServerRealPath+file_code+"_"+Integer.toString(j+1)+FILEEX;
                    }else{
                        finalPath = tempServerPath+file_code+"_"+Integer.toString(j+1)+".pdf";
                        finalRealPath = tempServerRealPath+file_code+"_"+Integer.toString(j+1)+".pdf";
                    }


                   // System.out.println("c:\\"+finalRealPath);
                    insertAttFile(CONTS_SEQ, "",SOURCENAME, file_seq, 0, originFile, finalPath, CRWL_URL, file_seq, SITE_CODE,REG_DATE );
                    //insertAttFile(CONTS_SEQ,String SITENAME, String SOURCENAME, int FILE_SEQ_DQ, int LOG_SEQ, String FILE_NAME, String FILE_PATH, String CRWL_URL, int UID, String SITE_CODE, String REG_DATE)

                    System.out.println(FILEURL_ARRAY[j]);
                    //getFileDown(FILEURL_ARRAY[j], site_code, "D:\\"+finalRealPath);
                    getFileDown(FILEURL_ARRAY[j], SITE_CODE, "D:\\crawling_data\\"+finalRealPath);

                    fos.close();
                    is.close();
                }catch (Exception e) {
                    System.out.println("에러 메세지" + e.getMessage());
                }
            }
            System.out.println(filenm);
            System.out.println(resultMap.get(i));
            System.out.println("["+(i+1)+" / "+(resultList.size())+"]");
            System.out.println();
        }
    }
    private static String setFileChange(String oriName){

        return null;
    }
    private static String mkdirs(String directory) throws IOException, Exception {
        String returnString = "폴더가 존재합니다.";
        File desti = new File(directory);
        if(!desti.exists()){
            //없다면 생성
            desti.mkdirs();
            returnString = "폴더를 생성합니다.";
        }
        return returnString;
    }

    /**
     * DB 연결
     * @return conn
     */
    public static Connection getConnection(){
//        if(OperType.equals("0")){
////                DB_IP = "211.45.203.237"; // IP
////                DB_PORT = "5432"; // 접속 Port
////                DB_SID = "postgres"; // SID
////                DB_ID = "postgres"; // 접속할 유저 ID
////                DB_PWD = "koiha123"; // 접속할 유저 패스워드
//            DBuser = "postgres";
//            DBpw = "koiha123";
//            DBurl = "jdbc:postgresql://211.45.203.237/postgres";

        try {
            DBuser = "postgres";
            DBpw = "koiha123";
            DBurl = "jdbc:postgresql://127.0.0.1/postgres";
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(DBurl, DBuser, DBpw);
            System.out.println("==========> 연결 성공");
        } catch (SQLException | ClassNotFoundException e) {
            // TODO Auto-generated catch block
            System.out.println("==========> DB 연결 실패" + e.getMessage());
            e.printStackTrace();
        }
        return conn;
    }
    public static List<Map<String, String>> getFileUrl(){
        List<Map<String, String>> resultList = new ArrayList<Map<String, String>>();
        String SELECT_SQL = "";
        try{
            stmt = conn.createStatement();
            SELECT_SQL =
                    "SELECT UNIQUE_ID, CONTS_SEQ, CRWL_URL, SOURCENAME, FILE_SEQ, FILE_NAME, FILE_URL, REG_DATE, SITE_CODE FROM DQ_CRWL"
                            + " WHERE FILE_URL IS NOT NULL  AND FILE_NAME IS NOT NULL AND FILE_SEQ  NOT IN (SELECT FILE_SEQ FROM TBL_FILE)";

            ResultSet rs = stmt.executeQuery(SELECT_SQL);
            while(rs.next()){
                Map<String, String> resultMap= new HashMap();
                resultMap.put("CONTS_SEQ", rs.getString("CONTS_SEQ"));
                resultMap.put("CRWL_URL", rs.getString("CRWL_URL"));
                resultMap.put("SOURCENAME", rs.getString("SOURCENAME"));
                resultMap.put("FILE_SEQ", rs.getString("FILE_SEQ"));
                resultMap.put("FILE_NAME", rs.getString("FILE_NAME"));
                resultMap.put("FILE_URL", rs.getString("FILE_URL"));
                resultMap.put("REG_DATE", rs.getString("REG_DATE"));
                resultMap.put("SITE_CODE", rs.getString("SITE_CODE"));
                resultList.add(resultMap);
            }
        } catch(Exception e){
            e.printStackTrace();
        }
        return resultList;
    }
    public static void setFile(Map<String, String> resultMap){
        String FILENM = "";
        String FILEURL = "";
        String CONTS_SEQ="";
        String REG_DATE ="";

        if( FILEURL != null && !FILEURL.equals("")) {
            FILEURL = resultMap.get("FILE_URL");
            if (FILEURL == null) FILEURL = "";

            FILEURL.replaceAll("\r\n", "\n");
            String[] FILEURL_ARRAY = FILEURL.split("\n");

            for (int i = 0; FILEURL_ARRAY.length - 1 >= i; i++) {
                try {
                    Thread.sleep(500);
                    if (FILEURL_ARRAY[i].contains("http://") || FILEURL_ARRAY[i].contains("https://")) {
                        if (FILEURL_ARRAY[i].contains("http://")) {
                            FILEURL_ARRAY[i] = FILEURL_ARRAY[i].substring(FILEURL_ARRAY[i].indexOf("http://"));
                        } else if (FILEURL_ARRAY[i].contains("https://")) {
                            FILEURL_ARRAY[i] = FILEURL_ARRAY[i].substring(FILEURL_ARRAY[i].indexOf("https://"));
                        }
                    } else {
                        FILEURL_ARRAY[i] = "http://" + FILEURL_ARRAY[i];
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void getFileDown(String file_url, int path_code, String filepath){
        try {
            URL url = new URL(file_url);
            System.out.println("File_URL====> "+file_url);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            int responseCode = conn.getResponseCode();

            System.out.println("responseCode " + responseCode);

            // Status 가 200 일 때
            if (responseCode == HttpURLConnection.HTTP_OK) {
                String fileName = "";
                String disposition = conn.getHeaderField("Content-Disposition");
                String contentType = conn.getContentType();
                int contentLength = conn.getContentLength();
                // 일반적으로 Content-Disposition 헤더에 있지만
                // 없을 경우 url 에서 추출해 내면 된다.
                if (disposition != null) {
                    String target = "filename=";
                    int index = disposition.indexOf(target);

                    if (index != -1) {
                        fileName = getFileName(file_url,disposition.substring(index + target.length() + 1));
                        disposition.length();
                    }
                } else {
                    fileName = file_url.substring(file_url.lastIndexOf("/") + 1);
                    file_url.length();
                }
                System.out.println("Content-Type = " + contentType);
                System.out.println("Content-Disposition = " + disposition);
                System.out.println("Content-Length = " + contentLength);
                System.out.println("fileName = " + fileName);

                is = conn.getInputStream();
//              fos = new FileOutputStream(new File(filepath));

                Files.copy(is, Paths.get(filepath), StandardCopyOption.REPLACE_EXISTING);
                Thread.sleep(4000);
                final int BUFFER_SIZE = 8192;
                int bytesRead;
                byte[] buffer = new byte[BUFFER_SIZE];
                while ((bytesRead = is.read(buffer)) != -1) {
                    fos.write(buffer, 0, bytesRead);
                }
//                fos.close();
                is.close();
                System.out.println("File downloaded");
                conn.setConnectTimeout(4000);
                conn.setReadTimeout(5000);
            } else {
                System.out.println("No file to download. Server replied HTTP code: " + responseCode);
            }
            conn.disconnect();
        } catch (Exception e) {
            System.out.println("An error occurred while trying to download a file."+e.getMessage());
            e.printStackTrace();
            try {
                if (is != null) {
                    is.close();
                }
                if (fos != null) {
                    fos.close();
                }
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }
    }

    private static String getFileName(String filePath, String fileName) {
        try {
            URL obj = new URL(filePath);
            URLConnection conn = obj.openConnection();
            conn.setConnectTimeout(20000);
            conn.setReadTimeout(20000);
            String header = conn.getHeaderField("Content-Disposition");
            if(header != null) {
                header = header.split("=")[1];
                header = header.replaceAll("\"", "");
                header = header.replaceAll(";", "");
                header = URLDecoder.decode(header,"8859_1");
                String s = new String(header.getBytes("8859_1"), "UTF-8");
                if(s.contains("�")) {
                    String s2 = new String(header.getBytes("8859_1"), "EUC-KR");
                    return s2;
                }else {
                    return s;
                }
            }
            return fileName;
        }catch(Exception e) {
            System.out.println("첨부파일명 가져오기 실패 : " + fileName);
            e.printStackTrace();
        }
        return fileName;
    }
    public static int insertAttFile(long SEQ,String SITENAME, String SOURCENAME, int FILE_SEQ_DQ, int LOG_SEQ, String FILE_NAME, String FILE_PATH, String CRWL_URL, int UID, int SITE_CODE,  String REG_DATE)    {
        ResultSet res = null;

        int returnCode =0;
        String INSERT_SQL ="";
        java.util.Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        try {
            INSERT_SQL="INSERT INTO tbl_file" +
                    "(seq, sitename, sourcename, file_seq, log_seq, originalfilename, filename, uid, crwl_url, site_code, created)" +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            pstmt = conn.prepareStatement(INSERT_SQL);
            pstmt.setLong(1, SEQ);
            pstmt.setString(2, SITENAME);
            pstmt.setString(3, SOURCENAME);
            pstmt.setInt(4, FILE_SEQ_DQ);
            pstmt.setInt(5, LOG_SEQ);
            pstmt.setString(6, FILE_NAME);
            pstmt.setString(7, FILE_PATH);
            pstmt.setInt(8, UID);
            pstmt.setString(9, CRWL_URL);
            pstmt.setInt(10, SITE_CODE);
            pstmt.setString(11, REG_DATE);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println("["+e.getMessage()+"] CONTS_SEQ: "+SEQ+" / FILE_SEQ_DQ: "+FILE_SEQ_DQ);
        }
        return returnCode;
    }
}
