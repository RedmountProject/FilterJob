package filterjob;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Book implements WritableComparable<Book>, DBWritable {

    public static Logger LOG = Logger.getLogger(Book.class.getName());
    private IntWritable id;// = new IntWritable(0);
    private String title;
    private String firstname;
    private String lastname;
    private int wordcount;
    private Pattern regexTitle;
    private Pattern regexTitleQuoteConcat;
    private Pattern regexAuthorCorpus;
    private Pattern regexFullAuthorCorpus;
    private Pattern regexAuthorQuoteConcat;
    private String status = null;
    private String content = "";
    private String FullyDecodedUrl = "";

    public Book() {
    }

    public Book(IntWritable id, String title, int wordcount, String firstname, String lastname, String regexTitle, String regexTitleConcat, String regexAuthorCorpus, String regexAuthorConcat, String regexFullAuthorCorpus) {

        this.title = title;
        this.firstname = firstname;
        this.lastname = lastname;
        this.status = null;
        this.wordcount = wordcount;
        this.regexAuthorCorpus = Pattern.compile(regexAuthorCorpus, Pattern.CASE_INSENSITIVE);
        this.regexFullAuthorCorpus = Pattern.compile(regexFullAuthorCorpus, Pattern.CASE_INSENSITIVE);
        this.regexAuthorQuoteConcat = Pattern.compile(regexAuthorConcat, Pattern.CASE_INSENSITIVE);
        this.regexTitle = Pattern.compile(regexTitle, Pattern.CASE_INSENSITIVE);
        this.regexTitleQuoteConcat = Pattern.compile(regexTitleConcat, Pattern.CASE_INSENSITIVE);
        this.id = id;
    }

    public boolean Match(String pContent, String FullyDecodedUrl, String pTitle) {
        return Decide(BuildMask(pTitle, FullyDecodedUrl), StringEscapeUtils.escapeSql(pContent), FullyDecodedUrl);
    }

    private int BuildMask(String FullyDecodedUrl, String ExtractedTitle) {
        if (regexTitle.matcher(FullyDecodedUrl).find()) {
            if (regexAuthorQuoteConcat.matcher(FullyDecodedUrl).find()) {
                return 1;
            } else if (regexAuthorQuoteConcat.matcher(ExtractedTitle).find()) {
                return 1;
            } else {
                return 2;
            }
        } else if (regexTitle.matcher(ExtractedTitle).find()) {
            if (regexAuthorQuoteConcat.matcher(FullyDecodedUrl).find()) {
                return 1;
            } else if (regexAuthorQuoteConcat.matcher(ExtractedTitle).find()) {
                return 1;
            } else {
                return 2;
            }
        }
        return 0;

    }

    private boolean Decide(int Mask, String content, String url) {
        
        if (Mask > 0) {
            if (Mask == 1 || (Mask == 2 && CheckCorpus(content))) {
                this.content = content;
                this.FullyDecodedUrl = url;
                return true;
            }
        }
        return false;
    }

    public boolean CheckCorpus(String content) {
        return regexFullAuthorCorpus.matcher(Utils.stripAccents(content)).find();
    }

    public IntWritable getId() {
        return this.id;
    }

    public String geturl() {
        return this.FullyDecodedUrl;
    }

    public String getContent() {
        return this.content;
    }

    public boolean isEmpty() {
        if (title.isEmpty()
                || firstname.isEmpty()
                || id != null) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(Book arg0) {
        return (this.id.compareTo(arg0.id));
    }

    @Override
    public void write(DataOutput d) throws IOException {
        if (null == this.id) {
            d.writeBoolean(true);
        } else {
            d.writeBoolean(false);
            this.id.write(d);
        }
        if (null == this.content) {
            d.writeBoolean(true);
        } else {
            d.writeBoolean(false);
            Text.writeString(d, this.content);
        }
        if (null == this.title) {
            d.writeBoolean(true);
        } else {
            d.writeBoolean(false);
            Text.writeString(d, this.title);
        }
        if (null == this.FullyDecodedUrl) {
            d.writeBoolean(true);
        } else {
            d.writeBoolean(false);
            Text.writeString(d, this.FullyDecodedUrl);
        }
    }
    
    private void  clear(){
        this.id = new IntWritable();
        this.content  = new String();
        this.title  = new String();
        this.FullyDecodedUrl = new String();
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        clear();
        
        if (di.readBoolean()) {
            this.id = null;
        } else {
            this.id.readFields(di);
        }


        if (di.readBoolean()) {
            this.content = null;
        } else {
            this.content = Text.readString(di);
        }

        if (di.readBoolean()) {
            this.title = null;
        } else {
            this.title = Text.readString(di);
        }


        if (di.readBoolean()) {
            this.FullyDecodedUrl = null;
        } else {
            this.FullyDecodedUrl = Text.readString(di);
        }
    }

    @Override
    public void write(PreparedStatement ps) throws SQLException {
       
        if (this.content.isEmpty() || (this.id.get() == 0)) {
            LOG.log(Level.ERROR, "Content or id is empty : " + this.content.length()+ "  id : "+this.id);
            return;
        }

        if (!SelectContent(ps)) {
            InsertContent(ps);
        } else {
            UpdateContent(ps);
        }
        AddUrl(ps);

        
//        JdbcWritableBridge.writeInteger(id.get(), 1 , 12, ps);
//        JdbcWritableBridge.writeString(content, 2 , -1, ps);
    }

    @Override
    public void readFields(ResultSet rs) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet : You're not Supposed to read Data From Database");
    }
    private String requete;

    private synchronized void AddUrl(PreparedStatement rs) throws SQLException {

        requete = "INSERT INTO `book_url` ( `bookUrl_id`, `book_id`, `url`, `insert_date` )"
                + " VALUES((SELECT IFNULL( MAX( bookUrl_id ) , 0 )+1"
                + " FROM `book_url` target), '" +this.id + "', '" + this.FullyDecodedUrl + "', '" + getDate() + "');";

        try {
            rs.execute(requete);
        } catch (SQLIntegrityConstraintViolationException e) {
            LOG.log(Level.ERROR, "Injecting Url into database : " + e);
        }
    }

    private synchronized void InsertContent(PreparedStatement ps) throws SQLException {

        requete = "INSERT INTO `book_content` ( `bookContent_id`, `book_id`, `content`, `nb_match` )"
                + " VALUES((SELECT IFNULL( MAX( bookContent_id ) , 0 )+1"
                + " FROM `book_content` target), '" + this.id + "', '" + this.content + "', 1);";
        try {
            ps.execute(requete);
        } catch (SQLIntegrityConstraintViolationException e) {
            LOG.log(Level.ERROR, "Inserting Content into database");
        }
    }

    private synchronized boolean SelectContent(PreparedStatement ps) throws SQLException {

        requete = "SELECT 1 FROM `book_content` WHERE `book_id` = '" + this.id + "';";

        ps.execute(requete);

        ResultSet rs = ps.getResultSet();
        if (rs.next()) {
            return true;
        }
        return false;

    }

    private synchronized void UpdateContent(PreparedStatement ps) throws SQLException {

        requete = "UPDATE `book_content` SET content = CONCAT( `content` , '" + this.content + "'), nb_match = nb_match+1  WHERE `book_id` = '" + this.id + "';";

        try {
            ps.executeUpdate(requete, Statement.RETURN_GENERATED_KEYS);

        } catch (SQLIntegrityConstraintViolationException e) {

            LOG.log(Level.ERROR, "Injecting Content into database " + e);

        }
    }

    private static String getDate() {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date(System.currentTimeMillis()));
    }
}
