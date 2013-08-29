/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package filterjob;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringEscapeUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

/**
 *
 * @author amaury
 */

public class Webpage implements Comparable<Webpage>, Writable, DBWritable {

    public static final Logger LOG = Logger.getLogger(Webpage.class.getName());
    private String book_id;
    private final String title;
    private final String firstname;
    private final String lastname;
    private final int wordcount;
    private final Pattern regexTitle;
    private final Pattern regexTitleQuoteConcat;
    private final Pattern regexAuthorCorpus;
    private final Pattern regexFullAuthorCorpus;
    private final Pattern regexAuthorQuoteConcat;
    private String status = null;
    private String content = "";
    public String FullyDecodedUrl = "";

    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeChars(book_id);
        out.writeChars(content);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        book_id = in.readUTF();
        content = in.readUTF();
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setString(1, book_id);
        statement.setString(1, content);
    }

    @Override
    public void readFields(ResultSet rs) throws SQLException {
        book_id = rs.getString(1);
        content = rs.getString(2);
    }

    public Webpage(String id, String title, int wordcount, String firstname, String lastname, String regexTitle, String regexTitleConcat, String regexAuthorCorpus, String regexAuthorConcat, String regexFullAuthorCorpus) {

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
        this.book_id = id;

    }

    public void info() {
        Object[] INFOS = new Object[]{
            title,
            FullyDecodedUrl,
            regexAuthorQuoteConcat.toString(),
            regexAuthorCorpus.toString(),
            regexFullAuthorCorpus.toString(),
            regexTitle.toString(),
            regexTitleQuoteConcat.toString(),};

        LOG.log(Level.INFO,
                "Title  : {1} ON {2}\n"
                + "RegexAuthor  : {3}\n"
                + "RegexAuthorCorpus  : {4}\n"
                + "RegexFullAuthorCorpus  : {5}\n"
                + "RegexTitle  : {6}\n"
                + "RegexTitleConcat  : {7}", INFOS);
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
        return regexFullAuthorCorpus.matcher(stripAccents(content)).find();
    }

    public String getId() {
        return book_id;
    }

    public boolean isEmpty() {
        if (title.isEmpty()
                || firstname.isEmpty()
                || book_id.isEmpty()) {
            return false;
        }
        return true;
    }

    public int compareTo(Webpage arg0) {

        return (arg0.wordcount - wordcount);
    }

    public void Dump() {
        log("################# BEGIN DUMP ################");
        log(" id = " + book_id);
        log(" title = " + title);
        log(" firstname = " + firstname);
        log(" lastname = " + lastname);
        log(" wordcount = " + wordcount);
        log(" status = " + status);
        log(" regexAuthor = " + regexAuthorQuoteConcat);
        log(" regexAuthorCorpus = " + regexAuthorCorpus);
        log(" regexTitle = " + regexTitle);
        log(" regexTitleConcat = " + regexTitleQuoteConcat);
        log("################# END DUMP ################");
    }

    public void log(String line) {
        System.out.println(line);
    }

    private String stripAccents(String s) {

        s = s.replaceAll("[éèêë]", "e");
        s = s.replaceAll("[ûù]", "u");
        s = s.replaceAll("[ïî]", "i");
        s = s.replaceAll("[àâ]", "a");
        s = s.replaceAll("ô", "o");

        s = s.replaceAll("[ÈÉÊË]", "E");
        s = s.replaceAll("[ÛÙ]", "U");
        s = s.replaceAll("[ÏÎ]", "I");
        s = s.replaceAll("[ÀÂ]", "A");
        s = s.replaceAll("Ô", "O");

        return s;
    }
}
