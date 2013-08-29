/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package filterjob;

import java.io.*;

import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

/**
 *
 * @author amaury
 */
public class ParseFilterFile extends Configured implements Tool {

    public static final Logger LOG = Logger.getLogger(ParseFilterFile.class.getName());
    public static final String FILTER_PATH = "filter.path";
    private int wordcount = 0;

    public ParseFilterFile(Configuration conf) {
        setConf(conf);
    }

    @Override
    public int run(String[] strings)  {
        
        return 0;
    }
    
    public void generate() throws IOException {

        String s, id, tempTitle, title, titleConcat, regexTitle, regexTitleConcat, firstname, tempFirstname, lastname, tempLastname, regexLastnameCorpus, regexLastname, regexFullAuthorCorpus;
        ArrayList<webpage> books = FilterDriver.BookList;
        BufferedReader FilterFileDescriptor;
        FilterFileDescriptor = getFilterFileDescriptor();

        while ((s = FilterFileDescriptor.readLine()) != null) {
            
            StringTokenizer st = new StringTokenizer(s, "|");
            
            tempTitle = st.nextToken();
            
            title = getPreparedTitle(tempTitle, false);
            regexTitle = getRegexTitle(title, false);
            
            titleConcat = getPreparedTitle(tempTitle, true);
            regexTitleConcat = getRegexTitle(titleConcat, true);
            
            firstname = st.nextToken().trim();
            tempFirstname = getPreparedFirstname(firstname);
            
            lastname = st.nextToken().trim();
            
            tempLastname = getPreparedAuthor(lastname);
            regexLastnameCorpus = getRegexAuthor(tempLastname);
            
            regexFullAuthorCorpus = getRegexFullAuthor(tempFirstname, tempLastname);
            
            regexLastname = getRegexAuthor(tempLastname);
            regexLastname = regexLastname.replace("'", "(_|\\s|\\-)?");
            
            
            id = st.nextToken();
            books.add(new webpage(id, title, wordcount, firstname, lastname, regexTitle.trim(), regexTitleConcat.trim(), regexLastnameCorpus.trim(), regexLastname.trim(), regexFullAuthorCorpus.trim()));
        }
        LOG.log(Level.INFO, "BookList Length : {0}", books.size());
    }
    
    private String getPreparedTitle(String pTitle, Boolean isConcat)
    {
        String preparedTitle;
        
        preparedTitle = pTitle.toLowerCase();
        preparedTitle = preparedTitle.replaceAll("\\b(a|o)\\b", "");
        preparedTitle = preparedTitle.replaceAll("\\bl(e|a|es)\\b", "");
        preparedTitle = preparedTitle.replaceAll("\\b(m|t|s)(on|a|es)\\b", "");
        preparedTitle = preparedTitle.replaceAll("\\bune?\\b", "");
        preparedTitle = preparedTitle.replaceAll("\\bes?t\\b", "");
        preparedTitle = preparedTitle.replaceAll("\\bce(lle)?s?\\b", "");
        preparedTitle = preparedTitle.replaceAll("\\bd(es?|u)\\b", "");
        
        if(isConcat)
            preparedTitle = preparedTitle.replaceAll("'", "");
        else
        {
            preparedTitle = preparedTitle.replaceAll("'", " ");
            preparedTitle = preparedTitle.replaceAll("\\b(c|d|j|l|m|n|s|t|:|-|&|a)\\b", "");
        }
        preparedTitle = preparedTitle.replaceAll("\\s{2,}", " ");
        return preparedTitle;
    }
    
    private String getRegexTitle(String pPreparedRegexTitle, Boolean isConcat)
    {
        String[] tempArray;
        String tempRegexTitle;
        
        tempArray = pPreparedRegexTitle.trim().split(" ");
        tempRegexTitle = ".*\\b" + StringUtils.join(tempArray, "\\b.*\\b") + "\\b.*";
        if(isConcat == false)
            wordcount = tempArray.length;
        
        return tempRegexTitle;
    }
    
    private String getPreparedFirstname(String pFirstname)
    {
        String preparedFirstname;
        
        preparedFirstname = pFirstname.replace("-", "@(\\.)?(\\s)?(\\-)?");
        
        return preparedFirstname;
    }
    
    private String getPreparedAuthor(String pAuthor)
    {
        String preparedAuthor;
        
        preparedAuthor = pAuthor.replace("-", "@(_|\\s|\\-)");
        
        return preparedAuthor;
    }
    
    private String getRegexAuthor(String pPreparedAuthor)
    {
        String tempRegexAuthor;
        String[] tempArray;
        
        tempArray = pPreparedAuthor.trim().split("@");
        tempRegexAuthor = ".*" + StringUtils.join(tempArray, "") + ".*";
        
        return tempRegexAuthor;
    }
    
    private String getRegexFullAuthor(String pPreparedFirstname, String pPreparedLastname)
    {
        String tempRegexFullAuthor;
        String[] tempArrayFirstname, tempArrayLastname;
        
        tempArrayLastname = pPreparedLastname.trim().split("@");
        if(pPreparedFirstname.equals(" ") == false) {
            tempArrayFirstname = pPreparedFirstname.trim().split("@");
            //TODO gérer les prénoms avec une lettre et un point après: ajouter \\. dans la regex prenom
            tempRegexFullAuthor = ".*(" + StringUtils.join(tempArrayFirstname, "")+"(\\.)?\\s"+StringUtils.join(tempArrayLastname, "") + "|"+ StringUtils.join(tempArrayLastname, "") + "(,)?(\\s)?" + StringUtils.join(tempArrayFirstname, "")+"(\\.)?).*";
        }
        else
            tempRegexFullAuthor = ".*" +StringUtils.join(tempArrayLastname, "") + ".*";
        
        return tempRegexFullAuthor;
    }
    
    private BufferedReader getFilterFileDescriptor() throws FileNotFoundException, UnsupportedEncodingException {
        String FilterFile;
        FilterFile = getFilterFilePath();

        BufferedReader FilterFileDescriptor = new BufferedReader(
                new InputStreamReader(
                new FileInputStream(FilterFile), "UTF8"));

        return FilterFileDescriptor;
    }

    private String getFilterFilePath() {
        Configuration conf = getConf();

        String FilterFile = conf.getRaw(FILTER_PATH);

        return FilterFile;
    }
}
