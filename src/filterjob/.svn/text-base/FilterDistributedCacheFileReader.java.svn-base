/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package filterjob;

/**
 *
 * @author amaury
 */
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.*;
import org.apache.commons.lang.StringUtils;


import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;

import org.apache.log4j.Logger;

public class FilterDistributedCacheFileReader
        implements DistributedCacheFileReader<IntWritable, Book>, Iterator<Pair<IntWritable, Book>> {

    public static final Logger LOG = Logger.getLogger(FilterDistributedCacheFileReader.class.getName());
    LineIterator iter;
    private int wordcount = 0;

    @Override
    public void init(File f) throws IOException {
        iter = FileUtils.lineIterator(f);
    }

    @Override
    public void close() {
        iter.close();
    }

    @Override
    public Iterator<Pair<IntWritable, Book>> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        return iter.hasNext();
    }
    
    String line, id, tempTitle, title, titleConcat, regexTitle, regexTitleConcat, firstname, tempFirstname, lastname, tempLastname, regexLastnameCorpus, regexLastname, regexFullAuthorCorpus = null;
    Pair<IntWritable, Book> pair;
    String[] parts;

    @Override
    public Pair<IntWritable, Book> next() {


        line = (String) iter.next();
        pair = new Pair<>();
        parts = StringUtils.split(line, "|", 5);

        tempTitle = parts[0];
        firstname = parts[1].trim();
        lastname = parts[2].trim();
        id = parts[3].trim();


        title = getPreparedTitle(tempTitle, false);
        regexTitle = getRegexTitle(title, false);

        titleConcat = getPreparedTitle(tempTitle, true);
        regexTitleConcat = getRegexTitle(titleConcat, true);

        tempFirstname = getPreparedFirstname(firstname);

        tempLastname = getPreparedAuthor(lastname);
        regexLastnameCorpus = getRegexAuthor(tempLastname);

        regexFullAuthorCorpus = getRegexFullAuthor(tempFirstname, tempLastname);

        regexLastname = getRegexAuthor(tempLastname);
        regexLastname = regexLastname.replace("'", "(_|\\s|\\-)?");

        pair.setKey(new IntWritable(Integer.parseInt(id)));

        if (parts.length > 3) {
            pair.setData(new Book(new IntWritable(Integer.parseInt(id)), title, wordcount, firstname, lastname, regexTitle.trim(), regexTitleConcat.trim(), regexLastnameCorpus.trim(), regexLastname.trim(), regexFullAuthorCorpus.trim()));
        }
         return pair;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
    private final String[] FirstInputReplace = {"\\b(a|o)\\b", "\\bl(e|a|es)\\b", "\\b(m|t|s)(on|a|es)\\b", "\\bune?\\b", "\\bes?t\\b", "\\bce(lle)?s?\\b", "\\bd(es?|u)\\b"};
    private final String[] SecondInputReplace = {"'", "\\b(c|d|j|l|m|n|s|t|:|-|&|a)\\b"};
    private final String[] EmptyString = {"", "", "", "", "", "", ""};
    private final String[] SecondOutputReplace = {" ", EmptyString[0]};

    private String getPreparedTitle(String pTitle, Boolean isConcat) {
        String preparedTitle;


        preparedTitle = pTitle.toLowerCase();
        preparedTitle = StringUtils.replaceEachRepeatedly(preparedTitle, FirstInputReplace, EmptyString);

        if (isConcat) {
            preparedTitle = preparedTitle.replaceAll("'", EmptyString[0]);
        } else {
            preparedTitle = StringUtils.replaceEachRepeatedly(preparedTitle, SecondInputReplace, SecondOutputReplace);

        }
        preparedTitle = preparedTitle.replaceAll("\\s{2,}", " ");
        return preparedTitle;
    }

    private String getRegexTitle(String pPreparedRegexTitle, Boolean isConcat) {
        String[] tempArray;
        String tempRegexTitle;

        tempArray = pPreparedRegexTitle.trim().split(" ");
        tempRegexTitle = ".*\\b" + StringUtils.join(tempArray, "\\b.*\\b") + "\\b.*";
        if (isConcat == false) {
            wordcount = tempArray.length;
        }

        return tempRegexTitle;
    }

    private String getPreparedFirstname(String pFirstname) {
        String preparedFirstname;

        preparedFirstname = pFirstname.replace("-", "@(\\.)?(\\s)?(\\-)?");

        return preparedFirstname;
    }

    private String getPreparedAuthor(String pAuthor) {
        return StringUtils.replace(pAuthor, "-", "@(_|\\s|\\-)");
    }

    private String getRegexAuthor(String pPreparedAuthor) {
        String tempRegexAuthor;
        String[] tempArray;

        tempArray = pPreparedAuthor.trim().split("@");
        tempRegexAuthor = ".*" + StringUtils.join(tempArray, "") + ".*";

        return tempRegexAuthor;
    }

    private String getRegexFullAuthor(String pPreparedFirstname, String pPreparedLastname) {
        String tempRegexFullAuthor;
        String[] tempArrayFirstname, tempArrayLastname;

        tempArrayLastname = pPreparedLastname.trim().split("@");
        if (pPreparedFirstname.equals(" ") == false) {
            tempArrayFirstname = pPreparedFirstname.trim().split("@");
            //TODO gérer les prénoms avec une lettre et un point après: ajouter \\. dans la regex prenom
            tempRegexFullAuthor = ".*(" + StringUtils.join(tempArrayFirstname, "") + "(\\.)?\\s" + StringUtils.join(tempArrayLastname, "") + "|" + StringUtils.join(tempArrayLastname, "") + "(,)?(\\s)?" + StringUtils.join(tempArrayFirstname, "") + "(\\.)?).*";
        } else {
            tempRegexFullAuthor = ".*" + StringUtils.join(tempArrayLastname, "") + ".*";
        }

        return tempRegexFullAuthor;
    }
}