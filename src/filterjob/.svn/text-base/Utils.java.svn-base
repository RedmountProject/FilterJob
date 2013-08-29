/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package filterjob;

import org.apache.commons.lang.StringUtils;

/**
 *
 * @author amaury
 */
public class Utils {

        
     private static final String[] InputReplace =  {"é", "è", "ê", "ë", "û", "ù", "ü", "ï", "î", "à", "â", "ö", "ô", "ç"};
    private static final String[] OutputReplace = {"e", "e", "e", "e", "u", "u", "u", "i", "i", "a", "a", "o", "o", "c"};

public static String stripAccents(String s) {
        return StringUtils.replaceEachRepeatedly(s, InputReplace, OutputReplace);
    }
}
