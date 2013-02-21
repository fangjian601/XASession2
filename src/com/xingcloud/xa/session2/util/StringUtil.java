package com.xingcloud.xa.session2.util;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * User: Jian Fang
 * Date: 13-2-21
 * Time: 上午10:32
 */
public class StringUtil {
    public static String getMD5(String input){
        MessageDigest m = null;
        try {
            m = MessageDigest.getInstance("MD5");
            m.reset();
            m.update(input.getBytes());
            byte[] digest = m.digest();
            BigInteger bigInt = new BigInteger(1,digest);
            String hashText = bigInt.toString(16);
            while(hashText.length() < 32 ){
                hashText = "0"+hashText;
            }
            return hashText;
        } catch (NoSuchAlgorithmException e) {
            return "";
        }
    }
}
