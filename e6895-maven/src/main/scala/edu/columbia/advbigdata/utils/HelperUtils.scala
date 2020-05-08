/** Commonly used Apache Spark functions.
  *
  * This is a package for all the commonly used helper functions
  */
package edu.columbia.advbigdata.utils

import java.time.LocalDateTime 
import java.time.format.DateTimeFormatter

/**
 * Factory for package [[utils.helpers]] 
 * 
 */
package object helpers {  

    /** Returns a datetime string in dataproc format YY/MM/dd HH:mm:ss
    *
    *  @param leader options leader text following datetime (e.g., INFO)
    *  @return returns a string of datetime + leader
    */
    def timeNow (leader: String = "") : String = {
        // dataproc time format example 20/03/29 04:38:41
        val now = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YY/MM/dd HH:mm:ss"))
        if(leader.trim.isEmpty)
            return now + " "
        else
            return now + " " + leader + " "

    }
}