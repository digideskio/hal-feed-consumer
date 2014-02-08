package com.qmetric.feed.consumer

import static java.lang.System.currentTimeMillis

class DomainNameFactory {
    public static userPrefixedDomainName(String username)
    {
        "${username}-${currentTimeMillis()}".toString()
    }
}
