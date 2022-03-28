package com.example.dsw.kafka.streams.pageview;



public class POJO {
    // POJO classes
    static public class PageView {
        public Long viewtime;
        public String userid;
        public String pageid;
    }

    static public class User {
        public String userid;
        public String regionid;
        public Long registertime;
        public String gender;
    }
    
    static public class PageViewByGender {
        public String user;
        public String page;
        public String gender;
    }

    static public class WindowedPageViewByRegion {
        public long windowStart;
        public String region;
    }

    static public class GenderCount {
        public long count;
        public String gender;
    }
}