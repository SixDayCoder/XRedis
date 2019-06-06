#include <string>
#include <vector>
#include <map>

#include "RedisConn.h"

using namespace std;
using namespace db;

void RedisTestString()
{
    RedisConn conn("127.0.0.1", 6379);

    printf("---------------------------TEST STRING BEGIN---------------------------\n");

    int32_t x = 12345678;
    int64_t y = 1234567898;
    float f1 = 0.5f;
    float f2 = 123.0f;
    float f3 = -0.5f;
    float f4 = 0.0f;

    conn.Del("int32");
    conn.Del("int64");
    conn.Del("f1");
    conn.Del("f2");
    conn.Del("f3");
    conn.Del("f4");
    conn.Del("str1");
    conn.Del("str2");

    conn.Set("int32", x);
    conn.Set("int64", y);
    conn.Set("f1", f1);
    conn.Set("f2", f2);
    conn.Set("f3", f3);
    conn.Set("f4", f4);

    int32_t x1 = -50;
    conn.Get("int32", &x1);
    printf("%d\n", x1);

    int64_t y1 = -77777777;
    conn.Get("int64", &y1);
    printf("%lld\n", y1);

    float f;
    conn.Get("f1", &f);
    printf("%f\n", f);

    conn.Get("f2", &f);
    printf("%f\n", f);

    conn.Get("f3", &f);
    printf("%f\n", f);

    conn.Get("f4", &f);
    printf("%f\n", f);

    const char* s1 = "strings1";
    conn.Set("str1", s1);

    std::string s2 = "132456";
    conn.Del("str2");
    conn.Set("str2", s2); 

    std::string res;
    conn.Get("str1", &res);
    printf("%s\n", res.c_str());

    conn.Get("str2", &res);
    printf("%s\n", res.c_str());

    printf("---------------------------TEST STRING END-----------------------------\n\n\n\n\n");
}

void RedisTestList()
{
    std::string str;
    vector<string> result;
    RedisConn conn("127.0.0.1", 6379);
    
    printf("---------------------------TEST LIST BEGIN---------------------------\n");

    //test lpush and lpop
    conn.Del("lst");
    conn.LPush("lst", 5);
    conn.LPush("lst", 77);
    conn.LPush("lst", -50.0f);
    conn.RPush("lst", "csdfs");
    conn.RPush("lst", "hellowrold");
    conn.RPush("lst", "hellowrold22");
    conn.LPop("lst", &str);
    conn.LPop("lst");
    conn.RPop("lst");
    
    //test llen and lrange
    printf("len is %d\n", conn.LLen("lst"));
    conn.LRange("lst", 0, -1, &result);
    for(int i = 0 ; i < result.size(); ++i) {
        printf("str : %s\n", result[i].c_str());
    }

    //test lindex and linsert and lset
    bool bFind = conn.LIndex("lst", 2, &str);
    if(bFind) {
        printf("lindex get : %s\n", str.c_str());
        if(conn.LInsert("lst", "hhhhhh2", str) > 0) {
            printf("linsert ok\n");
            conn.LRange("lst", 0, -1, &result);
            for(int i = 0 ; i < result.size(); ++i) {
                printf("str : %s\n", result[i].c_str());
            }
        }
               
    }
    bFind = conn.LIndex("lst", 40, &str);
    if(!bFind) {
        printf("lindex get fail\n");
    }
    conn.LSet("lst", 0, 1234);
    conn.LSet("lst", 0, 30.0f);
    conn.LSet("lst", 0, "557788");
    if(!conn.LSet("lst", 400, 2222)) {
        printf("lset list error, out of range\n");
    }
    if(conn.LSet("fake", 0, 1234)) {
        printf("lset fake list error\n");
    }

    printf("---------------------------TEST LIST END---------------------------\n\n\n\n\n");
}

void RedisTestSet()
{
    int n;
    std::string str;
    vector<string> result;
    RedisConn conn("127.0.0.1", 6379);

    printf("---------------------------TEST SET BEGIN---------------------------\n");

    conn.Del("s1");
    conn.Del("s2");
    conn.SAdd("s1", 1234);
    conn.SAdd("s1", "hell");
    conn.SAdd("s1", "56786");
    conn.SAdd("s1", 30.0f);
    conn.SMembers("s1", &result);
    n = conn.SCard("s1");
    printf("s1 len :%d\n", n);
    for(int i = 0 ; i < result.size(); ++i) {
        printf("member : %s\n", result[i].c_str());
    }

    conn.SAdd("s2", 1234);
    conn.SAdd("s2", "hell");
    conn.SAdd("s2", "worldba");
    conn.SAdd("s2",  77.0f);
    conn.SMembers("s2", &result);
    n = conn.SCard("s2");
    printf("s2 len :%d\n", n);
    for(int i = 0 ; i < result.size(); ++i) {
        printf("member : %s\n", result[i].c_str());
    }

    conn.SInter("s1", "s2", &result);
    printf("inter result\n");
    for(int i = 0 ; i < result.size(); ++i) {
        printf("member : %s\n", result[i].c_str());
    }

    conn.SUnion("s1", "s2", &result);
    printf("union result\n");
    for(int i = 0 ; i < result.size(); ++i) {
        printf("member : %s\n", result[i].c_str());
    }

    conn.SDiff("s1", "s2", &result);
    printf("diff result\n");
    for(int i = 0 ; i < result.size(); ++i) {
        printf("member : %s\n", result[i].c_str());
    }

    printf("---------------------------TEST SET END---------------------------\n\n\n\n\n");
}

void RedisTestHash()
{
    printf("---------------------------TEST HASH BEGIN---------------------------\n");

    RedisConn conn("127.0.0.1", 6379);
    bool bFind = false;
    std::string str;
    RedisReply::ArrayResult array;
    RedisReply::HashResult  hash;

    conn.Del("h1");
    conn.HSet("h1", "name", "sixday");
    conn.HSet("h1", "age", 10);
    conn.HSet("h1", "sex", "female");
    conn.HSet("h1", "score", 50.34f);

    printf("h1 size %d\n", conn.HLen("h1"));

    conn.HGet("h1", "name", &str);
    printf("name is :%s\n", str.c_str());

    conn.HGet("h1", "sex", &str);
    printf("sex is :%s\n", str.c_str());

    bFind = conn.HExists("h1", "sex");
    printf("exists sex : %d\n", bFind);

    bFind = conn.HExists("h1", "fake");
    printf("exists fake : %d\n", bFind);

    conn.HKeys("h1", &array);
    printf("h1 keys\n");
    for(int i = 0 ; i < array.size(); ++i) {
        printf("h1 key : %s\n", array[i].c_str());
    }

    conn.HVals("h1", &array);
    printf("h1 vals\n");
    for(int i = 0 ; i < array.size(); ++i) {
        printf("h1 val : %s\n", array[i].c_str());
    }

    conn.HGetAll("h1", &hash);
    printf("h1 all\n");
    for(auto it : hash) {
        printf("h1 key : %s, val : %s\n", it.first.c_str(), it.second.c_str());
    }

    printf("inc int val :%d\n", conn.HIncrby("h1", "age", 10));
    printf("inc fake int val :%d\n", conn.HIncrby("h1", "fake", 10));
    printf("inc string int val :%d\n", conn.HIncrby("h1", "name", 666));

    printf("inc float val :%f\n", conn.HIncrbyFloat("h1", "score", 33.0f));
    printf("inc fake float val :%f\n", conn.HIncrbyFloat("h1", "fake", 33.0f));
    printf("inc string float val :%f\n", conn.HIncrbyFloat("h1", "name", 33.0f));

    printf("---------------------------TEST HASH END---------------------------\n\n\n\n\n");
}

void RedisTestZSet()
{
    printf("---------------------------TEST ZSET BEGIN---------------------------\n");

    RedisConn conn("127.0.0.1", 6379);
    
    conn.Del("zs");
    conn.ZAdd("zs", 50.0f,  "f1");
    conn.ZAdd("zs", -50.0f, "f2");
    conn.ZAdd("zs", 0.0f,   "f3");
    conn.ZAdd("zs", 770.0f, "f4");
    conn.ZAdd("zs", 150.0f, "f5");
    conn.ZAdd("zs", -650.0f,"f6");
    conn.ZAdd("zs", 522.0f, "f7");
    conn.ZAdd("zs", 33.0f,  "f8");

    conn.ZAdd("zs", -40,    "i1");
    conn.ZAdd("zs", 50,     "i2");
    conn.ZAdd("zs", 0,      "i3");
    conn.ZAdd("zs", 333,    "i4");
    conn.ZAdd("zs", -40,    "i5");
    conn.ZAdd("zs", 75,     "i6");
    conn.ZAdd("zs", 21,     "i7");
    conn.ZAdd("zs", -666,   "i8");

    printf("f1 : %s\n", conn.ZScore("zs", "f1").c_str());
    printf("f2 : %s\n", conn.ZScore("zs", "f2").c_str());
    printf("f3 : %s\n", conn.ZScore("zs", "f3").c_str());
    printf("f4 : %s\n", conn.ZScore("zs", "f4").c_str());
    printf("i1 : %s\n", conn.ZScore("zs", "i1").c_str());
    printf("i2 : %s\n", conn.ZScore("zs", "i2").c_str());
    printf("i3 : %s\n", conn.ZScore("zs", "i3").c_str());
    printf("i4 : %s\n", conn.ZScore("zs", "i4").c_str());

    printf("inc zs f1 %s\n", conn.ZIncrby("zs", "f1", 50.0f).c_str());
    printf("inc zs f2 %s\n", conn.ZIncrby("zs", "f2", -50.0f).c_str());
    printf("inc zs i1 %s\n", conn.ZIncrby("zs", "i1", 10).c_str());
    printf("inc zs i2 %s\n", conn.ZIncrby("zs", "i2", 10.5f).c_str());
    
    printf("zcard zs : %d\n", conn.ZCard("zs"));
    printf("zcount zs : %d\n", conn.ZCount("zs", 0.0f, 50.0f));
    printf("zrank    zs i1 : %d\n", conn.ZRank("zs", "i1"));
    printf("zrevrank zs i1  : %d\n", conn.ZRevRank("zs", "i1"));

    printf("zrange : \n");
    RedisReply::ZSetResult result;
    conn.ZRange("zs", 0, -1, &result);
    for(int i = 0 ; i < result.size(); ++i) {
        printf("zrange key : %s\n", result[i].c_str());
    }

    printf("zrangewithscore : \n");
    RedisReply::ZSetWithScoreResult resultWithScore;
    conn.ZRangeWithScore("zs", 0, -1, &resultWithScore);
    for(int i = 0; i < resultWithScore.size(); ++i) {
             printf("zrangewithscore key : %s score : %s\n", 
             resultWithScore.Members[i].c_str(), resultWithScore.Scores[i].c_str());
    }

    printf("zrevrange : \n");
    conn.ZRevRange("zs", 0, -1, &result);
    for(int i = 0 ; i < result.size(); ++i) {
        printf("zrevrange key : %s\n", result[i].c_str());
    }

    printf("zrevrangewithscore : \n");
    conn.ZRevRangeWithScore("zs", 0, -1, &resultWithScore);
    for(int i = 0; i < resultWithScore.size(); ++i) {
             printf("zrevrangewithscore key : %s score : %s\n", 
             resultWithScore.Members[i].c_str(), resultWithScore.Scores[i].c_str());
    }
    printf("---------------------------TEST ZSET  END---------------------------\n\n\n\n\n");
}

int main()
{
    RedisTestString();
    RedisTestList();
    RedisTestSet();
    RedisTestHash();
    RedisTestZSet();
    return 0;
}
