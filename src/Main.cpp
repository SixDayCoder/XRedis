#include "RedisConn.h"

void RedisTestHash()
{
    RedisConn conn("127.0.0.1", 6379);
    bool bFind = false;
    std::string str;
    RedisConn::ArrayResult array;
    RedisConn::HashResult  hash;

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
}

int main()
{
    RedisTestHash();
    return 0;
}