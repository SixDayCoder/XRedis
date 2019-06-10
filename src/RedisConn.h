#ifndef SIXDAY_REDIS_CONN_H_
#define SIXDAY_REDIS_CONN_H

#include <string>
#include <vector>
#include <map>

#include "hiredis.h"
#include "RedisReply.h"

namespace db
{
    #define MAX_COMMAND_SIZE (128)
    #define MAX_CONN_TIME_OUT_SECOND  (3)
    #define MAX_CONN_TIME_OUT_USECOND (0)

    class RedisConn
    {
    public:

        using Key = std::string; 

    private:

        enum RedisGetType
        {
            TYPE_INT32 = 0,
            TYPE_INT64 = 1,
            TYPE_FLOAT = 2,
            TYPE_STRING = 3,
        };

        inline std::string ToStdString(float v)
        {
            char buf[32];
            snprintf(buf, 32, "%f", v);
            return std::string(buf);
        }

        inline std::string ToStdString(int32_t v)
        {
            char buf[32];
            snprintf(buf, 32, "%d", v);
            return std::string(buf);
        }

        inline std::string ToStdString(int64_t v)
        {
            char buf[64];
            snprintf(buf, 64, "%lld", v);
            return std::string(buf);
        }

        inline std::string ToStdString(const std::string& v) { return v; }

        inline std::string ToStdString(const char* v) { return v; }

        inline RedisGetType gettype(int32_t val) { return RedisGetType::TYPE_INT32; }

        inline RedisGetType gettype(int64_t val) { return RedisGetType::TYPE_INT64; }

        inline RedisGetType gettype(float   val) { return RedisGetType::TYPE_FLOAT; }

        inline RedisGetType gettype(const std::string& val) { return RedisGetType::TYPE_STRING; }

    public:

        RedisConn(const char* ip, uint16_t port, const char* requirepass = nullptr);

        ~RedisConn();

    public:

        template<typename T>
        bool Set(const Key& key, const T& v)
        {
            return set(key, ToStdString(v));
        }

        template<typename T>
        bool Get(const Key& key, T* val)
        {
            if(val == nullptr) { return false; }
            return get(key, val, gettype(*val));
        }

        bool Del(const Key& key);

    private:

        bool set(const Key& key, const Key& val);

        bool get(const Key& key, void* val, RedisGetType type);
        
     public:

        template<typename T>
        bool LPush(const Key& key, const T& v)
        {
            return lpush(key, ToStdString(v));
        }

        template<typename T>
        bool RPush(const Key& key, const T& v)
        {
            return rpush(key, ToStdString(v));
        }

        template<typename T1, typename T2>
        int LInsert(const Key& key, const T1& insertVal, const T2& pivotVal, bool bIsBefore = true)
        {
             return linsert(key, ToStdString(insertVal), ToStdString(pivotVal), bIsBefore);
        }

        template<typename T>
        bool LSet(const Key& key, int index, const T& val)
        {
            return lset(key, index, ToStdString(val));
        }

        bool LPop(const Key& key, std::string* val);

        bool LPop(const Key& key);

        bool RPop(const Key& key, std::string* val);

        bool RPop(const Key& key);

        int  LLen(const Key& key);

        bool LRange(const Key& key, int start, int end, RedisReply::ArrayResult* result);

        bool LIndex(const Key& key, int index, std::string* result);

        bool LTrim(const Key& key, int start, int stop);

    private:

        bool lpop(const Key& key, std::string* val);

        bool rpop(const Key& key, std::string* val);

        bool lpush(const Key& key, const std::string& val);
        
        bool rpush(const Key& key, const std::string& val);

        int  linsert(const Key& key, const std::string& insertVal, const std::string& pivotval, bool bIsBefore);    

        bool lset(const Key& key, int index, const std::string& val);

    public:

        template<typename T>
        bool SAdd(const Key& key, const T& val)
        {
            return sadd(key, ToStdString(val));
        }

        template<typename T>
        bool SIsSMemeber(const Key& key, const T& val)
        {
            return sissmemeber(key, ToStdString(val));
        }

        template<typename T>
        bool SRem(const Key& key, const T& val)
        {
            return srem(key, ToStdString(val));
        }

        template<typename T>
        bool SMove(const Key& srcKey, const Key& destKey, const T& val)
        {
            return smove(srcKey, destKey, ToStdString(val));
        }

        bool SMembers(const Key& key, RedisReply::ArrayResult* result);

        bool SPop(const Key& key);

        bool SPop(const Key& key, std::string* val);

        int  SCard(const Key& key);

        bool SInter(const Key& lhs, const Key& rhs, RedisReply::ArrayResult* result);

        bool SUnion(const Key& lhs, const Key& rhs, RedisReply::ArrayResult* result);

        bool SDiff(const Key& lhs, const Key& rhs, RedisReply::ArrayResult* result);

        bool SRandomMember(const Key& key, std::string* val);

        bool SRandomMember(const Key& key, int count, RedisReply::ArrayResult* val);

    private:

        bool sadd(const Key& key, const std::string& val);

        bool sissmemeber(const Key& key, const std::string& val);    

        bool spop(const Key& key, std::string* val);

        bool srem(const Key& key, const std::string& val);

        bool smove(const Key& src, const Key& dst, const std::string& val);

        bool sop(const Key& lhs, const Key& rhs, const char* fmt, RedisReply::ArrayResult* result);

    public:

        template<typename T1, typename T2>
        bool HSet(const Key& key, const T1& field, const T2& val)
        {
            return hset(key, ToStdString(field), ToStdString(val));
        }

        template<typename T>
        bool HGet(const Key& key, const T& field, std::string* val)
        {
            return hget(key, ToStdString(field), val);
        }

        template<typename T>
        bool HDel(const Key& key, const T& field)
        {
            return hdel(key, ToStdString(field));
        }

        template<typename T>
        bool HExists(const Key& key, const T& field)
        {
            return hexists(key, ToStdString(field));
        }

        template<typename T>
        int  HStrlen(const Key& key, const T& field)
        {
            return hstrlen(key, ToStdString(field));
        }

        template<typename T>
        int  HIncrby(const Key& key, const T& field, int val)
        {
            return hincrby(key, ToStdString(field), val);
        }

        template<typename T>
        float HIncrbyFloat(const Key& key, const T& field, float val)
        {
            return hincrbyfloat(key, ToStdString(field), val);
        }

        int  HLen(const Key& key);

        bool HKeys(const Key& key, RedisReply::ArrayResult* result);

        bool HVals(const Key& key, RedisReply::ArrayResult* result);

        bool HGetAll(const Key& key, RedisReply::HashResult* result);

    private:

        bool hset(const Key& key, const Key& field, const std::string& val);

        bool hget(const Key& key, const Key& field, std::string* val);

        bool hdel(const Key& key, const Key& field);

        bool hexists(const Key& key, const Key& field);

        int  hstrlen(const Key& key, const Key& field);

        int  hincrby(const Key& key, const Key& field, int val);

        float hincrbyfloat(const Key& key, const Key& field, float val);

    public:

        template<typename T>
        bool ZAdd(const Key& key, int score, const T& member)
        {
            return zadd(key, score, ToStdString(member));
        }

        template<typename T>
        bool ZAdd(const Key& key, float score, const T& member)
        {
            return zadd(key, score, ToStdString(member));
        }

        template<typename T>
        std::string ZScore(const Key& key, const T& member)
        {
            return zscore(key, ToStdString(member));
        }

        template<typename T>
        std::string ZIncrby(const Key& key, const T& member, float score)
        {
            return zincrby(key, ToStdString(member), score);
        }

        template<typename T>
        int ZRank(const Key& key, const T& member)
        {
            return zrank(key, ToStdString(member));
        }

        template<typename T>
        int ZRevRank(const Key& key, const T& member)
        {
            return zrevrank(key, ToStdString(member));
        }

        template<typename T>
        int ZRem(const Key& key, const T& member)
        {
            return zrem(key, ToStdString(member));
        }

        int  ZCard(const Key& key);

        int  ZCount(const Key& key, int min, int max);

        int  ZCount(const Key& key, float min, float max);

        bool ZRange(const Key& key, int start, int end, RedisReply::ZSetResult* result);

        bool ZRangeWithScore(const Key& key, int start, int end, RedisReply::ZSetWithScoreResult* result);

        bool ZRevRange(const Key& key, int start, int end, RedisReply::ZSetResult* result);

        bool ZRevRangeWithScore(const Key& key, int start, int end, RedisReply::ZSetWithScoreResult* result);

        int  ZRemRangeByRank(const Key& key, int start, int end);

        int  ZRemRangeByScore(const Key& key, float min, float max);

    private:

        bool  zadd(const Key& key, int score, const std::string& member);

        bool  zadd(const Key& key, float score, const std::string& member);

        std::string zscore(const Key& key, const std::string& member);

        std::string zincrby(const Key& key, const std::string& member, float score);

        int   zrank(const Key& key, const std::string& member);

        int   zrevrank(const Key& key, const std::string& member);

        int   zrem(const Key& key, const std::string& member);

    public:

        bool Expire(const Key& key, int nSeconds);

        bool ExpireAt(const Key& key, long nTimeStamp);

        int  TTL(const Key& key);

        bool Persist(const Key& key);

        bool PExpire(const Key& key, long nMillsec);

        bool PExpireAt(const Key& key, long nTimeStamp);

        long PTTL(const Key& key);

    private:

        redisContext* m_RedisContext;
    };
}

#endif