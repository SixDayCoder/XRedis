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

        using ArrayResult = std::vector<Key>;

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

        RedisReply ExecuteRawCommand(const char* command);

    public:

        template<typename T>
        bool Set(const Key& key, const T& v);

        template<typename T>
        bool Get(const Key& key, T* val);

        bool Del(const Key& key);

    private:

        bool set(const Key& key, const Key& val);

        bool get(const Key& key, void* val, RedisGetType type);
        
     public:

        template<typename T>
        bool LPush(const Key& key, const T& v);

        template<typename T>
        bool RPush(const Key& key, const T& v);

        template<typename T1, typename T2>
        int LInsert(const Key& key, const T1& insertVal, const T2& pivotVal, bool bIsBefore = true);

        template<typename T>
        bool LSet(const Key& key, int index, const T& val);

        bool LPop(const Key& key, std::string* val);

        bool LPop(const Key& key);

        bool RPop(const Key& key, std::string* val);

        bool RPop(const Key& key);

        int  LLen(const Key& key);

        bool LRange(const Key& key, int start, int end, ArrayResult* result);

        bool LIndex(const Key& key, int index, std::string* result);

        bool LTrim(const Key& key, int start, int stop);

    private:

        bool lpop(const Key& key, std::string* val);

        bool rpop(const Key& key, std::string* val);

        bool lpush(const Key& key, const std::string& val);
        
        bool rpush(const Key& key, const std::string& val);

        int linsert(const Key& key, const std::string& insertVal, const std::string& pivotval, bool bIsBefore);    

        bool lset(const Key& key, int index, const std::string& val);


    public:

        template<typename T>
        bool SAdd(const Key& key, const T& val);

        template<typename T>
        bool SIsSMemeber(const Key& key, const T& val);

        template<typename T>
        bool SRem(const Key& key, const T& val);

        template<typename T>
        bool SMove(const Key& srcKey, const Key& destKey, const T& val);

        bool SMembers(const Key& key, ArrayResult* result);

        bool SPop(const Key& key);

        bool SPop(const Key& key, std::string* val);

        int  SCard(const Key& key);

        bool SInter(const Key& lhs, const Key& rhs, ArrayResult* result);

        bool SUnion(const Key& lhs, const Key& rhs, ArrayResult* result);

        bool SDiff(const Key& lhs, const Key& rhs, ArrayResult* result);

        bool SRandomMember(const Key& key, std::string* val);

        bool SRandomMember(const Key& key, int count, ArrayResult* val);

    private:

        bool sadd(const Key& key, const std::string& val);

        bool sissmemeber(const Key& key, const std::string& val);    

        bool spop(const Key& key, std::string* val);

        bool srem(const Key& key, const std::string& val);

        bool smove(const Key& src, const Key& dst, const std::string& val);

        bool sop(const Key& lhs, const Key& rhs, const char* fmt, ArrayResult* result);

    private:

        redisContext* m_RedisContext;

    };

    template<typename T>
    bool RedisConn::Set(const Key& key, const T& v)
    {
        return set(key, ToStdString(v));
    }

    template<typename T>
    bool RedisConn::Get(const Key& key, T* val)
    {
        if(val == nullptr) { return false; }
        return get(key, val, gettype(*val));
    }

    template<typename T>
    bool RedisConn::LPush(const Key& key, const T& v)
    {
        return lpush(key, ToStdString(v));
    }

    template<typename T>
    bool RedisConn::RPush(const Key& key, const T& v)
    {
        return rpush(key, ToStdString(v));
    }

    template<typename T1, typename T2>
    int RedisConn::LInsert(const Key& key, const T1& insertVal, const T2& pivotVal, bool bIsBefore)
    {
        return linsert(key, ToStdString(insertVal), ToStdString(pivotVal), bIsBefore);
    }

    template<typename T>
    bool RedisConn::LSet(const Key& key, int index, const T& val)
    {
        return lset(key, index, ToStdString(val));
    }

    template<typename T>
    bool RedisConn::SAdd(const Key& key, const T& val)
    {
        return sadd(key, ToStdString(val));
    }

    template<typename T>
    bool RedisConn::SIsSMemeber(const Key& key, const T& val)
    {
        return sissmemeber(key, ToStdString(val));
    }

    template<typename T>
    bool RedisConn::SRem(const Key& key, const T& val)
    {
        return srem(key, ToStdString(val));
    }

    template<typename T>
    bool RedisConn::SMove(const Key& srcKey, const Key& destKey, const T& val)
    {
        return smove(srcKey, destKey, ToStdString(val));
    }
}

#endif