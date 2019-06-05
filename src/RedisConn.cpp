#include "RedisConn.h"
#include "RedisReply.h"

#include <assert.h>
#include <string.h>

using namespace std;

namespace db
{
    #define REDIS_COMMIT_ONE_PARAM_COMMAND(fmt, param_1)               \
        char command[MAX_COMMAND_SIZE] = { 0 };                        \
        int  size = snprintf(command, MAX_COMMAND_SIZE, fmt, param_1); \
        assert(size < MAX_COMMAND_SIZE);                               \
        RedisReply reply(redisCommand(m_RedisContext, command));       \


    #define REDIS_COMMIT_TWO_PARAM_COMMAND(fmt, param_1, param_2)                \
        char command[MAX_COMMAND_SIZE] = { 0 };                                  \
        int  size = snprintf(command, MAX_COMMAND_SIZE, fmt, param_1, param_2);  \
        assert(size < MAX_COMMAND_SIZE);                                         \
        RedisReply reply(redisCommand(m_RedisContext, command));                 \


    #define REDIS_COMMIT_THREE_PARAM_COMMAND(fmt, param_1, param_2, param_3)             \
        char command[MAX_COMMAND_SIZE] = { 0 };                                          \
        int  size = snprintf(command, MAX_COMMAND_SIZE, fmt, param_1, param_2, param_3); \
        assert(size < MAX_COMMAND_SIZE);                                                 \
        RedisReply reply(redisCommand(m_RedisContext, command));                         \
    

    RedisConn::RedisConn(const char* ip, uint16_t port, const char* requirepass) : m_RedisContext(nullptr)
    {
        struct timeval tv = { MAX_CONN_TIME_OUT_SECOND, MAX_CONN_TIME_OUT_USECOND };
        m_RedisContext = redisConnectWithTimeout(ip, port, tv);
        assert(m_RedisContext);
        if(requirepass != nullptr) {
            const char* fmt = "auth %s";
            REDIS_COMMIT_ONE_PARAM_COMMAND(fmt, requirepass);
            assert(reply.IsVaild());
        }
    }

    RedisConn::~RedisConn()
    {
        if(m_RedisContext) {
            redisFree(m_RedisContext);
        }
    }

    bool RedisConn::Del(const Key& key)
    {
        const char* fmt = "del %s"; 
        REDIS_COMMIT_ONE_PARAM_COMMAND(fmt, key.c_str());
        return reply.Integer() > 0;
    }

    bool RedisConn::set(const Key& key, const std::string& val)
    {
        const char* fmt = "del %s"; 
        REDIS_COMMIT_TWO_PARAM_COMMAND(fmt, key.c_str(), val.c_str());
        return reply.IsVaild();
    }

    bool RedisConn::get(const Key& key, void* val, RedisGetType type)
    {
        const char* fmt = "get %s"; 
        REDIS_COMMIT_ONE_PARAM_COMMAND(fmt, key.c_str());
        if(!reply.IsVaild()) { return false; }
        switch (type)
        {
            case RedisGetType::TYPE_INT32:
                return reply.ParseInt32((int32_t*)val);
            case RedisGetType::TYPE_INT64:
                return reply.ParseInt64((int64_t*)val);
            case RedisGetType::TYPE_FLOAT:
                return reply.ParseFloat((float*)val);
            case RedisGetType::TYPE_STRING:
                return reply.ParseStdString((Key*)val);
        default:
            return false;
        }
    }

    bool RedisConn::LPop(const Key& key)
    {
        lpop(key, nullptr);
    }

    bool RedisConn::LPop(const Key& key, std::string* val)
    {
        if(val == nullptr) { return false; };
        return lpop(key, val);
    }

    bool RedisConn::RPop(const Key& key)
    {
        return rpop(key, nullptr);
    }

    bool RedisConn::RPop(const Key& key, std::string* val)
    {
        if(val == nullptr) { return false; };
        return rpop(key, val);
    }

    bool RedisConn::lpop(const Key& key, std::string* val)
    {
        const char* fmt = "lpop %s"; 
        REDIS_COMMIT_ONE_PARAM_COMMAND(fmt, key.c_str());
        if(val == nullptr) { return reply.IsVaild(); }
        return reply.ParseStdString(val);
    }

    bool RedisConn::rpop(const Key& key, std::string* val)
    {
        const char* fmt = "rpop %s"; 
        REDIS_COMMIT_ONE_PARAM_COMMAND(fmt, key.c_str());
        if(val == nullptr) { return reply.IsVaild(); }
        return reply.ParseStdString(val);
    }

    bool RedisConn::lpush(const Key& key, const std::string& val)
    {
        const char* fmt = "lpush %s %s"; 
        REDIS_COMMIT_TWO_PARAM_COMMAND(fmt, key.c_str(), val.c_str());
        return reply.IsVaild();
    }

    bool RedisConn::rpush(const Key& key, const std::string& val)
    {
        const char* fmt = "rpush %s %s"; 
        REDIS_COMMIT_TWO_PARAM_COMMAND(fmt, key.c_str(), val.c_str());
        return reply.IsVaild();
    }

    int RedisConn::LLen(const Key& key)
    {
        const char* fmt = "llen %s"; 
        REDIS_COMMIT_ONE_PARAM_COMMAND(fmt, key.c_str());
        return reply.Integer();
    }

    bool RedisConn::LRange(const Key& key, int start, int end, ArrayResult* result)
    {
        if(result == nullptr) { return false; }
        result->clear();
        const char* fmt = "lrange %s %d %d"; 
        REDIS_COMMIT_THREE_PARAM_COMMAND(fmt, key.c_str(), start, end);
        return reply.ParseArray(result);
    }

    bool RedisConn::LIndex(const Key& key, int index, std::string* result)
    {
        if(result == nullptr) { return false; }
        const char* fmt = "lindex %s %d"; 
        REDIS_COMMIT_TWO_PARAM_COMMAND(fmt, key.c_str(), index);
        return reply.ParseStdString(result);
    }

    int RedisConn::linsert(const Key& key, const std::string& insertVal, const std::string& pivotval, bool bIsBefore)
    {
        char command[MAX_COMMAND_SIZE] = { 0 };
        int size = MAX_COMMAND_SIZE;
        if(bIsBefore) {
            size = snprintf(command, MAX_COMMAND_SIZE, "linsert %s before %s %s", key.c_str(), pivotval.c_str(), insertVal.c_str());
        }
        else {
            size = snprintf(command, MAX_COMMAND_SIZE, "linsert %s after %s %s", key.c_str(), pivotval.c_str(), insertVal.c_str());
        }
        assert(size < MAX_COMMAND_SIZE);
        RedisReply reply(redisCommand(m_RedisContext, command));
        return reply.Integer();
    }

    bool RedisConn::lset(const Key& key, int index, const std::string& val)
    {
        const char* fmt = "lset %s %d %s"; 
        REDIS_COMMIT_THREE_PARAM_COMMAND(fmt, key.c_str(), index, val.c_str());
        return reply.IsVaild();
    }

    bool RedisConn::LTrim(const Key& key, int start, int stop)
    {
        const char* fmt = "ltrim %s %d %d"; 
        REDIS_COMMIT_THREE_PARAM_COMMAND(fmt, key.c_str(), start, stop);
        return reply.IsVaild();
    }

    bool RedisConn::sadd(const Key& key, const std::string& val)
    {
        const char* fmt = "sadd %s %s"; 
        REDIS_COMMIT_TWO_PARAM_COMMAND(fmt, key.c_str(), val.c_str());
        return reply.Integer() > 0;
    }

    bool RedisConn::sissmemeber(const Key& key, const std::string& val)
    {
        const char* fmt = "sissmemeber %s %s"; 
        REDIS_COMMIT_TWO_PARAM_COMMAND(fmt, key.c_str(), val.c_str());
        return reply.Integer() > 0;
    }

    bool RedisConn::SMembers(const Key& key, ArrayResult* result)
    {
        if(result == nullptr) { return false; }
        result->clear();
        const char* fmt = "sissmemeber %s"; 
        REDIS_COMMIT_ONE_PARAM_COMMAND(fmt, key.c_str());
        return reply.ParseArray(result);
    }

    bool RedisConn::SPop(const Key& key)
    {
        return spop(key, nullptr);
    }

    bool RedisConn::SPop(const Key& key, std::string* val)
    {
        if( val == nullptr) { return false; }
        return spop(key, val);
    }

    bool RedisConn::spop(const Key& key, std::string* val)
    {
        const char* fmt = "spop %s";
        REDIS_COMMIT_ONE_PARAM_COMMAND(fmt, key.c_str());
        if(val == nullptr) { return reply.IsVaild(); }
        return reply.ParseStdString(val);
    }

    bool RedisConn::srem(const Key& key, const std::string& val)
    {   
        const char* fmt = "srem %s %s";
        REDIS_COMMIT_TWO_PARAM_COMMAND(fmt, key.c_str(), val.c_str());
        return reply.Integer() > 0;
    }

    bool RedisConn::smove(const Key& src, const Key& dst, const std::string& val)
    {
        const char* fmt = "smove %s %s %s";
        REDIS_COMMIT_THREE_PARAM_COMMAND(fmt, src.c_str(), dst.c_str(), val.c_str());
        return reply.Integer() > 0;
    }

    int RedisConn::SCard(const Key& key)
    {
        const char* fmt = "scard %s";
        REDIS_COMMIT_ONE_PARAM_COMMAND(fmt, key.c_str());
        return reply.Integer();
    }

    bool RedisConn::sop(const Key& lhs, const Key& rhs, const char* fmt, ArrayResult* result)
    {
        result->clear();
        REDIS_COMMIT_TWO_PARAM_COMMAND(fmt, lhs.c_str(), rhs.c_str());
        return reply.ParseArray(result);
    }

    bool RedisConn::SInter(const Key& lhs, const Key& rhs, ArrayResult* result)
    {
        if(result == nullptr) { return false; }
        return sop(lhs, rhs, "sinter %s %s", result);
    }

    bool RedisConn::SUnion(const Key& lhs, const Key& rhs, ArrayResult* result)
    {
        if(result == nullptr) { return false; }
        return sop(lhs, rhs, "sunion %s %s", result);
    }

    bool RedisConn::SDiff(const Key& lhs, const Key& rhs, ArrayResult* result)
    {
        if(result == nullptr) { return false; }
        return sop(lhs, rhs, "sdiff %s %s", result);
    }

    bool RedisConn::SRandomMember(const Key& key, std::string* val)
    {
        if(val == nullptr) { return false; }
        const char* fmt = "srandmemeber %s";
        REDIS_COMMIT_ONE_PARAM_COMMAND(fmt, key.c_str());
        return reply.ParseStdString(val);
    }
    
    bool RedisConn::SRandomMember(const Key& key, int count, ArrayResult* val)
    {
        if(val == nullptr) { return false; }
        val->clear();
        const char* fmt = "srandmemeber %s";
        REDIS_COMMIT_TWO_PARAM_COMMAND(fmt, key.c_str(), count);
        return reply.ParseArray(val);
    }

    bool RedisConn::hset(const Key& key, const Key& field, const std::string& val)
    {
        const char* fmt = "hset %s %s %s";
        REDIS_COMMIT_THREE_PARAM_COMMAND(fmt, key.c_str(), field.c_str(), val.c_str());
        return reply.Integer() >= 0;
    }

    bool RedisConn::hget(const Key& key, const Key& field, std::string* val)
    {
        if(val == nullptr) { return false; }
        const char* fmt = "hget %s %s";
        REDIS_COMMIT_TWO_PARAM_COMMAND(fmt, key.c_str(), field.c_str());
        return reply.ParseStdString(val);
    }

    bool RedisConn::hdel(const Key& key, const Key& field)
    {
        const char* fmt = "hdel %s %s";
        REDIS_COMMIT_TWO_PARAM_COMMAND(fmt, key.c_str(), field.c_str());
        return reply.Integer() >= 0;
    }

    bool RedisConn::hexists(const Key& key, const Key& field)
    {
        const char* fmt = "hexists %s %s";
        REDIS_COMMIT_TWO_PARAM_COMMAND(fmt, key.c_str(), field.c_str());
        return reply.Integer() > 0;
    }

    int  RedisConn::HLen(const Key& key)
    {
        const char* fmt = "hlen %s";
        REDIS_COMMIT_ONE_PARAM_COMMAND(fmt, key.c_str());
        return reply.Integer();
    }

    int  RedisConn::hstrlen(const Key& key, const Key& field)
    {
        const char* fmt = "hstrlen %s %s";
        REDIS_COMMIT_TWO_PARAM_COMMAND(fmt, key.c_str(), field.c_str());
        return reply.Integer();
    }

    bool RedisConn::HKeys(const Key& key, ArrayResult* result)
    {
        if(result == nullptr) { return false; }
        const char* fmt = "hkeys %s";
        REDIS_COMMIT_ONE_PARAM_COMMAND(fmt, key.c_str());
        return reply.ParseArray(result);
    }

    bool RedisConn::HVals(const Key& key, ArrayResult* result)
    {
        if(result == nullptr) { return false; }
        const char* fmt = "hvals %s";
        REDIS_COMMIT_ONE_PARAM_COMMAND(fmt, key.c_str());
        return reply.ParseArray(result);
    }

    bool RedisConn::HGetAll(const Key& key, HashResult* result)
    {
        if(result == nullptr) { return false; }
        const char* fmt = "hgetall %s";
        REDIS_COMMIT_ONE_PARAM_COMMAND(fmt, key.c_str());
        return reply.ParseHash(result);
    }

    int  RedisConn::hincrby(const Key& key, const Key& field, int val)
    {
        const char* fmt = "hincrby %s %s %d";
        REDIS_COMMIT_THREE_PARAM_COMMAND(fmt, key.c_str(), field.c_str(), val);
        return reply.Integer();
    }

    float RedisConn::hincrbyfloat(const Key& key, const Key& field, float val)
    {
        const char* fmt = "hincrbyfloat %s %s %f";
        REDIS_COMMIT_THREE_PARAM_COMMAND(fmt, key.c_str(), field.c_str(), val);
        float v = 0.0f;
        bool  b = reply.ParseFloat(&v);
        return b ? v : 0.0f;
    }
}