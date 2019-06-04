#include "RedisConn.h"
#include "RedisReply.h"

#include <assert.h>
#include <string.h>

using namespace std;

namespace db
{
    RedisConn::RedisConn(const char* ip, uint16_t port, const char* requirepass) : m_RedisContext(nullptr)
    {
        struct timeval tv;
        tv.tv_sec = MAX_CONN_TIME_OUT_SECOND;
        tv.tv_usec = MAX_CONN_TIME_OUT_USECOND;
        m_RedisContext = redisConnectWithTimeout(ip, port, tv);
        assert(m_RedisContext);
        if(requirepass != nullptr) {
            char command[MAX_COMMAND_SIZE] = { 0 };
            int  size = snprintf(command, MAX_COMMAND_SIZE, "auth %s", requirepass);
            assert(size < MAX_COMMAND_SIZE);
            RedisReply reply(redisCommand(m_RedisContext, command));
            assert(reply.IsVaild());
        }
    }

    RedisConn::~RedisConn()
    {
        if(m_RedisContext) {
            redisFree(m_RedisContext);
        }
    }

    RedisReply RedisConn::ExecuteRawCommand(const char* command)
    {
        RedisReply reply(redisCommand(m_RedisContext, command));
        return reply;
    }

    bool RedisConn::Del(const Key& key)
    {
        char command[MAX_COMMAND_SIZE] = { 0 };
        int  size = snprintf(command, MAX_COMMAND_SIZE, "del %s", key.c_str());
        assert(size < MAX_COMMAND_SIZE);
        RedisReply reply(redisCommand(m_RedisContext, command));
        return reply.Integer() > 0;
    }

    bool RedisConn::set(const Key& key, const std::string& val)
    {
        char command[MAX_COMMAND_SIZE] = { 0 };
        int  size = snprintf(command, MAX_COMMAND_SIZE, "set %s %s", key.c_str(), val.c_str());
        assert(size < MAX_COMMAND_SIZE);
        RedisReply reply(redisCommand(m_RedisContext, command));
        return reply.IsVaild();
    }

    bool RedisConn::get(const Key& key, void* val, RedisGetType type)
    {
        char command[MAX_COMMAND_SIZE] = { 0 };
        int  size = snprintf(command, MAX_COMMAND_SIZE, "get %s", key.c_str());
        assert(size < MAX_COMMAND_SIZE);

        RedisReply reply(redisCommand(m_RedisContext, command));
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
        char command[MAX_COMMAND_SIZE] = { 0 };
        int  size = snprintf(command, MAX_COMMAND_SIZE, "lpop %s", key.c_str());
        assert(size < MAX_COMMAND_SIZE);
        RedisReply reply(redisCommand(m_RedisContext, command));
        if(val == nullptr) {
            return reply.IsVaild();
        }
        return reply.ParseStdString(val);
    }

    bool RedisConn::rpop(const Key& key, std::string* val)
    {
        char command[MAX_COMMAND_SIZE] = { 0 };
        int  size = snprintf(command, MAX_COMMAND_SIZE, "rpop %s", key.c_str());
        assert(size < MAX_COMMAND_SIZE);
        RedisReply reply(redisCommand(m_RedisContext, command));
        if(val == nullptr) {
            return reply.IsVaild();
        }
        return reply.ParseStdString(val);
    }

    bool RedisConn::lpush(const Key& key, const std::string& val)
    {
        char command[MAX_COMMAND_SIZE] = { 0 };
        int  size = snprintf(command, MAX_COMMAND_SIZE, "lpush %s %s", key.c_str(), val.c_str());
        assert(size < MAX_COMMAND_SIZE);
        RedisReply reply(redisCommand(m_RedisContext, command));
        return reply.IsVaild();
    }

    bool RedisConn::rpush(const Key& key, const std::string& val)
    {
        char command[MAX_COMMAND_SIZE] = { 0 };
        int  size = snprintf(command, MAX_COMMAND_SIZE, "rpush %s %s", key.c_str(), val.c_str());
        assert(size < MAX_COMMAND_SIZE);
        RedisReply reply(redisCommand(m_RedisContext, command));
        return reply.IsVaild();
    }

    int RedisConn::LLen(const Key& key)
    {
        char command[MAX_COMMAND_SIZE] = { 0 };
        int  size = snprintf(command, MAX_COMMAND_SIZE, "llen %s", key.c_str());
        assert(size < MAX_COMMAND_SIZE);
        RedisReply reply(redisCommand(m_RedisContext, command));
        return reply.Integer();
    }

    bool RedisConn::LRange(const Key& key, int start, int end, ArrayResult* result)
    {
        if(result == nullptr) { return false; }
        result->clear();
        char command[MAX_COMMAND_SIZE] = { 0 };
        int  size = snprintf(command, MAX_COMMAND_SIZE, "lrange %s %d %d", key.c_str(), start, end);
        assert(size < MAX_COMMAND_SIZE);
        RedisReply reply(redisCommand(m_RedisContext, command));
        return reply.ParseArray(result);
    }

    bool RedisConn::LIndex(const Key& key, int index, std::string* result)
    {
        if(result == nullptr) { return false; }
        char command[MAX_COMMAND_SIZE] = { 0 };
        int  size = snprintf(command, MAX_COMMAND_SIZE, "lindex %s %d", key.c_str(), index);
        assert(size < MAX_COMMAND_SIZE);
        RedisReply reply(redisCommand(m_RedisContext, command));
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
        char command[MAX_COMMAND_SIZE] = { 0 };
        int  size = snprintf(command, MAX_COMMAND_SIZE, "lset %s %d %s", key.c_str(), index, val.c_str());
        assert(size < MAX_COMMAND_SIZE);
        RedisReply reply(redisCommand(m_RedisContext, command));
        return reply.IsVaild();
    }

    bool RedisConn::LTrim(const Key& key, int start, int stop)
    {
        char command[MAX_COMMAND_SIZE] = { 0 };
        int  size = snprintf(command, MAX_COMMAND_SIZE, "ltrim %s %d %d", key.c_str(), start, stop);
        assert(size < MAX_COMMAND_SIZE);
        RedisReply reply(redisCommand(m_RedisContext, command));
        return reply.IsVaild();
    }

    bool RedisConn::sadd(const Key& key, const std::string& val)
    {
        char command[MAX_COMMAND_SIZE] = { 0 };
        int  size = snprintf(command, MAX_COMMAND_SIZE, "sadd %s %s", key.c_str(), val.c_str());
        assert(size < MAX_COMMAND_SIZE);
        RedisReply reply(redisCommand(m_RedisContext, command));
        return reply.Integer() > 0;
    }

    bool RedisConn::sissmemeber(const Key& key, const std::string& val)
    {
        char command[MAX_COMMAND_SIZE] = { 0 };
        int  size = snprintf(command, MAX_COMMAND_SIZE, "sissmemeber %s %s", key.c_str(), val.c_str());
        assert(size < MAX_COMMAND_SIZE);
        RedisReply reply(redisCommand(m_RedisContext, command));
        return reply.Integer() > 0;
    }

    bool RedisConn::SMembers(const Key& key, ArrayResult* result)
    {
        if(result == nullptr) { return false; }
        result->clear();
        char command[MAX_COMMAND_SIZE] = { 0 };
        int  size = snprintf(command, MAX_COMMAND_SIZE, "smembers %s", key.c_str());
        assert(size < MAX_COMMAND_SIZE);
        RedisReply reply(redisCommand(m_RedisContext, command));
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
        char command[MAX_COMMAND_SIZE] = { 0 };
        int  size = snprintf(command, MAX_COMMAND_SIZE, "spop %s", key.c_str());
        assert(size < MAX_COMMAND_SIZE);
        RedisReply reply(redisCommand(m_RedisContext, command));
        if(val == nullptr) {
            return reply.IsVaild();
        }
        return reply.ParseStdString(val);
    }

    bool RedisConn::srem(const Key& key, const std::string& val)
    {   
        char command[MAX_COMMAND_SIZE] = { 0 };
        int  size = snprintf(command, MAX_COMMAND_SIZE, "srem %s %s", key.c_str(), val.c_str());
        assert(size < MAX_COMMAND_SIZE);
        RedisReply reply(redisCommand(m_RedisContext, command));
        return reply.Integer() > 0;
    }

    bool RedisConn::smove(const Key& src, const Key& dst, const std::string& val)
    {
        char command[MAX_COMMAND_SIZE] = { 0 };
        int  size = snprintf(command, MAX_COMMAND_SIZE, "smove %s %s %s", src.c_str(), dst.c_str(), val.c_str());
        assert(size < MAX_COMMAND_SIZE);
        RedisReply reply(redisCommand(m_RedisContext, command));
        return reply.Integer() > 0;
    }

    int RedisConn::SCard(const Key& key)
    {
        char command[MAX_COMMAND_SIZE] = { 0 };
        int  size = snprintf(command, MAX_COMMAND_SIZE, "scard %s", key.c_str());
        assert(size < MAX_COMMAND_SIZE);
        RedisReply reply(redisCommand(m_RedisContext, command));
        return reply.Integer();
    }

    bool RedisConn::sop(const Key& lhs, const Key& rhs, const char* fmt, ArrayResult* result)
    {
        result->clear();
        char command[MAX_COMMAND_SIZE] = { 0 };
        int  size = snprintf(command, MAX_COMMAND_SIZE, fmt, lhs.c_str(), rhs.c_str());
        assert(size < MAX_COMMAND_SIZE);
        RedisReply reply(redisCommand(m_RedisContext, command));
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
        char command[MAX_COMMAND_SIZE] = { 0 };
        int  size = snprintf(command, MAX_COMMAND_SIZE, "srandmemeber %s", key.c_str());
        assert(size < MAX_COMMAND_SIZE);
        RedisReply reply(redisCommand(m_RedisContext, command));
        return reply.ParseStdString(val);
    }
    
    bool RedisConn::SRandomMember(const Key& key, int count, ArrayResult* val)
    {
        if(val == nullptr) { return false; }
        val->clear();
        char command[MAX_COMMAND_SIZE] = { 0 };
        int  size = snprintf(command, MAX_COMMAND_SIZE, "srandmemeber %s %d", key.c_str(), count);
        assert(size < MAX_COMMAND_SIZE);
        RedisReply reply(redisCommand(m_RedisContext, command));
        return reply.ParseArray(val);
    }
}