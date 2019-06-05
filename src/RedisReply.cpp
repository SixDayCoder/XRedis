#include "RedisReply.h"

#include <stdlib.h>
#include <assert.h>
namespace db
{
    RedisReply::RedisReply(void* reply)
    {
        if(reply) {
            m_Reply = static_cast<redisReply*>(reply);
        }
    }

    RedisReply::RedisReply(redisReply* reply) : m_Reply(reply)
    {

    }

    RedisReply::~RedisReply()
    {
        if(m_Reply) {
            freeReplyObject(m_Reply);
        }
    }

    bool RedisReply::IsVaild() const
    {
        if(!m_Reply) {
            return false;
        }
        if(m_Reply->type == REDIS_REPLY_ERROR) {
            return false;
        }
        return true;
    }

    int RedisReply::Type() const 
    {
        if(m_Reply) {
            return m_Reply->type;
        }
        return REDIS_ERR;
    }

    int RedisReply::Integer() const
    {
        if(m_Reply) {
            if(m_Reply->type == REDIS_REPLY_INTEGER) {
                return m_Reply->integer;
            }
        }
        return -1;
    }

    std::string RedisReply::ErrorMsg() const
    {
        if(!m_Reply) {
            return "Reply Is Null Pointer";
        }
        if(m_Reply->type != REDIS_REPLY_ERROR) {
            return "Reply Type Is Not REDIS_REPLY_ERROR";
        }
        return std::string(m_Reply->str);
    }

    bool RedisReply::ParseInt32(int32_t* val) const 
    {
        if(!val || !m_Reply) { return false; }
        if( m_Reply->type != REDIS_REPLY_STRING ) { return false; }
        *val = static_cast<int64_t>(atoi(m_Reply->str));
        return true;
    }

    bool RedisReply::ParseInt64(int64_t* val) const 
    {
        if(!val || !m_Reply) { return false; }
        if( m_Reply->type != REDIS_REPLY_STRING ) { return false; }
        *val = static_cast<int64_t>(atoll(m_Reply->str));
        return true;
    }

    bool RedisReply::ParseFloat(float* val) const 
    {
        if(!val || !m_Reply) { return false; }
        if( m_Reply->type != REDIS_REPLY_STRING ) { return false; }
        *val = static_cast<float>(atof(m_Reply->str));
        return true;
    }

    bool RedisReply::ParseStdString(std::string* val)
    {
        if(!val || !m_Reply) { return false; }
        if( m_Reply->type != REDIS_REPLY_STRING ) { return false; }
        *val = m_Reply->str;
        return true;
    }

    bool RedisReply::ParseArray(std::vector<std::string>* val)
    {
        if(!val || !m_Reply) { return false; }
        if( m_Reply->type != REDIS_REPLY_ARRAY ) { return false; }
        assert(m_Reply->elements);
        for(int i = 0 ; i < m_Reply->elements; ++i) {
            val->push_back(m_Reply->element[i]->str);
        }
        return true;
    }

    bool RedisReply::ParseHash(std::map<std::string, std::string>* val)
    {
        if(!val || !m_Reply) { return false; }
        if(m_Reply->type != REDIS_REPLY_ARRAY ) { return false; }
        if(m_Reply->elements % 2 != 0) { return false; }
        for(int i = 0 ; i < m_Reply->elements; ) {
            if(i + 1 < m_Reply->elements) {
                std::string k(m_Reply->element[i]->str);
                std::string v(m_Reply->element[i + 1]->str);
                val->insert(std::make_pair(k, v));
                i += 2;
            }
        }
        return true;
    }
}