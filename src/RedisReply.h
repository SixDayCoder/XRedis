#ifndef SIXDAY_REDIS_REPLY_H_
#define SIXDAY_REDIS_REPLY_H_

#include "hiredis.h"

#include <string>
#include <vector>

namespace db
{
    class RedisReply
    {
    public:

        explicit RedisReply(void* reply);

        explicit RedisReply(redisReply* reply = nullptr);

        ~RedisReply();

    public:

        bool IsVaild() const;

        int Type() const;

        int Integer() const;

        std::string ErrorMsg() const;
        
    public:

        bool ParseInt32(int32_t *val) const;

        bool ParseInt64(int64_t *val) const;

        bool ParseFloat(float *val) const;
        
        bool ParseStdString(std::string* val);

        bool ParseArray(std::vector<std::string>* val);

    private:

        redisReply* m_Reply;

    };
}

#endif