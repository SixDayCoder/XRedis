#ifndef SIXDAY_REDIS_REPLY_H_
#define SIXDAY_REDIS_REPLY_H_

#include "hiredis.h"

#include <string>
#include <vector>
#include <map>

namespace db
{
    class RedisReply
    {
    public:

        using ArrayResult = std::vector<std::string>;

        using HashResult = std::map<std::string, std::string>;

        using ZSetResult = std::vector<std::string>;

        struct ZSetWithScoreResult
        {
            std::vector<std::string> Members;
            std::vector<std::string> Scores;

            int size() {
                if( Members.size() != Scores.size() ) {
                    return -1;
                }
                return Members.size();
            }

            void clear() {
                Members.clear();
                Scores.clear();
            }
        };   

    public:

        explicit RedisReply(void* reply);

        explicit RedisReply(redisReply* reply = nullptr);

        ~RedisReply();

    public:

        bool IsVaild() const;

        int  Type() const;

        long long  Integer() const;

        std::string ErrorMsg() const;
        
    public:

        bool ParseInt32(int32_t *val) const;

        bool ParseInt64(int64_t *val) const;

        bool ParseFloat(float *val) const;
        
        bool ParseStdString(std::string* val);

        bool ParseArray(std::vector<std::string>* val);

        bool ParseHash(std::map<std::string, std::string>* val);

        bool ParseZSet(std::vector<std::string>* val);

        bool ParseZSetWithScore(ZSetWithScoreResult* val);

    private:

        redisReply* m_Reply;

    };
}

#endif