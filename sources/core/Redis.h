//
// Created by Alexander Andreev on 15.11.2023.
//

#ifndef REDIS_H
#define REDIS_H

#include <hiredis.h>
#include "core/Solver.h"

namespace Minisat {

class Solver;

class Redis {
public:
    virtual ~Redis() = default;
    explicit Redis(Solver& solver);
    Solver& solverRef;
    const char * redis_host;
    int redis_port;
    unsigned int redis_last_from_minisat_id;
    unsigned int redis_buffer;
    unsigned int max_clause_len;
    vec<Lit>           units;          // List of unit in DL=0
    vec<CRef>          learnts;        // List of learnts

    char* to_str(const Clause&);
    char* to_str(Lit);
    bool from_str(char*, vec<Lit>&);

    redisContext* get_context();
    void redis_free(redisContext*);
    bool flush_redis();
    void save_learnts();
    void load_clauses();
    bool save_learnt_clauses(redisContext*);
    bool save_unit_clauses(redisContext*);
    bool load_clause(redisReply*, vec<Lit>&);
    size_t get_redis_queue_len(redisContext*);
    redisReply* rpop(redisContext*, size_t len);
};

}


#endif //REDIS_H
