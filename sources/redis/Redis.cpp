//
// Created by Alexander Andreev on 15.11.2023.
//

#include "Redis.h"

using namespace Minisat;

Redis::Redis(Solver& solver) : solverRef(solver) {

}

//=================================================================================================
// Convert clause format between sat-solver and redis

char* Redis::to_str(const Clause& c) {
    std::string formula;

    for (int i = 0; i < c.size(); i++) {
        formula += sign(c[i]) ? "-" : "";
        formula += std::to_string(var(c[i]) + 1);
        formula += " ";
    }

    formula += "0";

    // Convert the string to a char*
    char* charFormula = new char[formula.length() + 1];
    std::strcpy(charFormula, formula.c_str());
    return charFormula;
}

char* Redis::to_str(Lit lit) {
    std::string formula;

    formula += sign(lit) ? "-" : "";
    formula += std::to_string(var(lit) + 1);
    formula += " ";

    formula += "0";

    // Convert the string to a char*
    char* charFormula = new char[formula.length() + 1];
    std::strcpy(charFormula, formula.c_str());
    return charFormula;
}

bool Redis::from_str(char* formula, vec<Lit>& learnt_clause) {
    if (verbosity > 1) fprintf(stderr, "from_str(formula = %p)\n", formula);
    char* token = strtok(formula, " ");

    while (token != NULL) {
        int elit = atoi(token);
        if (elit == 0) {
            token = strtok(NULL, " ");
            if (token != NULL) {
                fprintf(stderr, "PARSE ERROR deriving clause from redis! Expected 0\n", *in);
                exit(3);
            }
            continue;
        }
        int var = abs(elit)-1;
        learnt_clause.push( (elit > 0) ? mkLit(var) : ~mkLit(var));
        token = strtok(NULL, " ");
    }
    return true;
}

//=================================================================================================
// Work with redis

redisContext* Redis::get_context() {
    redisContext *c = redisConnect(redis_host, redis_port);
    if (c == NULL || c->err) {
        if (c) {
            fprintf(stderr, "Error: %s\n", c->errstr);
            exit(3);
        } else {
            fprintf(stderr, "Can't allocate redis context\n");
            exit(3);
        }
    }
    return c;
}

void Redis::redis_free(redisContext* c) {
    redisFree(c);
}


bool Redis::flush_redis() {
    if (verbosity > 1) fprintf(stderr, "flush_redis()\n");
    redisContext *c = redisConnect(redis_host, redis_port);
    redisReply *reply = (redisReply*) redisCommand(c, "FLUSHDB");

    bool res = true;
    if (reply == NULL) {
        fprintf(stderr, "Error in FLUSHDB command. Pleas run redis, or run 'docker compose up' with redis image\n");
        exit(3);
    }

    redis_free(c);
    return res;
}

void Redis::save_learnts() {
    if (verbosity > 1) fprintf(stderr, "save_learnts()...\n");
    // assert(solverRef->learnts.size() >= redis_last_learnt_id);
    redisContext* context = get_context();
    // save_learnt_clauses(context);
    save_unit_clauses(context);
    redis_free(context);
}

void Redis::load_clauses() {
    // if (verbosity > 1) fprintf(stderr, "load_clauses() start\n");
    // // assert(redis_last_learnt_id == learnts.size());
    // redisContext* context = get_context();
    // vec<Lit> learnt_clause;
    //
    // size_t len = get_redis_queue_len(context);
    // if (len > 0) {
    //     redisReply* reply = rpop(context, len);
    //     redisReply** data = reply->element;
    //     for (size_t i = 0; i < len; ++i) {
    //         learnt_clause.clear();
    //         load_clause(data[i], learnt_clause);
    //     }
    //     freeReplyObject(reply);
    // }
    // redis_free(context);
    // if (verbosity > 1) fprintf(stderr, "load_clauses() end\n");
}

bool Redis::save_learnt_clauses(redisContext* context) {
    // int curr = redis_last_learnt_id;
    // int end = solverRef->learnts.size();
    // int __offset = 0;
    // while (curr < end) {
    //     redisReply *reply;
    //     int buf = 0;
    //     for (; curr < end && buf < redis_buffer; curr++, buf++) {
    //         const Clause &c = ca[solverRef->learnts[curr]];
    //         if (c.size() > max_clause_len) {
    //             buf--;
    //             continue;
    //         }
    //         assert(c.size() > 1);
    //         redisAppendCommand(context, "SET from_minisat:%d %s", redis_last_from_minisat_id + __offset, to_str(c));
    //         assert(redis_last_from_minisat_id + __offset < INT_MAX);
    //         __offset++;
    //     }
    //     while (buf-- > 0) {
    //         redisGetReply(context,(void**)&reply); // reply for SET
    //         if (reply == NULL) {
    //             fprintf(stderr, "Error during saving clause");
    //             exit(3);
    //         }
    //         freeReplyObject(reply);
    //     }
    // }
    // redis_last_from_minisat_id += __offset;
    // if (verbosity > 1)
    //     fprintf(stderr, "new saved: %d\n", __offset);
    // redis_last_learnt_id = curr;
    // assert(learnts.size() == redis_last_learnt_id);
    // return true;
}

bool Redis::save_unit_clauses(redisContext* context) {
    int curr = 0;
    int end_pos = units.size();
    while (curr < end_pos) {
        redisReply *reply;
        int buf = 0;
        for (; curr < end_pos && buf < redis_buffer; curr++, buf++) {
            Lit lit = units[curr];
            redisAppendCommand(context, "SET from_minisat:%d %s", redis_last_from_minisat_id, to_str(lit));
            assert(redis_last_from_minisat_id < INT_MAX);
            redis_last_from_minisat_id++;
        }
        while (buf-- > 0) {
            redisGetReply(context,(void**)&reply); // reply for SET
            if (reply == NULL) {
                fprintf(stderr, "Error during saving unit");
                exit(3);
            }
            freeReplyObject(reply);
        }
    }
    assert(units.size() == curr);
    units.clear();
    assert(units.size() == 0);
    return true;
}

bool Redis::load_clause(redisReply* element, vec<Lit>& learnt_clause) {
    // if (verbosity > 1) fprintf(stderr, "load_clause(element = %p)\n", element);
    // if (verbosity > 1) fprintf(stderr, "load_clause(element.type = %d)\n", element->type);
    // if (verbosity > 1) fprintf(stderr, "load_clause(element.str = %s)\n", element->str);
    // assert(learnt_clause.size() == 0);
    //
    // if (element == NULL || element->type != REDIS_REPLY_STRING || element->str == NULL) {
    //     fprintf(stderr, "Error: element == NULL || element->type != REDIS_REPLY_STRING || element->str == NULL\n");
    //     exit(3);
    // }
    // from_str(element->str, learnt_clause);
    //
    // if (learnt_clause.size() == 1) {
    //     if (verbosity > 1) fprintf(stderr, "unit\n");
    //     if (value(learnt_clause[0]) == l_Undef) {
    //         uncheckedEnqueue(learnt_clause[0]);
    //         fprintf(stderr, "New useful unit: %s%d \n", sign(learnt_clause[0]) ? "-" : "", var(learnt_clause[0]) + 1);
    //     } else {
    //         if (value(learnt_clause[0]) == l_True) {
    //             fprintf(stderr, "Is already %s%d  == l_True \n", sign(learnt_clause[0]) ? "-" : "", var(learnt_clause[0]) + 1);
    //         } else {
    //             // TODO ok = false
    //             fprintf(stderr, "Is already %s%d  == l_False. Is unsat \n", sign(learnt_clause[0]) ? "-" : "", var(learnt_clause[0]) + 1);
    //             return ok = false;
    //         }
    //     }
    // } else {
    //     if (verbosity > 1) fprintf(stderr, "not unit\n");
    //     CRef cr = ca.alloc(learnt_clause, true);
    //     learnts.push(cr);
    //     attachClause(cr);
    //     claBumpActivity(ca[cr]);
    // }
    // return true;
}

size_t Redis::get_redis_queue_len(redisContext* context) {
    // if (verbosity > 1) fprintf(stderr, "get_redis_queue_len()\n");
    //
    // // Use the LLEN command to get the length of the list
    // redisReply* reply = (redisReply*) redisCommand(context, "LLEN to_minisat");
    //
    // if (reply == NULL) {
    //     fprintf(stderr, "Error executing LLEN command\n");
    //     exit(3);
    // }
    //
    // size_t res;
    // if (reply->type == REDIS_REPLY_INTEGER) {
    //     res = (size_t) reply->integer;
    // } else {
    //     fprintf(stderr, "Unexpected reply type: %d\n", reply->type);
    //     exit(3);
    // }
    //
    // freeReplyObject(reply);
    // return res;
}

redisReply* Redis::rpop(redisContext* context, size_t len) {
    // if (verbosity > 1) fprintf(stderr, "rpop(context = %p, len = %ld)\n", context, len);
    // redisReply* reply = (redisReply*) redisCommand(context, "RPOP to_minisat %ld", len);
    //
    // if (reply == NULL) {
    //     fprintf(stderr, "Error executing RPOP command\n");
    //     exit(3);
    // }
    //
    // redisReply** res;
    //
    // if (reply->type == REDIS_REPLY_ARRAY) {
    //     if (reply->elements != len) {
    //         fprintf(stderr, "Assert failed: reply->elements = %ld, len = %ld", reply->elements, len);
    //         exit(3);
    //     }
    //     assert(len == reply->elements);
    //     return reply;
    // } else {
    //     fprintf(stderr, "Unexpected reply type: %d\n", reply->type);
    //     if (reply->type == REDIS_REPLY_ERROR) {
    //         fprintf(stderr, "Redis Error: %s\n", reply->str);
    //     }
    //     freeReplyObject(reply);
    //     exit(3);
    // }
}