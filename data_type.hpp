#pragma once
#include <stdlib.h>

namespace my_data_type
{
    struct kv_data
    {
        int32_t k,v;

        kv_data():k(0),v(0){}
        kv_data(int32_t _k, int32_t _v):k(_k),v(_v){}
    };
    /*
    * one entry in the log
    * attributes:
    *   term_id, the term when the entry is created
    *   index and term are related with log entry
    */
    struct log_entry
    {
        kv_data data;
        int32_t index,term;

        log_entry():index(-1),term(-1){}
    };
} // namespace my_data_type
