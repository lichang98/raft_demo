#pragma once
#include <stdlib.h>
#include <stdio.h>
#include <vector>

/*
* offer basic operations for log management
* extended function snapshot is included
*/
namespace log_manager
{

    struct kv_data
    {
        int32_t k,v;
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

    class MachineLog
    {
    public:
        void append_entries(std::vector<log_entry> apd_logs)
        {
            logs.reserve(logs.size()+apd_logs.size());
            logs.insert(logs.end(),apd_logs.begin(),apd_logs.end());
        }

        void append_entries_fix_pos(int32_t pos, std::vector<log_entry> apd_logs)
        {
            logs.erase(logs.begin()+pos,logs.end());
            logs.reserve(logs.size()+apd_logs.size());
            logs.insert(logs.end(),apd_logs.begin(),apd_logs.end());
        }

        int32_t found_by_term_index(int32_t index, int32_t term)
        {
            if(logs.empty())
                return 0;
            int32_t i=logs.size();
            while(--i >=0)
            {
                if(logs[i].term==term && logs[i].index == index)
                    return i+1;
            }
            return -1;
        }

        log_entry get_entry_by_index(int32_t index)
        {
            int32_t i=logs.size();
            while(--i >=0)
            {
                if(logs[i].index == index)
                    return logs[i];
            }
            return log_entry();
        }


        void get_last_log_index_term(int32_t &index, int32_t& term)
        {
            int32_t n=logs.size();
            index = logs[n-1].index;
            term=logs[n-1].term;
        }
    private:
        std::vector<log_entry> logs;
    };
    
} // namespace log_manager
