#pragma once
#include "data_type.hpp"
#include <stdlib.h>
#include <stdio.h>
#include <vector>
#include <algorithm>

/*
* offer basic operations for log management
* extended function snapshot is included
*/
namespace log_manager
{
    class MachineLog
    {
    public:
        void append_entries(std::vector<my_data_type::log_entry> apd_logs)
        {
            logs.reserve(logs.size()+apd_logs.size());
            logs.insert(logs.end(),apd_logs.begin(),apd_logs.end());
        }

        void append_entries_fix_pos(int32_t pos, std::vector<my_data_type::log_entry> apd_logs)
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

        /*
        * get log entries in the given range, both bound inclusive
        */
        std::vector<my_data_type::log_entry> get_range(int32_t low,int32_t high)
        {
            if(logs.empty())
                return std::vector<my_data_type::log_entry>();
            std::vector<my_data_type::log_entry> ans(high-low+1);
            high = std::min(high,(int32_t)logs.size()-1);
            ans.assign(logs.begin()+low,logs.begin()+high+1);
            return ans;
        }

        int32_t get_log_size()
        {
            return logs.size();
        }

        my_data_type::log_entry get_entry_by_index(int32_t index)
        {
            int32_t i=logs.size();
            while(--i >=0)
            {
                if(logs[i].index == index)
                    return logs[i];
            }
            return my_data_type::log_entry();
        }


        void get_last_log_index_term(int32_t &index, int32_t& term)
        {
            if(logs.empty())
            {
                index=-1;
                term=-1;
                return;
            }
            int32_t n=logs.size();
            index = logs[n-1].index;
            term=logs[n-1].term;
        }
    private:
        std::vector<my_data_type::log_entry> logs;
    };
    
} // namespace log_manager
