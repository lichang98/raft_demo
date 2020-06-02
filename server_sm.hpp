#pragma once
#include "log_manager.hpp"
#include <unordered_map>
#include <iostream>

namespace server_sm
{
    class SM
    {
    public:
        void update_db(std::vector<my_data_type::log_entry> committed_entries)
        {
            for(my_data_type::log_entry& ele : committed_entries)
            {
                in_mem_db[ele.data.k]=ele.data.v;
            }
        }

        friend std::ostream& operator<<(std::ostream& out, SM& obj)
        {
            out << "\n======in mem KV db====\n";
            for(const std::pair<int32_t,int32_t>& ele : obj.in_mem_db)
            {
                out << "K=" << ele.first << ", V=" << ele.second << "\n";
            }
            out << "====================\n";
            return out;
        }

        std::unordered_map<int32_t, int32_t> in_mem_db;
    };
    
}; // namespace server_sm


