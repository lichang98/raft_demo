#pragma once
#include <iostream>
#include <fstream>
#include <memory>
#include "json.hpp"
#include "server.hpp"
#include "router.hpp"

/*
* this file contains system configures
*/
namespace my_system
{
    struct config
    {
        int32_t num_servers;
        std::vector<server::ServerEnt> servers;

        friend std::ostream& operator<<(std::ostream& out, config& obj)
        {
            out << "num servers=" << obj.num_servers << ", server configs are:\n";
            for(const server::ServerEnt& server : obj.servers)
            {
                out << "idx= " <<server.server_idx << "\n";
            }
            return out;
        }
    };

    static config system_config;
    static my_router::MyRouter* opaque_router;

    class MySys
    {
    public:

        static void sys_start()
        {
            LOG(INFO) << "start each server ......";
            for(server::ServerEnt& serv: system_config.servers)
            {
                serv.start();
            }
        }

        static void shutdown()
        {
            opaque_router->is_sys_running=false;
            for(server::ServerEnt& serv: system_config.servers)
            {
                serv.is_server_running=false;
            }
        }

        static void parse_json_config(const char* file)
        {
            LOG(INFO) << "system parse json configure";
            std::ifstream f;
            f.open(file,std::ifstream::in);
            f.seekg(0,f.end);
            int64_t file_size=f.tellg();
            char* buf = new char[file_size];
            f.seekg(0,f.beg);
            f.read(buf,file_size);
            nlohmann::json config=nlohmann::json::parse(buf);
            system_config.num_servers = (int32_t)config.value("num_servers",0);
            int32_t router_port = (int32_t)(config["router_info"].value("port",0));
            std::string router_ip = (std::string)(config["router_info"].value("ip_addr",""));
            LOG(INFO) << "num_servers=" << system_config.num_servers << ", create opaque router";
            opaque_router = new my_router::MyRouter(router_port, router_ip.c_str());
            std::vector<int32_t> serv_idxs;
            for(nlohmann::json::basic_json::reference ele : config["server_info"])
            {
                int32_t idx = (int32_t)(ele.value("id",0));
                serv_idxs.push_back(idx);
                std::string ip_addr = (std::string)(ele.value("ip_addr",""));
                int32_t port=(int32_t)(ele.value("port",0));
                LOG(INFO) << "create new server instance, idx=" << idx << ", ip=" <<ip_addr << ", port="
                    << port;
                server::ServerEnt new_server(\
                        idx,(char*)ip_addr.c_str(),port,\
                        router_ip.c_str(),router_port);
                system_config.servers.emplace_back(new_server);
            }
            LOG(INFO) << "server info load finish";
            // initial each server's match index
            for(server::ServerEnt& sv: system_config.servers)
            {
                for(int32_t idx : serv_idxs)
                {
                    if(idx == sv.server_idx)
                        continue;
                    sv.match_index.insert(std::make_pair(idx,-1));
                }
            }
            server::ServerEnt::num_majority=(system_config.num_servers / 2)+(system_config.num_servers % 2);
            server::ServerEnt::num_servers=system_config.num_servers;
        }
    };
}; // namespace system

