#pragma once
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <vector>
#include <string>
#include <iostream>
#include <fstream>
#include <memory>
#include <ios>
#include "json.hpp"
#include "server.hpp"
#include "router.hpp"

/*
* this file contains system configures
*/
namespace my_system
{
  
    /*
    * the basic infos of each server
    */
    struct server_basic_info
    {
        server::identity iden;
        char *ip;
        int32_t port;
        int32_t idx;
        server_basic_info():iden(server::identity::FOLLOWER),ip(nullptr),port(-1),idx(-1){}
        server_basic_info(server::identity _iden,const char* _ip, int32_t _port, int32_t _idx):\
                iden(_iden),port(_port),idx(_idx)
        {
            ip = new char[strlen(_ip)+1];
            strncpy(ip,_ip, strlen(_ip));
            ip[strlen(_ip)]='\0';
        }

        friend std::ostream& operator<<(std::ostream& out, server_basic_info& obj)
        {
            out << "iden=" << obj.iden << ", addr=" << obj.ip << ", port=" << obj.port
                << ", idx=" << obj.idx;
            return out;
        }
    };
    struct config
    {
        int32_t num_servers;
        std::vector<server_basic_info> server_info;

        friend std::ostream& operator<<(std::ostream& out, config& obj)
        {
            out << "num servers=" << obj.num_servers << ", server configs are:\n";
            for(server_basic_info serv_info : obj.server_info)
            {
                out << serv_info << "\n";
            }
            return out;
        }
    };

    static config system_config;
    static my_router::MyRouter* opaque_router;

    class MySys
    {
    public:

        void parse_json_config(const char* file)
        {
            std::ifstream f;
            f.open(file,std::ifstream::in);
            f.seekg(0,f.end);
            int64_t file_size=f.tellg();
            char* buf = new char[file_size];
            f.seekg(0,f.beg);
            f.read(buf,file_size);
            nlohmann::json config=nlohmann::json::parse(buf);
            system_config.num_servers = (int32_t)config.value("num_servers",0);
            for(nlohmann::json::basic_json::reference ele : config["server_info"])
            {
                int32_t iden = (int32_t)(ele.value("iden",0)),
                        idx = (int32_t)(ele.value("id",0));
                server::identity identity = (iden == 0 ? server::identity::FOLLOWER : \
                            (iden==1 ? server::identity::CANDIDATE : server::identity::LEADER));
                std::string ip_addr = (std::string)(ele.value("ip_addr",""));
                int32_t port=(int32_t)(ele.value("port",0));
                system_config.server_info.emplace_back(server_basic_info(identity,ip_addr.c_str(),port,idx));
            }
            int32_t router_port = (int32_t)(config["router_info"].value("port",0));
            std::string router_ip = (std::string)(config["router_info"].value("ip_addr",""));
            opaque_router = new my_router::MyRouter(router_port, router_ip.c_str());
        }
    };
    
    
}; // namespace system

