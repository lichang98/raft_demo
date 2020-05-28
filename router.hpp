#pragma once
#include "rpc.hpp"
#include <stdlib.h>
#include <vector>
#include <chrono>
#include <pthread.h>
#include <random>
#include <thread>
#include <unordered_map>
#include <future>
#include <functional>
#include <atomic>
#include <iostream>

namespace my_router
{
    class MyRouter
    {
    public:
        MyRouter(int32_t _port, const char* _ip)
        {
            this->port=_port;
            int32_t n=strlen(_ip);
            ip_addr=new char[n+1];
            strncpy(ip_addr,_ip,n);
            ip_addr[n]='\0';
            rpcmanager.create_socket();
            rpcmanager.bind_socket_port(_port);
            // listen and accept has a timeout
            // start a new thread to process
            std::async(&MyRouter::router_run,this);
        }

        friend std::ostream& operator<<(std::ostream& out, MyRouter &obj)
        {
            out << "ip = " << obj.ip_addr << ", port=" << obj.port;
            return out;
        }

    private:

        void router_run()
        {
            while(true)
            {
                rpcmanager.listen_and_accept();
                // try to read from connections
                rpc::rpc_data* recv_data = rpcmanager.server_recv_data();
                if(recv_data)
                {
                    if(recv_data == rpc::rpc_type::CONN)
                    {
                        id2sock_fd.insert(std::make_pair(recv_data->src_server_index,\
                                    *(rpcmanager.accepted_socket_fds.end()-1)));
                    }
                    else
                    {
                        int32_t target_sock_fd = id2sock_fd[recv_data->dest_server_index];
                        rpcmanager.send_msg(target_sock_fd,(void*)recv_data, sizeof(rpc::rpc_data));
                    }
                }
            }
        }
        rpc::RPCManager rpcmanager;
        int32_t port;
        char* ip_addr;
        // map from id to sockfd
        std::unordered_map<int32_t, int32_t> id2sock_fd;
    };
    
} // namespace my_router
