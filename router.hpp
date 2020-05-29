#pragma once
#include "rpc.hpp"

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
            is_sys_running=true;
            std::async(&MyRouter::router_run,this);
        }

        friend std::ostream& operator<<(std::ostream& out, MyRouter &obj)
        {
            out << "ip = " << obj.ip_addr << ", port=" << obj.port;
            return out;
        }

        ~MyRouter()
        {
            rpcmanager.close_socket();
        }
        bool is_sys_running;
    private:

        void router_run()
        {
            while(is_sys_running)
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
                        LOG(INFO) << "router accept connection from server " << recv_data->src_server_index;
                    }
                    else
                    {
                        if(recv_data->dest_server_index >=0)
                        {
                            int32_t target_sock_fd = id2sock_fd[recv_data->dest_server_index];
                            rpcmanager.send_msg(target_sock_fd,(void*)recv_data, sizeof(rpc::rpc_data));
                            LOG(INFO) << "router receive from " << recv_data->src_server_index << 
                                ", and redirect it to " << recv_data->dest_server_index;
                        }
                        else
                        {
                            // broadcast
                            for(const std::pair<int32_t, int32_t>& ele : id2sock_fd)
                            {
                                if(ele.first == recv_data->src_server_index)
                                    continue;
                                recv_data->dest_server_index = ele.first;
                                rpcmanager.send_msg(ele.second,(void*)recv_data,sizeof(rpc::rpc_data));
                                LOG(INFO) << "server idx=" << recv_data->src_server_index <<\
                                         " broadcast, send to server " << ele.first;
                            }
                        }
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
