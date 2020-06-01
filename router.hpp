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
            LOG(INFO) << "router running, at ip=" << _ip << ", port=" << _port;
            std::thread(&MyRouter::router_run,this).detach();
            std::this_thread::sleep_for(std::chrono::milliseconds(1)); // be sure the router run first
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
        int32_t port;
        char* ip_addr;
    private:

        void router_run()
        {
            while(is_sys_running)
            {
                LOG(INFO) << "router listen for connection with timeout 10ms";
                rpcmanager.listen_and_accept();
                // try to read from connections
                char* recv_ch = rpcmanager.server_recv_data(rpc::rpc_data::serialize_size());
                rpc::rpc_data* recv_data=nullptr;
                if(recv_ch)
                {
                    recv_data=new rpc::rpc_data();
                    recv_data->deserialize(recv_ch);
                }
                if(recv_data)
                {
                    if(recv_data->type == rpc::rpc_type::CONN)
                    {
                        id2sock_fd.insert(std::make_pair(recv_data->src_server_index,\
                                    *(rpcmanager.accepted_socket_fds.end()-1)));
                        LOG(INFO) << "router accept connection from server " << recv_data->src_server_index;
                    }
                    else if(recv_data->type == rpc::rpc_type::CLIENT_REQUEST)
                    {
                        // if the request is from client, the request need to redirect to current leader
                        // here mark client index=-2, there is only one client push command request to system
                        if(id2sock_fd.find(-2) == id2sock_fd.end())
                            id2sock_fd.insert(std::make_pair(-2,*(rpcmanager.accepted_socket_fds.end()-1)));
                        LOG(INFO) << "router accept user client connection";
                        // redirect to an arbitrary server, if the server is not the leader, it will redirect
                        // the request to current leader
                        // the received server if not the leader, it will redirect to the leader it knows
                        int32_t target_sock_fd=-1;
                        if(recv_data->dest_server_index==-1)
                        {
                            for(const std::pair<int32_t, int32_t>& ele : id2sock_fd)
                            {
                                if(ele.first != -2)
                                {
                                    recv_data->dest_server_index = ele.first;
                                    target_sock_fd=ele.second;
                                    break;
                                }
                            }
                        }
                        else
                        {
                            target_sock_fd=id2sock_fd[recv_data->dest_server_index];
                        }
                        
                        LOG(INFO) << "router redirect user client msg to server " << recv_data->dest_server_index;
                        char* send_data = nullptr;
                        recv_data->serialize(send_data);
                        rpcmanager.send_msg(target_sock_fd,(void*)send_data, rpc::rpc_data::serialize_size());
                    }
                    else
                    {
                        if(recv_data->dest_server_index >=0)
                        {
                            int32_t target_sock_fd = id2sock_fd[recv_data->dest_server_index];
                            char* send_ch = nullptr;
                            recv_data->serialize(send_ch);
                            rpcmanager.send_msg(target_sock_fd,(void*)send_ch,rpc::rpc_data::serialize_size());
                            LOG(INFO) << "router receive from " << recv_data->src_server_index << 
                                ", and redirect it to " << recv_data->dest_server_index;
                        }
                        else
                        {
                            // broadcast
                            for(const std::pair<int32_t, int32_t>& ele : id2sock_fd)
                            {
                                // broadcast not send to self and user client
                                if(ele.first == recv_data->src_server_index || ele.first < 0)
                                    continue;
                                recv_data->dest_server_index = ele.first;
                                char* send_ch = nullptr;
                                recv_data->serialize(send_ch);
                                rpcmanager.send_msg(ele.second,(void*)send_ch,rpc::rpc_data::serialize_size());
                                LOG(INFO) << "server idx=" << recv_data->src_server_index <<\
                                         " broadcast, send to server " << ele.first;
                            }
                        }
                    }
                }
            }
        }
        rpc::RPCManager rpcmanager;
        // map from id to sockfd
        std::unordered_map<int32_t, int32_t> id2sock_fd;
    };
} // namespace my_router
