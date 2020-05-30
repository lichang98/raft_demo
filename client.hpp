#pragma once
#include "rpc.hpp"
#include "log_manager.hpp"
#include <fstream>

namespace client
{
    class MyClient
    {
    public:
        MyClient(char* _ip_addr, int32_t _port, char* _sys_open_ip, int32_t _sys_open_port)
        {
            this->port=_port;
            int n=strlen(_ip_addr);
            this->ip = new char[n+1];
            strncpy(this->ip, _ip_addr,n);
            this->ip[n]='\0';
            rpcmanager.create_socket();
            rpcmanager.bind_socket_port(_port);
            rpcmanager.create_connection(_sys_open_ip, _sys_open_port);
        }
        /*
        * user client data file contains the KV pairs the client will push to system
        * this function load the KV pairs and parcel them into rpc data structure
        */
        static rpc::rpc_data* file_parser(char* file)
        {
            rpc::rpc_data* data=new rpc::rpc_data();
            data->type=rpc::rpc_type::CLIENT_REQUEST;
            data->src_server_index=-2;
            data->is_request=true;

            rpc::client_request* param=new rpc::client_request();
            std::vector<log_manager::log_entry>* entries=new std::vector<log_manager::log_entry>();
            std::fstream fs(file, std::fstream::in);
            while(!fs.eof())
            {
                int32_t k,v;
                fs >> k >> v;
                log_manager::log_entry entry;
                entry.data=log_manager::kv_data(k,v);
                entries->emplace_back(entry);
            }
            param->data=entries;
            param->data_size = sizeof(std::vector<log_manager::log_entry>);
            data->params=param;
            fs.close();
            return data;
        }

        /*
        * send command request to system for process
        */
        void send_msg(rpc::rpc_data* msg)
        {
            rpcmanager.client_send_msg((void*)msg,sizeof(rpc::rpc_data));
        }
        /*
        * try to receive data response with timeout
        */
        rpc::rpc_data* recv_response(int32_t timeout_milliseconds=100)
        {
            std::tuple<int32_t,void*> back_data = rpcmanager.recv_data(sizeof(rpc::rpc_data),timeout_milliseconds);
            if(std::get<0>(back_data) == 0)
                return nullptr;
            else
                return (rpc::rpc_data*)std::get<1>(back_data);
        }
        ~MyClient()
        {
            rpcmanager.close_socket();
            delete ip;
        }

        rpc::RPCManager rpcmanager;
        char* ip;
        int32_t port;
    };
    
} // namespace client
