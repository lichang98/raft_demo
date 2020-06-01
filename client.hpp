#pragma once
#include "rpc.hpp"
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
            // in this system, only one user client request for system for process commands
            data->src_server_index=-2;
            data->dest_server_index=-1;
            data->is_request=true;

            rpc::client_request* param=new rpc::client_request();
            int32_t index=0;
            std::fstream fs(file, std::fstream::in);
            while(!fs.eof())
            {
                int32_t k,v;
                fs >> k >> v;
                my_data_type::log_entry entry;
                entry.data=my_data_type::kv_data(k,v);
                param->data[index++] = entry;
            }
            param->real_bring = index;
            data->params.clt = *param;
            fs.close();
            return data;
        }

        /*
        * send command request to system for process
        */
        void send_msg(rpc::rpc_data* msg)
        {
            char* send_msg=nullptr;
            msg->serialize(send_msg);
            LOG(INFO) << "user client send to system commands";
            rpcmanager.client_send_msg((void*)send_msg, rpc::rpc_data::serialize_size());
        }
        /*
        * try to receive data response with timeout
        */
        rpc::rpc_data* recv_response(int32_t timeout_milliseconds=100)
        {
            std::tuple<int32_t,char*> back_data = rpcmanager.recv_data(rpc::rpc_data::serialize_size(),timeout_milliseconds);
            if(std::get<0>(back_data) <=0)
            {
                return nullptr;
            }
            else
            {
                rpc::rpc_data* ret = new rpc::rpc_data();
                ret->deserialize(std::get<1>(back_data));
                return ret;
            }
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
