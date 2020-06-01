#include "system.hpp"
#include "client.hpp"

int main(int argc, char const *argv[])
{
    // google::InitGoogleLogging(argv[0]);
    // google::SetLogDestination(google::INFO,"./log/log_info.txt");
    // google::SetLogDestination(google::ERROR,"./log/log_error.txt");
    // my_system::MySys sys;
    // sys.parse_json_config("./sys_config.json");
    // sys.sys_start();
    // std::this_thread::sleep_for(std::chrono::seconds(5));
    // client::MyClient usr_client((char*)"127.0.0.1",61000,my_system::opaque_router->ip_addr,my_system::opaque_router->port);
    // rpc::rpc_data* data = client::MyClient::file_parser((char*)"client_data.txt");
    // usr_client.send_msg(data);
    // sys.shutdown();
    // std::this_thread::sleep_for(std::chrono::seconds(5));
    // google::ShutdownGoogleLogging();

    rpc::rpc_data* data=new rpc::rpc_data();
    rpc::rpc_requestvote* req=new rpc::rpc_requestvote();
    data->type=rpc::rpc_type::REQUEST_VOTE;
    req->candidate_id=3;
    data->src_server_index=13;
    data->params = reinterpret_cast<void*>(req);
    char* ch=nullptr;
    data->serialize(ch);
    rpc::rpc_data* recv = new rpc::rpc_data();
    recv->deserialize(ch);
    printf("%d, candidate id=%d\n",recv->src_server_index,static_cast<rpc::rpc_requestvote*>(recv->params)->candidate_id);

    rpc::rpc_requestvote* req2=new rpc::rpc_requestvote();
    req2->candidate_id=15;
    char* ch2=nullptr;
    req2->serialize(ch2);
    rpc::rpc_requestvote* rev_req=new rpc::rpc_requestvote();
    rev_req->deserialize(ch2);
    printf("candi %d\n",rev_req->candidate_id);
    return 0;
}
