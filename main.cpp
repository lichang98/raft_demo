#include "system.hpp"
#include "client.hpp"

int main(int argc, char const *argv[])
{
    google::InitGoogleLogging(argv[0]);
    google::SetLogDestination(google::INFO,"./log/log_info.txt");
    google::SetLogDestination(google::ERROR,"./log/log_error.txt");
    my_system::MySys sys;
    sys.parse_json_config("./sys_config.json");
    sys.sys_start();
    std::this_thread::sleep_for(std::chrono::seconds(5));
    client::MyClient usr_client((char*)"127.0.0.1",61000,my_system::opaque_router->ip_addr,my_system::opaque_router->port);
    rpc::rpc_data* data = client::MyClient::file_parser((char*)"client_data.txt");
    usr_client.send_msg(data);
    std::this_thread::sleep_for(std::chrono::seconds(5));
    sys.shutdown();
    std::this_thread::sleep_for(std::chrono::seconds(5)); // wait for shutdown
    google::ShutdownGoogleLogging();
    return 0;
}
