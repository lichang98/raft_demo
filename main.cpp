#include "system.hpp"

int main(int argc, char const *argv[])
{
    
    // google::InitGoogleLogging(argv[0]);
    // google::SetLogDestination(google::INFO,"./log/log_info.txt");
    // google::SetLogDestination(google::ERROR,"./log/log_error.txt");
    // my_system::MySys sys;
    // sys.parse_json_config("./sys_config.json");
    // sys.sys_start();
    // std::this_thread::sleep_for(std::chrono::seconds(20));
    // sys.shutdown();
    // google::ShutdownGoogleLogging();
    if(strcmp(argv[1],"s")==0)
    {
        rpc::RPCManager serv;
        serv.create_socket();
        serv.bind_socket_port(50000);
        for(int i=0;i<10;++i)
        {
            serv.listen_and_accept();
            char* data=new char[11];
            data = (char*)serv.server_recv_data(10);
            printf("server receive [%s]\n",data);
            sleep(1);
        }
        serv.close_socket();
    }
    else
    {
        printf("client start\n");
        std::thread([](){
            printf("thread start\n");
            rpc::RPCManager client;
            client.create_socket();
            client.bind_socket_port(60220);
            client.create_connection((char*)"127.0.0.1",50000);
            char *data=(char*)"1200920192";
            client.client_send_msg(data,10);
            client.close_socket();
        }).detach();
        std::thread([](){

            rpc::RPCManager client;
            client.create_socket();
            client.bind_socket_port(60201);
            client.create_connection((char*)"127.0.0.1",50000);
            char *data=(char*)"0001920100";
            client.client_send_msg(data,10);
            client.close_socket();
        }).detach();
        sleep(10);

    }
    
    return 0;
}
