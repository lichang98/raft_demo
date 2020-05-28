#include "rpc.hpp"

int main(int argc, char const *argv[])
{
    // google::InitGoogleLogging(argv[0]);
    // google::SetLogDestination(google::INFO,"./log.txt");
    if(strcmp(argv[1],"server")==0)
    {
        printf("server start\n");
        rpc::RPCManager server;
        server.create_socket();
        server.bind_socket_port(8080);
        server.listen_and_accept();
        char *msg=(char*)"msgfrom_server";
        sleep(1);
        // server.send_msg(target_fd,(void*)msg,strlen(msg));
        server.close_socket();
    }
    else
    {
        rpc::RPCManager client;
        client.create_socket();
        client.bind_socket_port(6000);
        client.create_connection((char*)"127.0.0.1",8080);
        std::tuple<int32_t, void*> data=client.recv_data(2000);
        printf("client self sock=%d\n",client.self_socket_fd);
        // printf("client receive data [%s]\n",(char*)std::get<1>(data));
        client.close_socket();
    }

    // LOG(INFO) << "just info";
    // google::ShutdownGoogleLogging();
    
    return 0;
}
