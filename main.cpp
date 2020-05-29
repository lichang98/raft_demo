#include "system.hpp"

int main(int argc, char const *argv[])
{
    
    google::InitGoogleLogging(argv[0]);
    google::SetLogDestination(google::INFO,"./log_info.txt");
    google::SetLogDestination(google::ERROR,"./log_error.txt");
    my_system::MySys sys;
    sys.parse_json_config("./sys_config.json");
    sys.sys_start();
    std::this_thread::sleep_for(std::chrono::seconds(20));
    sys.shutdown();
    google::ShutdownGoogleLogging();
    return 0;
}
