#include "system.hpp"

int main(int argc, char const *argv[])
{
    
    my_system::MySys sys;
    sys.parse_json_config("./sys_config.json");
    for(my_system::server_basic_info info: my_system::system_config.server_info)
        std::cout << info << std::endl;

    std::cout << *my_system::opaque_router << std::endl;

    return 0;
}
